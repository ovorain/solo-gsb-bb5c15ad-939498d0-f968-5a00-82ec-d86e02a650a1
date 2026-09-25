"""设备组写入路径增量对账测试（历史窗口不再全量重扫）。

每次 POST /ingest 触发的组对账只应处理：
- 本批实际新增或迟到修正触及的窗口；
- 因水位线推进而刚封存的窗口。
同组同窗只处理一次；其余无变化的历史窗口不读取、不参与本次写入对账。
纯重复批次（幂等重试）不产生任何组窗口读写；组创建仍做全量初始化。

这里通过 sqlite3 的 trace 回调记录写入事务内执行的 SQL 文本，区分：
- 旧全量口径的 "SELECT DISTINCT w.window_start ... JOIN device_group_members" 扫描；
- 增量口径下按起点的点查 "FROM windows w JOIN device_group_members ... WHERE w.window_start = ?"
  （每个受影响窗口恰好一条），以此证明历史窗口未被重扫。
"""

import pytest

from app import db


def ingest(client, device_id, events):
    return client.post("/ingest", json={"device_id": device_id, "events": events})


def make_group(client, group_id="g1", members=("d1", "d2"), aggregation="sum",
               window_size=60, lateness=0):
    return client.post("/groups", json={
        "group_id": group_id,
        "member_ids": list(members),
        "aggregation": aggregation,
        "window_size_seconds": window_size,
        "allowed_lateness_seconds": lateness,
    })


@pytest.fixture()
def sql_tracer(monkeypatch):
    """给每个新连接挂 trace 回调，返回事务内执行过的 SQL 文本列表（可随时清空）。"""
    statements: list[str] = []
    real_connect = db.connect

    def traced_connect():
        conn = real_connect()
        conn.set_trace_callback(statements.append)
        return conn

    monkeypatch.setattr(db, "connect", traced_connect)
    return statements


def _has_full_history_scan(statements: list[str]) -> bool:
    return any("SELECT DISTINCT w.window_start" in s for s in statements)


def _member_window_point_queries(statements: list[str]) -> int:
    """增量对账按 window_start 点查成员窗口的次数（每个受影响组窗口一条）。"""
    return sum(
        1 for s in statements
        if "FROM windows w" in s and "JOIN device_group_members" in s
    )


def _group_table_queries(statements: list[str]) -> list[str]:
    return [
        s for s in statements
        if "device_group_members" in s or "group_windows" in s
    ]


def _seed_history(client, n=30):
    """构造 n 个已封存历史窗口（d1/d2 各一条/窗），窗口 n 为两成员均 open 的当前窗口。"""
    for did in ("d1", "d2"):
        client.put(f"/devices/{did}/config", json={
            "window_size_seconds": 60, "allowed_lateness_seconds": 0,
        })
        events = [
            {"event_id": f"{did}-w{k}", "event_time": f"2026-09-22T00:{k:02d}:10Z", "value": 1.0}
            for k in range(n)
        ]
        # 落在窗口 n 起点的事件把水位线推到 n:00，封存窗口 0..n-1
        events.append({
            "event_id": f"{did}-cur", "event_time": f"2026-09-22T00:{n:02d}:00Z", "value": 1.0,
        })
        assert ingest(client, did, events).status_code == 200


# ---------- 增量点查，不重扫历史 ----------

def test_ingest_does_not_rescan_historical_group_windows(client, sql_tracer):
    n = 30
    _seed_history(client, n)
    make_group(client)

    # 组创建是全量初始化：历史 n 个窗口全部 ready，当前窗口 n 两成员仍 open => pending
    ws = client.get("/groups/g1/windows").json()
    assert len(ws) == n + 1
    assert sum(1 for w in ws if w["status"] == "ready") == n
    cur = f"2026-09-22T00:{n:02d}:00Z"
    assert next(w for w in ws if w["window_start"] == cur)["status"] == "pending"
    before = client.get("/groups/g1/windows").json()

    # 新事件只落入当前 open 窗口 n，不封存任何窗口：增量对账应只点查窗口 n 一次
    sql_tracer.clear()
    r = ingest(client, "d1", [
        {"event_id": "d1-new", "event_time": f"2026-09-22T00:{n:02d}:20Z", "value": 7.0},
    ])
    assert r.status_code == 200

    assert not _has_full_history_scan(sql_tracer), "写入路径不得全量扫描成员历史窗口"
    assert _member_window_point_queries(sql_tracer) == 1  # 仅窗口 n 被对账一次

    # 历史组窗口（含 updated_at）完全不变，只有当前窗口所在行可能被触达
    after = client.get("/groups/g1/windows").json()
    assert after == before

    # 旧窗口的 ready 值/版本不受影响
    w0 = client.get("/groups/g1/windows/2026-09-22T00:00:00Z").json()
    assert w0["status"] == "ready" and w0["revision"] == 1


def test_watermark_newly_sealed_window_is_reconciled_even_if_not_touched(client, sql_tracer):
    n = 30
    _seed_history(client, n)
    make_group(client)

    # d1 事件落在窗口 n+1：触及窗口 n+1，同时水位线把 d1 的窗口 n 刚封存。
    # 窗口 n 未被本批事件触及，但必须作为"刚封存窗口"参与对账。
    sql_tracer.clear()
    ingest(client, "d1", [
        {"event_id": "d1-jump", "event_time": f"2026-09-22T00:{n + 1:02d}:00Z", "value": 3.0},
    ])
    assert not _has_full_history_scan(sql_tracer)
    # 受影响起点恰好两个：窗口 n（刚封存）与窗口 n+1（本批触及）
    assert _member_window_point_queries(sql_tracer) == 2

    # d2 的窗口 n 仍 open => 组窗口 n 保持 pending（绝不用部分成员提前 ready）
    cur = f"2026-09-22T00:{n:02d}:00Z"
    gw = client.get(f"/groups/g1/windows/{cur}").json()
    assert gw["status"] == "pending"
    assert gw["sealed_member_count"] == 1

    # d2 同样推进：窗口 n 两成员均封存 => ready；窗口 n+1 仍 pending
    ingest(client, "d2", [
        {"event_id": "d2-jump", "event_time": f"2026-09-22T00:{n + 1:02d}:00Z", "value": 4.0},
    ])
    gw = client.get(f"/groups/g1/windows/{cur}").json()
    assert gw["status"] == "ready"
    assert gw["value"] == 2.0  # d1 窗口 n 仅 cur(1.0)，d2 同
    assert gw["revision"] == 1
    nxt = client.get(f"/groups/g1/windows/2026-09-22T00:{n + 1:02d}:00Z").json()
    assert nxt["status"] == "pending" and nxt["value"] is None

    # ready 只追加了窗口 n 这一份 revision=1 快照
    revs = client.get(f"/groups/g1/windows/{cur}/revisions").json()
    assert [r["revision"] for r in revs] == [1]


def test_late_correction_only_reconciles_corrected_window(client, sql_tracer):
    _seed_history(client, n=10)
    make_group(client)

    # d1 对早已封存的窗口 3 来一条迟到修正：增量集合只含窗口 3
    sql_tracer.clear()
    ingest(client, "d1", [
        {"event_id": "d1-late-w3", "event_time": "2026-09-22T00:03:40Z", "value": 5.0},
    ])

    assert not _has_full_history_scan(sql_tracer)
    assert _member_window_point_queries(sql_tracer) == 1

    w3 = client.get("/groups/g1/windows/2026-09-22T00:03:00Z").json()
    assert w3["status"] == "ready"
    assert w3["value"] == 7.0  # 1.0(d1) + 5.0(迟到) + 1.0(d2)
    assert w3["revision"] == 2
    # 其余历史窗口 revision 不动
    w0 = client.get("/groups/g1/windows/2026-09-22T00:00:00Z").json()
    assert w0["revision"] == 1 and w0["value"] == 2.0


# ---------- 纯重复批次：零组窗口读写 ----------

def test_pure_duplicate_batch_touches_no_group_tables(client, sql_tracer):
    _seed_history(client, n=20)
    make_group(client)

    before = client.get("/groups/g1/windows").json()

    # 幂等重试：整批事件均已存在
    sql_tracer.clear()
    r = ingest(client, "d1", [
        {"event_id": "d1-w3", "event_time": "2026-09-22T00:03:10Z", "value": 1.0},
        {"event_id": "d1-w7", "event_time": "2026-09-22T00:07:10Z", "value": 1.0},
    ])
    assert r.status_code == 200
    body = r.json()
    assert (body["accepted"], body["duplicates"], body["late"]) == (0, 2, 0)

    assert _group_table_queries(sql_tracer) == []
    assert client.get("/groups/g1/windows").json() == before

    # 再来一次同样的重试，结果与组状态依然稳定
    sql_tracer.clear()
    ingest(client, "d1", [
        {"event_id": "d1-w3", "event_time": "2026-09-22T00:03:10Z", "value": 1.0},
    ])
    assert _group_table_queries(sql_tracer) == []
    assert client.get("/groups/g1/windows").json() == before


def test_duplicate_retry_after_correction_does_not_bump_revision(client, sql_tracer):
    _seed_history(client, n=5)
    make_group(client)

    ingest(client, "d1", [
        {"event_id": "late-x", "event_time": "2026-09-22T00:02:50Z", "value": 3.0},
    ])
    w2 = client.get("/groups/g1/windows/2026-09-22T00:02:00Z").json()
    assert w2["revision"] == 2

    # 同一迟到事件幂等重试：不产生新修正、不重扫历史、revision 不重复升版
    sql_tracer.clear()
    ingest(client, "d1", [
        {"event_id": "late-x", "event_time": "2026-09-22T00:02:50Z", "value": 3.0},
    ])
    assert _group_table_queries(sql_tracer) == []
    again = client.get("/groups/g1/windows/2026-09-22T00:02:00Z").json()
    assert again["revision"] == 2 and again["value"] == w2["value"]


# ---------- 组创建保留全量初始化 ----------

def test_group_creation_still_does_full_initialization(client, sql_tracer):
    _seed_history(client, n=12)

    sql_tracer.clear()
    assert make_group(client).status_code == 201

    # 组创建路径保留全量扫描：一次性补算全部历史窗口
    assert _has_full_history_scan(sql_tracer)

    ws = client.get("/groups/g1/windows").json()
    assert len(ws) == 13
    ready = [w for w in ws if w["status"] == "ready"]
    assert len(ready) == 12
    assert all(w["revision"] == 1 for w in ready)


def test_group_created_after_sealed_history_backfills_all_and_preview_rolls_back(client, sql_tracer):
    # 组在历史数据全部封存后创建：全量初始化补齐 ready（与组创建前是否已扫描无关）
    for did in ("d1", "d2"):
        client.put(f"/devices/{did}/config", json={
            "window_size_seconds": 60, "allowed_lateness_seconds": 0,
        })
        ingest(client, did, [
            {"event_id": f"{did}-a", "event_time": "2026-09-22T00:00:10Z", "value": 10.0},
            {"event_id": f"{did}-b", "event_time": "2026-09-22T00:01:00Z", "value": 20.0},
        ])
    make_group(client, lateness=0)
    ws = {w["window_start"]: w for w in client.get("/groups/g1/windows").json()}
    assert ws["2026-09-22T00:00:00Z"]["status"] == "ready"
    assert ws["2026-09-22T00:00:00Z"]["value"] == 20.0

    # 预演迟到修正同样走增量对账（只点查窗口 0），回滚后无痕
    sql_tracer.clear()
    r = client.post("/ingest/preview", json={
        "device_id": "d1",
        "events": [{"event_id": "late-p", "event_time": "2026-09-22T00:00:30Z", "value": 5.0}],
    })
    assert r.status_code == 200
    assert not _has_full_history_scan(sql_tracer)
    assert _member_window_point_queries(sql_tracer) == 1

    gw = client.get("/groups/g1/windows/2026-09-22T00:00:00Z").json()
    assert gw["value"] == 20.0 and gw["revision"] == 1
