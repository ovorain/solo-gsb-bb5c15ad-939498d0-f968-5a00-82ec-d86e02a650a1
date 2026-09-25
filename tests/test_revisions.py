"""设备组窗口修订审计（不可变快照）测试。"""

import sqlite3
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from app import db


def datetime_utc(year, month, day):
    return datetime(year, month, day, tzinfo=timezone.utc)


def ingest(client, device_id, events):
    return client.post("/ingest", json={"device_id": device_id, "events": events})


def seal_window(client, device_id, at="2026-09-22T00:02:00Z", value=99.0, event_id=None):
    """推进水位线到 00:02:00，封存窗口 [00:00, 00:01)（默认 lateness=30s）。"""
    event_id = event_id or f"seal-{device_id}"
    return ingest(client, device_id, [
        {"event_id": event_id, "event_time": at, "value": value},
    ])


def make_ready_group(client, members=("d1", "d2"), aggregation="sum", values=(10.0, 20.0)):
    for device_id, value in zip(members, values):
        ingest(client, device_id, [
            {"event_id": "e1", "event_time": "2026-09-22T00:00:10Z", "value": value},
        ])
    r = client.post("/groups", json={
        "group_id": "g1",
        "member_ids": list(members),
        "aggregation": aggregation,
        "window_size_seconds": 60,
        "allowed_lateness_seconds": 30,
    })
    assert r.status_code == 201
    for i, device_id in enumerate(members):
        seal_window(client, device_id, event_id=f"s{i}")
    return "2026-09-22T00:00:00Z"


def late_correct(client, device_id, event_id, value, at="2026-09-22T00:00:30Z"):
    return ingest(client, device_id, [
        {"event_id": event_id, "event_time": at, "value": value},
    ])


# ---------- 修订列表 ----------

def test_revision_list_first_ready_appends_revision_one(client):
    make_ready_group(client)

    revs = client.get("/groups/g1/windows/2026-09-22T00:00:00Z/revisions").json()
    assert [r["revision"] for r in revs] == [1]
    r1 = revs[0]
    assert r1["value"] == 30.0
    assert r1["kind"] == "revision"
    assert r1["member_count"] == 2
    assert r1["created_at"] is not None


def test_revision_list_grows_with_corrections_but_duplicate_report_no_extra_revision(client):
    make_ready_group(client)

    late_correct(client, "d1", "late-1", 5.0)
    # 同一 event_id 重试：幂等去重，不追加新快照
    late_correct(client, "d1", "late-1", 5.0)
    late_correct(client, "d2", "late-2", 1.0)

    revs = client.get("/groups/g1/windows/2026-09-22T00:00:00Z/revisions").json()
    assert [r["revision"] for r in revs] == [1, 2, 3]
    assert [r["value"] for r in revs] == [30.0, 35.0, 36.0]
    assert all(r["kind"] == "revision" for r in revs)
    assert all(r["member_count"] == 2 for r in revs)


def test_revision_list_pending_window_empty_and_missing_404(client):
    make_ready_group(client)
    # 00:02 窗口：仅封存事件自身，组窗口 pending（d2 无数据）
    pending_ws = "2026-09-22T00:02:00Z"
    assert client.get(f"/groups/g1/windows/{pending_ws}").json()["status"] == "pending"
    assert client.get(f"/groups/g1/windows/{pending_ws}/revisions").json() == []

    assert client.get("/groups/ghost/windows/2026-09-22T00:00:00Z/revisions").status_code == 404
    assert client.get("/groups/g1/windows/2030-01-01T00:00:00Z/revisions").status_code == 404


# ---------- 两版本 diff ----------

def test_diff_sum_only_changed_member_in_member_order(client):
    make_ready_group(client, members=("d1", "d2", "d3"), values=(10.0, 20.0, 30.0))

    # d2（position=1）迟到修正：30 -> 25
    late_correct(client, "d2", "late-1", 5.0)

    diff = client.get(
        "/groups/g1/windows/2026-09-22T00:00:00Z/revisions/diff"
        "?from_revision=1&to_revision=2"
    ).json()

    assert diff["aggregation"] == "sum"
    assert (diff["from_revision"], diff["to_revision"]) == (1, 2)
    assert (diff["from_value"], diff["to_value"], diff["value_delta"]) == (60.0, 65.0, 5.0)

    # 仅变化成员 d2
    assert [m["device_id"] for m in diff["members"]] == ["d2"]
    m = diff["members"][0]
    assert m["position"] == 1
    assert (m["before"]["count"], m["before"]["sum"]) == (1, 20.0)
    assert (m["after"]["count"], m["after"]["sum"]) == (2, 25.0)
    assert m["before"]["correction_count"] == 0
    assert m["after"]["correction_count"] == 1
    assert (m["count_delta"], m["sum_delta"], m["correction_count_delta"]) == (1, 5.0, 1)
    assert m["before"]["contribution"] == 20.0
    assert m["after"]["contribution"] == 25.0
    assert m["contribution_delta"] == 5.0
    assert m["before"]["avg"] == 20.0
    assert m["after"]["avg"] == 12.5
    assert m["avg_delta"] == 12.5 - 20.0


def test_diff_multiple_changes_ordered_by_position(client):
    make_ready_group(client, members=("d1", "d2", "d3"), values=(10.0, 20.0, 30.0))

    late_correct(client, "d3", "late-d3", 3.0)  # position 2 先变
    late_correct(client, "d1", "late-d1", 7.0)  # position 0 后变

    diff = client.get(
        "/groups/g1/windows/2026-09-22T00:00:00Z/revisions/diff"
        "?from_revision=1&to_revision=3"
    ).json()
    # 跨版本 diff 按成员顺序，仅列变化成员
    assert [m["device_id"] for m in diff["members"]] == ["d1", "d3"]
    assert diff["members"][0]["contribution_delta"] == 7.0
    assert diff["members"][1]["contribution_delta"] == 3.0
    assert diff["value_delta"] == 10.0
    # 没变化的 d2 不在列表中
    assert all(m["device_id"] != "d2" for m in diff["members"])


def test_diff_avg_contribution_is_member_window_avg(client):
    # d1: [10,20] avg 15；d2: [40] avg 40；组 avg = (15+40)/2 = 27.5
    make_ready_group(
        client,
        members=("d1", "d2"),
        aggregation="avg",
        values=(10.0, 40.0),
    )
    ingest(client, "d1", [
        {"event_id": "b", "event_time": "2026-09-22T00:00:20Z", "value": 20.0},
    ])
    # 上面这条在 ready 前？不——窗口已封存，所以是迟到修正，revision=2
    w = client.get("/groups/g1/windows/2026-09-22T00:00:00Z").json()
    assert w["revision"] == 2

    diff = client.get(
        "/groups/g1/windows/2026-09-22T00:00:00Z/revisions/diff?from_revision=1&to_revision=2"
    ).json()
    assert diff["aggregation"] == "avg"
    m = diff["members"][0]
    assert m["device_id"] == "d1"
    # d1 窗口 avg 从 10 变为 15，贡献差 5；组 value 差 5/2 = 2.5
    assert m["before"]["contribution"] == 10.0
    assert m["after"]["contribution"] == 15.0
    assert m["contribution_delta"] == 5.0
    assert diff["value_delta"] == 2.5


def test_diff_validation_422_and_missing_404(client):
    make_ready_group(client)
    late_correct(client, "d1", "late-1", 5.0)  # revision 2
    base = "/groups/g1/windows/2026-09-22T00:00:00Z/revisions/diff"

    # 缺少查询参数 / 非正版本 / from >= to → 422
    assert client.get(base).status_code == 422
    assert client.get(f"{base}?from_revision=0&to_revision=2").status_code == 422
    assert client.get(f"{base}?from_revision=2&to_revision=2").status_code == 422
    assert client.get(f"{base}?from_revision=2&to_revision=1").status_code == 422

    # 组 / 窗口 / revision 不存在 → 404
    ghost_base = "/groups/ghost/windows/2026-09-22T00:00:00Z/revisions/diff"
    assert client.get(f"{ghost_base}?from_revision=1&to_revision=2").status_code == 404
    no_win_base = "/groups/g1/windows/2030-01-01T00:00:00Z/revisions/diff"
    assert client.get(f"{no_win_base}?from_revision=1&to_revision=2").status_code == 404
    assert client.get(f"{base}?from_revision=1&to_revision=9").status_code == 404
    assert client.get(f"{base}?from_revision=9&to_revision=10").status_code == 404


# ---------- 不可变性 ----------

def test_snapshots_cannot_be_updated_or_deleted(client):
    make_ready_group(client)
    with db.session() as conn:
        with pytest.raises(sqlite3.IntegrityError) as excinfo:
            conn.execute(
                "UPDATE group_window_snapshots SET value = 999 WHERE revision = 1"
            )
        assert "append-only" in str(excinfo.value)

        with pytest.raises(sqlite3.IntegrityError):
            conn.execute("DELETE FROM group_window_snapshots")

        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "UPDATE group_window_snapshot_members SET sum = 999 WHERE position = 0"
            )

        with pytest.raises(sqlite3.IntegrityError):
            conn.execute("DELETE FROM group_window_snapshot_members")

    # 后续新修正不会覆盖旧快照：revision 1 仍是 30.0
    late_correct(client, "d1", "late-1", 5.0)
    revs = client.get("/groups/g1/windows/2026-09-22T00:00:00Z/revisions").json()
    assert revs[0]["revision"] == 1 and revs[0]["value"] == 30.0
    assert revs[1]["revision"] == 2 and revs[1]["value"] == 35.0


# ---------- 旧库初始化 baseline ----------

def test_startup_backfills_single_baseline_at_current_revision(tmp_path, monkeypatch):
    # 构造一个"旧版服务"留下的库：有 ready 组窗口（revision 已到 3），但没有任何审计快照
    db_file = tmp_path / "legacy.db"
    monkeypatch.setenv("TELEMETRY_DB_PATH", str(db_file))

    # 模拟升级后的二进制：init_db 先把新审计表建到旧库上，旧业务数据仍无快照
    db.init_db()
    from app.main import to_epoch

    w_start = to_epoch(datetime_utc(2026, 9, 22))
    with db.session() as conn:
        now = "2026-09-22T00:05:00+00:00"
        conn.execute(
            "INSERT INTO devices (device_id, window_size_seconds, allowed_lateness_seconds,"
            " watermark, created_at) VALUES (?, 60, 30, ?, ?)",
            ("d1", w_start + 120, now),
        )
        conn.execute(
            "INSERT INTO devices (device_id, window_size_seconds, allowed_lateness_seconds,"
            " watermark, created_at) VALUES (?, 60, 30, ?, ?)",
            ("d2", w_start + 120, now),
        )
        conn.execute(
            "INSERT INTO windows (device_id, window_start, window_end, count, sum, min, max,"
            " status, correction_count, late_event_count, sealed_at)"
            " VALUES ('d1', ?, ?, 2, 15.0, 5.0, 10.0, 'sealed', 1, 1, ?)",
            (w_start, w_start + 60, now),
        )
        conn.execute(
            "INSERT INTO windows (device_id, window_start, window_end, count, sum, min, max,"
            " status, correction_count, late_event_count, sealed_at)"
            " VALUES ('d2', ?, ?, 1, 20.0, 20.0, 20.0, 'sealed', 0, 0, ?)",
            (w_start, w_start + 60, now),
        )
        conn.execute(
            "INSERT INTO device_groups (group_id, window_size_seconds, allowed_lateness_seconds,"
            " aggregation, created_at) VALUES ('g1', 60, 30, 'sum', ?)",
            (now,),
        )
        conn.executemany(
            "INSERT INTO device_group_members (group_id, device_id, position) VALUES (?, ?, ?)",
            [("g1", "d1", 0), ("g1", "d2", 1)],
        )
        conn.execute(
            "INSERT INTO group_windows (group_id, window_start, window_end, status, value,"
            " member_count, sealed_member_count, revision, correction_sig, ready_at,"
            " created_at, updated_at)"
            " VALUES ('g1', ?, ?, 'ready', 35.0, 2, 2, 3, 1, ?, ?, ?)",
            (w_start, w_start + 60, now, now, now),
        )

    # 用旧库启动应用：lifespan 初始化只补当前 revision 的 baseline，不伪造 rev 1/2
    from app.main import app

    with TestClient(app) as c:
        revs = c.get("/groups/g1/windows/2026-09-22T00:00:00Z/revisions").json()
        assert len(revs) == 1
        assert revs[0]["revision"] == 3
        assert revs[0]["value"] == 35.0
        assert revs[0]["kind"] == "baseline"
        assert revs[0]["member_count"] == 2

        # baseline 成员明细取自当时成员窗口
        with db.session() as conn:
            rows = conn.execute(
                "SELECT m.device_id, m.position, m.count, m.sum, m.avg, m.correction_count"
                " FROM group_window_snapshot_members m"
                " JOIN group_window_snapshots s ON s.id = m.snapshot_id"
                " ORDER BY m.position"
            ).fetchall()
        assert [r["device_id"] for r in rows] == ["d1", "d2"]
        assert (rows[0]["count"], rows[0]["sum"], rows[0]["avg"], rows[0]["correction_count"]) == (
            2, 15.0, 7.5, 1
        )
        assert (rows[1]["count"], rows[1]["sum"], rows[1]["avg"], rows[1]["correction_count"]) == (
            1, 20.0, 20.0, 0
        )

        # 没有伪造的早期版本：diff 1->3 的 from 修订 404
        r = c.get(
            "/groups/g1/windows/2026-09-22T00:00:00Z/revisions/diff?from_revision=1&to_revision=3"
        )
        assert r.status_code == 404

        # baseline 也不可被后续修正覆盖：再来一次迟到修正只追加 revision=4
        late_correct(c, "d2", "late-x", 2.0)
        revs2 = c.get("/groups/g1/windows/2026-09-22T00:00:00Z/revisions").json()
        assert [r["revision"] for r in revs2] == [3, 4]
        assert [r["kind"] for r in revs2] == ["baseline", "revision"]
        assert revs2[0]["value"] == 35.0
        assert revs2[1]["value"] == 37.0
