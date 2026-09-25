"""POST /ingest/preview 预演测试。

核心保证：
- 预演结果与随即在无并发写入时 POST /ingest 同批次的结果完全一致（逐事件状态、
  水位线、触及窗口的 count/sum/status/correction_count）；
- 批内重复 event_id 只计一次；前一事件抬高水位线后，后一旧事件会变成 late；
- 整个预演一律回滚：设备（含自动注册）、事件、窗口、修正、告警规则外的状态/流水、
  组汇总与不可变审计快照均不留痕。
"""

from app import db


def preview(client, device_id, events):
    return client.post("/ingest/preview", json={"device_id": device_id, "events": events})


def ingest(client, device_id, events):
    return client.post("/ingest", json={"device_id": device_id, "events": events})


_TABLES = [
    "devices", "device_labels", "events", "windows", "corrections",
    "device_groups", "device_group_members", "group_windows",
    "group_window_snapshots", "group_window_snapshot_members",
    "device_alert_rules", "device_alert_states", "device_alert_transitions",
]


def table_counts() -> dict[str, int]:
    with db.session() as conn:
        return {
            t: conn.execute(f"SELECT COUNT(*) AS c FROM {t}").fetchone()["c"]
            for t in _TABLES
        }


def _touched_starts(preview_body: dict) -> set[str]:
    return {r["window_start"] for r in preview_body["results"] if r["status"] != "duplicate"}


def test_preview_matches_then_commit_without_writes(client):
    # 既有状态：窗口 [00:00,00:01) 已封存，水位线 00:02:00
    ingest(client, "d1", [{"event_id": "e1", "event_time": "2026-09-22T00:00:10Z", "value": 1.0}])
    ingest(client, "d1", [{"event_id": "e2", "event_time": "2026-09-22T00:02:00Z", "value": 10.0}])

    before_device = client.get("/devices/d1").json()
    before_windows = client.get("/devices/d1/windows").json()
    before_counts = table_counts()

    batch = [
        {"event_id": "e1", "event_time": "2026-09-22T00:00:10Z", "value": 1.0},   # 库内已存在 -> duplicate
        {"event_id": "e3", "event_time": "2026-09-22T00:00:30Z", "value": 5.0},   # late
        {"event_id": "e4", "event_time": "2026-09-22T00:03:00Z", "value": 7.0},   # accepted，抬水位线
        {"event_id": "e3", "event_time": "2026-09-22T00:00:30Z", "value": 5.0},   # 批内重复 -> duplicate
        {"event_id": "e5", "event_time": "2026-09-22T00:00:40Z", "value": 3.0},   # late
        {"event_id": "e6", "event_time": "2026-09-22T00:02:10Z", "value": 2.0},   # accepted（[00:02] 未封存）
        {"event_id": "e4", "event_time": "2026-09-22T00:03:00Z", "value": 7.0},   # 批内重复 -> duplicate
    ]
    r = preview(client, "d1", batch)
    assert r.status_code == 200
    body = r.json()

    assert body["accepted"] == 2
    assert body["duplicates"] == 3
    assert body["late"] == 2
    assert body["watermark"] == "2026-09-22T00:03:00Z"
    assert [(x["event_id"], x["status"]) for x in body["results"]] == [
        ("e1", "duplicate"), ("e3", "late"), ("e4", "accepted"),
        ("e3", "duplicate"), ("e5", "late"), ("e6", "accepted"),
        ("e4", "duplicate"),
    ]
    # 触及窗口预演后的口径，按 window_start 升序
    assert body["windows"] == [
        {"window_start": "2026-09-22T00:00:00Z", "window_end": "2026-09-22T00:01:00Z",
         "status": "sealed", "count": 3, "sum": 9.0, "correction_count": 2},
        {"window_start": "2026-09-22T00:02:00Z", "window_end": "2026-09-22T00:03:00Z",
         "status": "open", "count": 2, "sum": 12.0, "correction_count": 0},
        {"window_start": "2026-09-22T00:03:00Z", "window_end": "2026-09-22T00:04:00Z",
         "status": "open", "count": 1, "sum": 7.0, "correction_count": 0},
    ]

    # 预演不留任何写入：设备水位线/统计、窗口列表、各表行数均与预演前一致
    assert client.get("/devices/d1").json() == before_device
    assert client.get("/devices/d1/windows").json() == before_windows
    assert client.get("/devices/d1/corrections").json() == []
    assert table_counts() == before_counts

    # 随即提交同一批次：逐事件结果、计数、水位线与预演完全一致
    committed = ingest(client, "d1", batch).json()
    assert committed["accepted"] == body["accepted"]
    assert committed["duplicates"] == body["duplicates"]
    assert committed["late"] == body["late"]
    assert committed["watermark"] == body["watermark"]
    assert [
        (x["event_id"], x["status"], x["window_start"]) for x in committed["results"]
    ] == [
        (x["event_id"], x["status"], x["window_start"]) for x in body["results"]
    ]

    # 提交后触及窗口的实际状态与预演口径一致
    touched = _touched_starts(body)
    actual_windows = [
        w for w in client.get("/devices/d1/windows").json()
        if w["window_start"] in touched
    ]
    assert len(actual_windows) == len(body["windows"])
    for preview_w, actual_w in zip(body["windows"], actual_windows):
        assert actual_w["window_start"] == preview_w["window_start"]
        assert actual_w["window_end"] == preview_w["window_end"]
        assert actual_w["status"] == preview_w["status"]
        assert actual_w["count"] == preview_w["count"]
        assert actual_w["sum"] == preview_w["sum"]
        assert actual_w["correction_count"] == preview_w["correction_count"]

    # 预演中的两条迟到事件确实在真正提交时各落一条修正
    corrections = client.get("/devices/d1/corrections").json()
    assert {c["event_id"] for c in corrections} == {"e3", "e5"}


def test_intra_batch_watermark_can_make_later_old_event_late(client):
    # e2 先把水位线抬到 00:02:00（封存 [00:00,00:01)），随后的 e3 即变成 late
    r = preview(client, "d1", [
        {"event_id": "e1", "event_time": "2026-09-22T00:00:10Z", "value": 1.0},
        {"event_id": "e2", "event_time": "2026-09-22T00:02:00Z", "value": 10.0},
        {"event_id": "e3", "event_time": "2026-09-22T00:00:20Z", "value": 2.0},
    ])
    body = r.json()
    assert [x["status"] for x in body["results"]] == ["accepted", "accepted", "late"]
    w00 = next(w for w in body["windows"] if w["window_start"] == "2026-09-22T00:00:00Z")
    assert (w00["status"], w00["count"], w00["sum"], w00["correction_count"]) == (
        "sealed", 2, 3.0, 1,
    )

    # 同一批事件换个顺序：旧事件在水位线抬高之前到达则为 accepted
    r2 = preview(client, "d1", [
        {"event_id": "e1", "event_time": "2026-09-22T00:00:10Z", "value": 1.0},
        {"event_id": "e3", "event_time": "2026-09-22T00:00:20Z", "value": 2.0},
        {"event_id": "e2", "event_time": "2026-09-22T00:02:00Z", "value": 10.0},
    ])
    assert [x["status"] for x in r2.json()["results"]] == ["accepted", "accepted", "accepted"]

    # 两次预演互不影响，设备始终未被创建
    assert client.get("/devices/d1").status_code == 404
    with db.session() as conn:
        assert conn.execute("SELECT COUNT(*) AS c FROM devices").fetchone()["c"] == 0
        assert conn.execute("SELECT COUNT(*) AS c FROM events").fetchone()["c"] == 0
        assert conn.execute("SELECT COUNT(*) AS c FROM windows").fetchone()["c"] == 0


def test_preview_unknown_device_autoregister_is_rolled_back(client):
    assert client.get("/devices/ghost").status_code == 404
    r = preview(client, "ghost", [
        {"event_id": "e1", "event_time": "2026-09-22T00:00:10Z", "value": 1.0},
    ])
    assert r.status_code == 200
    body = r.json()
    assert body["watermark"] == "2026-09-22T00:00:10Z"
    assert body["windows"][0]["status"] == "open"

    # 自动注册的设备连同事件/窗口全部回滚
    assert client.get("/devices/ghost").status_code == 404
    assert table_counts()["devices"] == 0


def test_preview_all_duplicates_has_empty_windows(client):
    ingest(client, "d1", [
        {"event_id": "e1", "event_time": "2026-09-22T00:00:10Z", "value": 1.0},
    ])
    r = preview(client, "d1", [
        {"event_id": "e1", "event_time": "2026-09-22T00:00:10Z", "value": 1.0},
    ])
    body = r.json()
    assert (body["accepted"], body["duplicates"], body["late"]) == (0, 1, 0)
    assert body["watermark"] == "2026-09-22T00:00:10Z"
    assert body["windows"] == []

    # 重复预演结果稳定，且不产生写入
    assert preview(client, "d1", [
        {"event_id": "e1", "event_time": "2026-09-22T00:00:10Z", "value": 1.0},
    ]).json() == body
    assert client.get("/devices/d1").json()["event_count"] == 1


def test_preview_leaves_no_alert_writes(client):
    client.put("/devices/d1/config", json={
        "window_size_seconds": 60, "allowed_lateness_seconds": 30,
    })
    assert client.put("/devices/d1/alert-rule", json={
        "trigger_threshold": 80.0, "recovery_threshold": 70.0,
    }).status_code == 200

    r = preview(client, "d1", [
        {"event_id": "e1", "event_time": "2026-09-22T00:00:10Z", "value": 90.0},
    ])
    assert r.status_code == 200

    alert = client.get("/devices/d1/alert").json()
    assert alert["alerting"] is False
    assert alert["alarm_count"] == 0
    assert alert["recovery_count"] == 0
    with db.session() as conn:
        assert conn.execute(
            "SELECT COUNT(*) AS c FROM device_alert_states"
        ).fetchone()["c"] == 0
        assert conn.execute(
            "SELECT COUNT(*) AS c FROM device_alert_transitions"
        ).fetchone()["c"] == 0


def test_preview_leaves_no_group_or_snapshot_writes(client):
    for did in ("d1", "d2"):
        ingest(client, did, [
            {"event_id": "e1", "event_time": "2026-09-22T00:00:10Z", "value": 1.0},
        ])
        ingest(client, did, [
            {"event_id": "e2", "event_time": "2026-09-22T00:02:00Z", "value": 10.0},
        ])
    assert client.post("/groups", json={
        "group_id": "g1", "member_ids": ["d1", "d2"], "aggregation": "sum",
        "window_size_seconds": 60, "allowed_lateness_seconds": 30,
    }).status_code == 201

    gw_before = client.get("/groups/g1/windows/2026-09-22T00:00:00Z").json()
    assert gw_before["status"] == "ready"
    assert gw_before["revision"] == 1
    with db.session() as conn:
        snaps_before = conn.execute(
            "SELECT COUNT(*) AS c FROM group_window_snapshots"
        ).fetchone()["c"]
        assert snaps_before == 1

    # 预演 d1 的迟到修正：事务内会把组汇总重算到 revision 2 并追加快照，回滚后必须无痕
    r = preview(client, "d1", [
        {"event_id": "e3", "event_time": "2026-09-22T00:00:20Z", "value": 5.0},
    ])
    assert r.status_code == 200
    assert r.json()["results"][0]["status"] == "late"

    gw_after = client.get("/groups/g1/windows/2026-09-22T00:00:00Z").json()
    assert gw_after == gw_before
    with db.session() as conn:
        assert conn.execute(
            "SELECT COUNT(*) AS c FROM group_window_snapshots"
        ).fetchone()["c"] == snaps_before
        assert conn.execute(
            "SELECT COUNT(*) AS c FROM group_window_snapshot_members"
        ).fetchone()["c"] == 2  # 仍只有 revision 1 的两条成员明细


def test_preview_pending_group_window_change_is_rolled_back(client):
    ingest(client, "d1", [
        {"event_id": "e1", "event_time": "2026-09-22T00:00:10Z", "value": 1.0},
    ])
    ingest(client, "d2", [
        {"event_id": "e1", "event_time": "2026-09-22T00:00:10Z", "value": 2.0},
    ])
    assert client.post("/groups", json={
        "group_id": "g1", "member_ids": ["d1", "d2"], "aggregation": "sum",
        "window_size_seconds": 60, "allowed_lateness_seconds": 30,
    }).status_code == 201

    before = client.get("/groups/g1/windows").json()
    assert len(before) == 1 and before[0]["status"] == "pending"

    # 预演只封存 d1 的窗口：事务内 sealed_member_count 1->... 保持 pending，但会被 UPDATE
    r = preview(client, "d1", [
        {"event_id": "e2", "event_time": "2026-09-22T00:02:00Z", "value": 10.0},
    ])
    assert r.status_code == 200
    assert client.get("/groups/g1/windows").json() == before


def test_preview_uses_same_request_validation_as_ingest(client):
    # 空批次与 /ingest 一样被请求模型拒绝（min_length=1）
    r = client.post("/ingest/preview", json={"device_id": "d1", "events": []})
    assert r.status_code == 422
    # 非法时间同样 422
    r = preview(client, "d1", [{"event_id": "e1", "event_time": "not-a-time", "value": 1.0}])
    assert r.status_code == 422
    # device_id 缺失
    r = client.post("/ingest/preview", json={"events": []})
    assert r.status_code == 422
    assert table_counts()["devices"] == 0
