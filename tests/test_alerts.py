"""设备级高值告警测试。

覆盖：
- 双阈值滞回状态机：首次越线进入、中间区间保持、降至恢复线解除、阈值边界取等；
- 批次重试 / 重复 event_id 不重复产生状态变化（含重启后重试）；
- 规则与状态持久化（重启不丢失）；
- 非法阈值（非有限数、recovery >= trigger）返回 400 且旧规则不被覆盖；
- 设备不存在 404；有设备无规则 GET /alert 404；
- 迟到事件同样驱动状态机；告警中再次高值不重复告警；
- 规则替换不改变当前状态。
"""

from fastapi.testclient import TestClient

from app import db


def ingest(client, device_id, events):
    return client.post("/ingest", json={"device_id": device_id, "events": events})


def set_rule(client, device_id, trigger, recovery):
    return client.put(
        f"/devices/{device_id}/alert-rule",
        json={"trigger_threshold": trigger, "recovery_threshold": recovery},
    )


def get_alert(client, device_id):
    return client.get(f"/devices/{device_id}/alert")


def setup_device_with_rule(client, device_id="dev-1", trigger=80.0, recovery=70.0):
    ingest(client, device_id, [
        {"event_id": "seed", "event_time": "2026-09-23T00:00:00Z", "value": 50.0},
    ])
    r = set_rule(client, device_id, trigger, recovery)
    assert r.status_code == 200
    return r.json()


# ---------- 规则校验 ----------

def test_alert_rule_requires_existing_device(client):
    r = set_rule(client, "ghost", 80.0, 70.0)
    assert r.status_code == 404


def test_recovery_threshold_must_be_below_trigger(client):
    ingest(client, "dev-1", [
        {"event_id": "e0", "event_time": "2026-09-23T00:00:00Z", "value": 50.0},
    ])
    # recovery > trigger
    r = set_rule(client, "dev-1", 70.0, 80.0)
    assert r.status_code == 400
    # recovery == trigger 也非法（必须严格更低，否则没有滞回区间）
    r = set_rule(client, "dev-1", 80.0, 80.0)
    assert r.status_code == 400
    # 非法请求后查询仍无规则
    assert get_alert(client, "dev-1").status_code == 404


def test_non_finite_thresholds_rejected_with_raw_json(client):
    ingest(client, "dev-1", [
        {"event_id": "e0", "event_time": "2026-09-23T00:00:00Z", "value": 50.0},
    ])
    # JSON 标准不允许 NaN/Infinity，但 Python 解析端接受这些字面量；服务端必须拒绝
    for raw in (
        '{"trigger_threshold": NaN, "recovery_threshold": 70.0}',
        '{"trigger_threshold": Infinity, "recovery_threshold": 70.0}',
        '{"trigger_threshold": -Infinity, "recovery_threshold": -9e999}',
        '{"trigger_threshold": 80.0, "recovery_threshold": NaN}',
    ):
        r = client.put(
            "/devices/dev-1/alert-rule",
            content=raw,
            headers={"Content-Type": "application/json"},
        )
        assert r.status_code == 400, raw
    assert get_alert(client, "dev-1").status_code == 404


def test_invalid_thresholds_never_overwrite_existing_rule(client):
    setup_device_with_rule(client, "dev-1", 80.0, 70.0)
    r = set_rule(client, "dev-1", 5.0, 9.0)
    assert r.status_code == 400
    rule = get_alert(client, "dev-1").json()["rule"]
    assert (rule["trigger_threshold"], rule["recovery_threshold"]) == (80.0, 70.0)


def test_rule_replace_keeps_current_alert_state(client):
    setup_device_with_rule(client, "dev-1", 80.0, 70.0)
    ingest(client, "dev-1", [
        {"event_id": "hi", "event_time": "2026-09-23T00:00:05Z", "value": 81.0},
    ])
    assert get_alert(client, "dev-1").json()["alerting"] is True

    # 替换规则本身不触发任何状态转换；告警状态保持
    r = set_rule(client, "dev-1", 90.0, 60.0)
    assert r.status_code == 200
    body = get_alert(client, "dev-1").json()
    assert body["alerting"] is True
    assert body["alarm_count"] == 1
    assert (body["rule"]["trigger_threshold"], body["rule"]["recovery_threshold"]) == (90.0, 60.0)

    # 此后按新阈值驱动：65 在新区间 (60, 90) 内，保持告警
    ingest(client, "dev-1", [
        {"event_id": "mid", "event_time": "2026-09-23T00:00:06Z", "value": 65.0},
    ])
    assert get_alert(client, "dev-1").json()["alerting"] is True
    # 降到 60（新恢复线，取等）才解除
    ingest(client, "dev-1", [
        {"event_id": "lo", "event_time": "2026-09-23T00:00:07Z", "value": 60.0},
    ])
    body = get_alert(client, "dev-1").json()
    assert body["alerting"] is False
    assert body["recovery_count"] == 1


# ---------- 状态查询 ----------

def test_get_alert_unknown_device_and_device_without_rule(client):
    assert get_alert(client, "ghost").status_code == 404
    ingest(client, "dev-1", [
        {"event_id": "e0", "event_time": "2026-09-23T00:00:00Z", "value": 50.0},
    ])
    assert get_alert(client, "dev-1").status_code == 404


def test_alert_initial_state_after_rule_creation(client):
    setup_device_with_rule(client, "dev-1")
    body = get_alert(client, "dev-1").json()
    assert body["alerting"] is False
    assert body["alarm_count"] == 0
    assert body["recovery_count"] == 0
    assert body["last_transition"] is None
    assert body["last_value"] is None
    assert body["entered_at"] is None
    assert body["cleared_at"] is None


# ---------- 滞回状态机 ----------

def test_hysteresis_state_machine_full_cycle(client):
    setup_device_with_rule(client, "dev-1", trigger=80.0, recovery=70.0)

    def push(eid, v):
        ingest(client, "dev-1", [
            {"event_id": eid, "event_time": f"2026-09-23T00:00:{int(eid):02d}Z", "value": v},
        ])

    # 中间区间：保持 not_alerting
    push("1", 75.0)
    s = get_alert(client, "dev-1").json()
    assert s["alerting"] is False and s["alarm_count"] == 0
    assert s["last_value"] == 75.0 and s["last_transition"] is None

    # 首次越过触发线（取等）：进入告警
    push("2", 80.0)
    s = get_alert(client, "dev-1").json()
    assert s["alerting"] is True
    assert s["alarm_count"] == 1 and s["recovery_count"] == 0
    assert s["last_transition"] == "entered"
    assert s["entered_at"] is not None and s["cleared_at"] is None
    assert s["last_event_id"] == "2"

    # 告警中再次出现高值：不重复告警
    push("3", 99.0)
    s = get_alert(client, "dev-1").json()
    assert s["alerting"] is True and s["alarm_count"] == 1
    assert s["last_transition"] == "entered"  # 最近转换仍是进入

    # 回落到中间区间：保持告警（防抖的核心）
    push("4", 75.0)
    s = get_alert(client, "dev-1").json()
    assert s["alerting"] is True and s["alarm_count"] == 1

    # 降至恢复线（取等）：解除
    push("5", 70.0)
    s = get_alert(client, "dev-1").json()
    assert s["alerting"] is False
    assert s["alarm_count"] == 1 and s["recovery_count"] == 1
    assert s["last_transition"] == "cleared"
    assert s["cleared_at"] is not None

    # 解除后在中间区间波动：保持正常，不重新进入
    push("6", 72.0)
    push("7", 79.9)
    s = get_alert(client, "dev-1").json()
    assert s["alerting"] is False and s["alarm_count"] == 1

    # 再次越线：第二轮告警
    push("8", 80.1)
    s = get_alert(client, "dev-1").json()
    assert s["alerting"] is True
    assert s["alarm_count"] == 2 and s["recovery_count"] == 1


def test_transitions_processed_in_batch_submission_order(client):
    setup_device_with_rule(client, "dev-1", trigger=80.0, recovery=70.0)
    # 同一批次内按提交顺序驱动：先越线、回落中间、解除
    r = ingest(client, "dev-1", [
        {"event_id": "a", "event_time": "2026-09-23T00:00:01Z", "value": 81.0},
        {"event_id": "b", "event_time": "2026-09-23T00:00:02Z", "value": 75.0},
        {"event_id": "c", "event_time": "2026-09-23T00:00:03Z", "value": 69.0},
    ])
    assert r.status_code == 200
    s = get_alert(client, "dev-1").json()
    assert s["alerting"] is False
    assert (s["alarm_count"], s["recovery_count"]) == (1, 1)
    assert s["last_event_id"] == "c"


def test_late_events_also_drive_state_machine(client):
    # 先建立规则并推进水位线封存窗口，再用迟到高值事件触发告警
    setup_device_with_rule(client, "dev-1", trigger=80.0, recovery=70.0)
    ingest(client, "dev-1", [
        {"event_id": "watermark", "event_time": "2026-09-23T00:02:00Z", "value": 50.0},
    ])
    r = ingest(client, "dev-1", [
        {"event_id": "late-hi", "event_time": "2026-09-23T00:00:10Z", "value": 95.0},
    ])
    assert r.json()["results"][0]["status"] == "late"
    s = get_alert(client, "dev-1").json()
    assert s["alerting"] is True
    assert s["alarm_count"] == 1
    assert s["last_event_id"] == "late-hi"


def test_events_ignored_when_no_rule_configured(client):
    # 没有规则的设备上报高值：不产生任何告警状态
    ingest(client, "dev-1", [
        {"event_id": "e1", "event_time": "2026-09-23T00:00:00Z", "value": 999.0},
    ])
    with db.session() as conn:
        assert conn.execute(
            "SELECT COUNT(*) AS c FROM device_alert_states WHERE device_id = 'dev-1'"
        ).fetchone()["c"] == 0
        assert conn.execute(
            "SELECT COUNT(*) AS c FROM device_alert_transitions WHERE device_id = 'dev-1'"
        ).fetchone()["c"] == 0

    # 之后配置规则；配置前的历史事件不追溯驱动（状态机只由写入时的事件驱动）
    set_rule(client, "dev-1", 80.0, 70.0)
    s = get_alert(client, "dev-1").json()
    assert s["alerting"] is False and s["alarm_count"] == 0


# ---------- 重试幂等 ----------

def test_batch_retry_does_not_duplicate_transitions(client):
    setup_device_with_rule(client, "dev-1", trigger=80.0, recovery=70.0)
    batch = [
        {"event_id": "e1", "event_time": "2026-09-23T00:00:01Z", "value": 81.0},
        {"event_id": "e2", "event_time": "2026-09-23T00:00:02Z", "value": 65.0},
    ]
    ingest(client, "dev-1", batch)
    assert get_alert(client, "dev-1").json()["alerting"] is False

    # 原样重试整个批次：全部 duplicate，计数与流水不变
    r = ingest(client, "dev-1", batch)
    assert r.json()["duplicates"] == 2
    s = get_alert(client, "dev-1").json()
    assert (s["alarm_count"], s["recovery_count"]) == (1, 1)

    with db.session() as conn:
        kinds = [
            row["kind"]
            for row in conn.execute(
                "SELECT kind FROM device_alert_transitions WHERE device_id = 'dev-1' ORDER BY id"
            )
        ]
    assert kinds == ["entered", "cleared"]


def test_duplicate_high_event_does_not_retrigger_alarm(client):
    setup_device_with_rule(client, "dev-1", trigger=80.0, recovery=70.0)
    event = [{"event_id": "hi", "event_time": "2026-09-23T00:00:01Z", "value": 90.0}]
    ingest(client, "dev-1", event)
    # 先解除
    ingest(client, "dev-1", [
        {"event_id": "lo", "event_time": "2026-09-23T00:00:02Z", "value": 60.0},
    ])
    assert get_alert(client, "dev-1").json()["alerting"] is False

    # 重试旧的高值事件：去重跳过，绝不重新进入告警
    ingest(client, "dev-1", event)
    s = get_alert(client, "dev-1").json()
    assert s["alerting"] is False
    assert s["alarm_count"] == 1


# ---------- 持久化 ----------

def test_rule_state_and_transitions_persist_across_restart(tmp_path, monkeypatch):
    db_file = tmp_path / "alerts.db"
    monkeypatch.setenv("TELEMETRY_DB_PATH", str(db_file))
    from app.main import app

    with TestClient(app) as c:
        ingest(c, "dev-1", [
            {"event_id": "e0", "event_time": "2026-09-23T00:00:00Z", "value": 50.0},
        ])
        set_rule(c, "dev-1", 80.0, 70.0)
        ingest(c, "dev-1", [
            {"event_id": "hi", "event_time": "2026-09-23T00:00:05Z", "value": 81.0},
        ])

    # 重启（同一 DB 文件）：规则与告警状态原样恢复
    with TestClient(app) as c:
        body = get_alert(c, "dev-1").json()
        assert (body["rule"]["trigger_threshold"], body["rule"]["recovery_threshold"]) == (80.0, 70.0)
        assert body["alerting"] is True
        assert body["alarm_count"] == 1 and body["recovery_count"] == 0
        assert body["last_event_id"] == "hi"

        # 重启后重试旧批次仍然幂等
        r = ingest(c, "dev-1", [
            {"event_id": "hi", "event_time": "2026-09-23T00:00:05Z", "value": 81.0},
        ])
        assert r.json()["duplicates"] == 1
        body = get_alert(c, "dev-1").json()
        assert body["alerting"] is True and body["alarm_count"] == 1

        # 状态机在重启后继续正常工作：降到恢复线解除
        ingest(c, "dev-1", [
            {"event_id": "lo", "event_time": "2026-09-23T00:00:06Z", "value": 70.0},
        ])
        body = get_alert(c, "dev-1").json()
        assert body["alerting"] is False
        assert (body["alarm_count"], body["recovery_count"]) == (1, 1)


def test_transition_unique_constraint_blocks_duplicate_directly(tmp_path, monkeypatch):
    """数据库层兜底：同一 (device_id, event_id) 无法插入第二条转换流水。"""
    db_file = tmp_path / "constraint.db"
    monkeypatch.setenv("TELEMETRY_DB_PATH", str(db_file))
    import sqlite3

    db.init_db()
    conn = sqlite3.connect(str(db_file))
    conn.execute(
        "INSERT INTO devices (device_id, created_at) VALUES ('dev-1', '2026-09-23T00:00:00Z')"
    )
    conn.execute(
        "INSERT INTO device_alert_transitions"
        " (device_id, event_id, event_time, value, kind, created_at)"
        " VALUES ('dev-1', 'e1', 1.0, 90.0, 'entered', '2026-09-23T00:00:00Z')"
    )
    conn.commit()
    import pytest

    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO device_alert_transitions"
            " (device_id, event_id, event_time, value, kind, created_at)"
            " VALUES ('dev-1', 'e1', 1.0, 90.0, 'entered', '2026-09-23T00:00:01Z')"
        )
        conn.commit()
    conn.close()
