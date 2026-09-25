"""设备告警周期查询测试。

覆盖：
- start/end 必填、end <= start、非法时间格式返回 422；
- 设备不存在 404；设备存在但无告警规则 404；
- 无转换流水 / 无相交周期返回空明细与 0；
- 按流水自增顺序配对 entered/cleared，以 created_at 为起止时刻；
- 半开区间 [start, end) 相交判定与裁剪后的区间内秒数；
- 尚未解除的周期只计至 end 与查询时刻的较早者；
- 迟到事件不按 event_time 重排（含真实 ingest 路径）；
- 同一处理时刻的零长周期不与任何区间相交；
- total_seconds 为各明细之和；接口纯只读。
"""

from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

from app import db


def ingest(client, device_id, events):
    return client.post("/ingest", json={"device_id": device_id, "events": events})


def set_rule(client, device_id, trigger=80.0, recovery=70.0):
    return client.put(
        f"/devices/{device_id}/alert-rule",
        json={"trigger_threshold": trigger, "recovery_threshold": recovery},
    )


def get_episodes(client, device_id, start, end):
    return client.get(
        f"/devices/{device_id}/alert-episodes", params={"start": start, "end": end}
    )


def setup_device_with_rule(client, device_id="dev-1"):
    ingest(client, device_id, [
        {"event_id": "seed", "event_time": "2026-09-23T00:00:00Z", "value": 50.0},
    ])
    assert set_rule(client, device_id).status_code == 200


def insert_transition(device_id, event_id, kind, created_at, event_time=0.0, value=0.0):
    """直接写入一条转换流水（created_at 可控，便于断言确定性的秒数）。"""
    with db.session() as conn:
        conn.execute(
            "INSERT INTO device_alert_transitions"
            " (device_id, event_id, event_time, value, kind, created_at)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (device_id, event_id, event_time, value, kind, created_at),
        )


def insert_closed_episode(device_id, prefix, entered_at, cleared_at, event_time=0.0):
    insert_transition(device_id, f"{prefix}-enter", "entered", entered_at, event_time)
    insert_transition(device_id, f"{prefix}-clear", "cleared", cleared_at, event_time)


# 两条已闭合周期：[00:00:10, 00:00:40] 与 [01:00:00, 01:00:30]
EP1_ENTER = "2026-09-23T00:00:10+00:00"
EP1_CLEAR = "2026-09-23T00:00:40+00:00"
EP2_ENTER = "2026-09-23T01:00:00+00:00"
EP2_CLEAR = "2026-09-23T01:00:30+00:00"


def setup_two_episodes(client, device_id="dev-1"):
    setup_device_with_rule(client, device_id)
    insert_closed_episode(device_id, "ep1", EP1_ENTER, EP1_CLEAR)
    insert_closed_episode(device_id, "ep2", EP2_ENTER, EP2_CLEAR)


# ---------- 参数校验 ----------

def test_start_and_end_required(client):
    setup_device_with_rule(client)
    assert client.get("/devices/dev-1/alert-episodes").status_code == 422
    assert client.get(
        "/devices/dev-1/alert-episodes", params={"start": "2026-09-23T00:00:00Z"}
    ).status_code == 422
    assert client.get(
        "/devices/dev-1/alert-episodes", params={"end": "2026-09-23T01:00:00Z"}
    ).status_code == 422


def test_end_must_be_strictly_greater_than_start(client):
    setup_device_with_rule(client)
    r = get_episodes(client, "dev-1", "2026-09-23T00:00:00Z", "2026-09-23T00:00:00Z")
    assert r.status_code == 422
    r = get_episodes(client, "dev-1", "2026-09-23T01:00:00Z", "2026-09-23T00:00:00Z")
    assert r.status_code == 422


def test_invalid_datetime_returns_422(client):
    setup_device_with_rule(client)
    r = get_episodes(client, "dev-1", "not-a-date", "2026-09-23T00:00:00Z")
    assert r.status_code == 422


def test_naive_datetimes_treated_as_utc(client):
    setup_two_episodes(client)
    r = get_episodes(client, "dev-1", "2026-09-23T00:00:00", "2026-09-23T02:00:00")
    assert r.status_code == 200
    assert r.json()["total_seconds"] == 60.0


# ---------- 404 ----------

def test_unknown_device_returns_404(client):
    r = get_episodes(client, "ghost", "2026-09-23T00:00:00Z", "2026-09-23T01:00:00Z")
    assert r.status_code == 404


def test_device_without_rule_returns_404(client):
    ingest(client, "dev-1", [
        {"event_id": "e0", "event_time": "2026-09-23T00:00:00Z", "value": 50.0},
    ])
    r = get_episodes(client, "dev-1", "2026-09-23T00:00:00Z", "2026-09-23T01:00:00Z")
    assert r.status_code == 404


# ---------- 空结果 ----------

def test_no_transitions_returns_empty_and_zero(client):
    setup_device_with_rule(client)
    r = get_episodes(client, "dev-1", "2026-09-23T00:00:00Z", "2026-09-23T01:00:00Z")
    assert r.status_code == 200
    body = r.json()
    assert body["device_id"] == "dev-1"
    assert body["episodes"] == []
    assert body["total_seconds"] == 0


def test_no_intersecting_episode_returns_empty_and_zero(client):
    setup_two_episodes(client)
    # 区间落在两个周期之间的空档
    r = get_episodes(client, "dev-1", "2026-09-23T00:30:00Z", "2026-09-23T00:50:00Z")
    body = r.json()
    assert body["episodes"] == []
    assert body["total_seconds"] == 0


# ---------- 配对与裁剪 ----------

def test_closed_episodes_pairing_and_total(client):
    setup_two_episodes(client)
    r = get_episodes(client, "dev-1", "2026-09-23T00:00:00Z", "2026-09-23T02:00:00Z")
    assert r.status_code == 200
    body = r.json()
    assert body["start"] == "2026-09-23T00:00:00Z"
    assert body["end"] == "2026-09-23T02:00:00Z"
    assert len(body["episodes"]) == 2
    ep1, ep2 = body["episodes"]
    assert ep1["entered_at"] == "2026-09-23T00:00:10Z"
    assert ep1["cleared_at"] == "2026-09-23T00:00:40Z"
    assert ep1["seconds"] == 30.0
    assert ep2["entered_at"] == "2026-09-23T01:00:00Z"
    assert ep2["cleared_at"] == "2026-09-23T01:00:30Z"
    assert ep2["seconds"] == 30.0
    assert body["total_seconds"] == 60.0
    # 总秒数为明细之和
    assert body["total_seconds"] == sum(ep["seconds"] for ep in body["episodes"])


def test_episode_clipped_to_interval(client):
    setup_two_episodes(client)
    # 裁剪周期前端：[00:00:20, 00:30:00) 只覆盖第一个周期的后 20 秒
    r = get_episodes(client, "dev-1", "2026-09-23T00:00:20Z", "2026-09-23T00:30:00Z")
    body = r.json()
    assert len(body["episodes"]) == 1
    ep = body["episodes"][0]
    # 进入/解除时刻仍给原始流水时刻，秒数为裁剪后的区间内秒数
    assert ep["entered_at"] == "2026-09-23T00:00:10Z"
    assert ep["cleared_at"] == "2026-09-23T00:00:40Z"
    assert ep["seconds"] == 20.0
    assert body["total_seconds"] == 20.0

    # 裁剪周期末端：[00:00:00, 00:00:25) 只覆盖前 15 秒
    r = get_episodes(client, "dev-1", "2026-09-23T00:00:00Z", "2026-09-23T00:00:25Z")
    body = r.json()
    assert len(body["episodes"]) == 1
    assert body["episodes"][0]["seconds"] == 15.0
    assert body["total_seconds"] == 15.0


def test_half_open_interval_boundaries(client):
    setup_two_episodes(client)
    # start 恰好等于 cleared_at：不相交
    r = get_episodes(client, "dev-1", "2026-09-23T00:00:40Z", "2026-09-23T00:50:00Z")
    assert r.json()["episodes"] == []
    # end 恰好等于 entered_at：不相交
    r = get_episodes(client, "dev-1", "2026-09-22T23:00:00Z", "2026-09-23T00:00:10Z")
    assert r.json()["episodes"] == []
    # 区间与周期完全重合：整段计入
    r = get_episodes(client, "dev-1", "2026-09-23T00:00:10Z", "2026-09-23T00:00:40Z")
    body = r.json()
    assert len(body["episodes"]) == 1
    assert body["episodes"][0]["seconds"] == 30.0


def test_pairing_follows_transition_log_order(client):
    """真实 ingest 路径：两轮进入/解除产生两个周期，按流水顺序排列。"""
    setup_device_with_rule(client)
    for i, v in enumerate((90.0, 60.0, 90.0, 60.0)):
        ingest(client, "dev-1", [
            {
                "event_id": f"e{i}",
                "event_time": f"2026-09-23T00:00:0{i}Z",
                "value": v,
            }
        ])
    r = get_episodes(client, "dev-1", "2020-01-01T00:00:00Z", "2100-01-01T00:00:00Z")
    body = r.json()
    assert len(body["episodes"]) == 2
    ep1, ep2 = body["episodes"]
    assert ep1["entered_at"] < ep1["cleared_at"]
    assert ep2["entered_at"] < ep2["cleared_at"]
    # 第二个周期在第一轮解除之后才开始（流水自增顺序）
    assert ep2["entered_at"] >= ep1["cleared_at"]
    for ep in body["episodes"]:
        assert ep["seconds"] > 0
    assert body["total_seconds"] == sum(ep["seconds"] for ep in body["episodes"])


def test_zero_length_episode_intersects_nothing(client):
    """同一处理时刻进入并解除（同一批次）的零长周期不与任何区间相交。"""
    setup_device_with_rule(client)
    ingest(client, "dev-1", [
        {"event_id": "hi", "event_time": "2026-09-23T00:00:01Z", "value": 90.0},
        {"event_id": "lo", "event_time": "2026-09-23T00:00:02Z", "value": 60.0},
    ])
    r = get_episodes(client, "dev-1", "2020-01-01T00:00:00Z", "2100-01-01T00:00:00Z")
    body = r.json()
    assert body["episodes"] == []
    assert body["total_seconds"] == 0


# ---------- 未解除周期 ----------

def test_open_episode_counts_until_end_when_end_in_past(client):
    setup_device_with_rule(client)
    insert_transition(
        "dev-1", "enter", "entered", "2026-09-23T00:00:10+00:00"
    )
    # end 在过去（早于查询时刻）：计至 end
    r = get_episodes(client, "dev-1", "2026-09-23T00:00:00Z", "2026-09-23T00:01:00Z")
    body = r.json()
    assert len(body["episodes"]) == 1
    ep = body["episodes"][0]
    assert ep["entered_at"] == "2026-09-23T00:00:10Z"
    assert ep["cleared_at"] is None
    assert ep["seconds"] == 50.0
    assert body["total_seconds"] == 50.0


def test_open_episode_counts_until_query_time_when_end_in_future(client):
    setup_device_with_rule(client)
    entered = datetime.now(timezone.utc) - timedelta(seconds=120)
    insert_transition("dev-1", "enter", "entered", entered.isoformat())
    # end 在未来：只计至查询时刻（约 120 秒，随执行时间略微增长）
    r = get_episodes(client, "dev-1", "2020-01-01T00:00:00Z", "2100-01-01T00:00:00Z")
    body = r.json()
    assert len(body["episodes"]) == 1
    ep = body["episodes"][0]
    assert ep["cleared_at"] is None
    assert 110.0 < ep["seconds"] < 180.0
    assert body["total_seconds"] == ep["seconds"]


def test_open_episode_not_counted_beyond_now_into_future_interval(client):
    setup_device_with_rule(client)
    entered = datetime.now(timezone.utc) - timedelta(seconds=10)
    insert_transition("dev-1", "enter", "entered", entered.isoformat())
    # 查询区间整体在未来：未解除的周期尚未延伸到该区间，不相交
    r = get_episodes(client, "dev-1", "2100-01-01T00:00:00Z", "2100-01-02T00:00:00Z")
    body = r.json()
    assert body["episodes"] == []
    assert body["total_seconds"] == 0


# ---------- 迟到事件不按 event_time 重排 ----------

def test_pairing_uses_created_at_not_event_time(client):
    """流水中 event_time 乱序时仍按 created_at 配对与计时。"""
    setup_device_with_rule(client)
    # cleared 的 event_time 早于 entered 的 event_time，但 created_at 更晚
    insert_transition(
        "dev-1", "enter", "entered", "2026-09-23T00:00:50+00:00", event_time=200.0
    )
    insert_transition(
        "dev-1", "clear", "cleared", "2026-09-23T00:01:10+00:00", event_time=100.0
    )
    r = get_episodes(client, "dev-1", "2026-09-23T00:00:00Z", "2026-09-23T01:00:00Z")
    body = r.json()
    assert len(body["episodes"]) == 1
    ep = body["episodes"][0]
    assert ep["entered_at"] == "2026-09-23T00:00:50Z"
    assert ep["cleared_at"] == "2026-09-23T00:01:10Z"
    assert ep["seconds"] == 20.0
    assert body["total_seconds"] == 20.0


def test_late_events_keep_ingest_order_in_episodes(client):
    """真实 ingest 路径：迟到事件按到达顺序驱动状态机，不按 event_time 重排。"""
    setup_device_with_rule(client)
    # 推进水位线，封存 00:04:30 之前的窗口
    ingest(client, "dev-1", [
        {"event_id": "wm", "event_time": "2026-09-23T00:05:00Z", "value": 50.0},
    ])
    # 迟到高值（event_time 00:02:00）先到达：进入告警
    r = ingest(client, "dev-1", [
        {"event_id": "late-hi", "event_time": "2026-09-23T00:02:00Z", "value": 90.0},
    ])
    assert r.json()["results"][0]["status"] == "late"
    # 迟到低值（event_time 00:01:00，更早）后到达：解除告警
    r = ingest(client, "dev-1", [
        {"event_id": "late-lo", "event_time": "2026-09-23T00:01:00Z", "value": 60.0},
    ])
    assert r.json()["results"][0]["status"] == "late"

    r = get_episodes(client, "dev-1", "2020-01-01T00:00:00Z", "2100-01-01T00:00:00Z")
    body = r.json()
    # 若按 event_time 重排，clear(00:01:00) 会排在 enter(00:02:00) 之前；
    # 实际按流水顺序配对：先进入后解除，周期为正时长
    assert len(body["episodes"]) == 1
    ep = body["episodes"][0]
    assert ep["entered_at"] < ep["cleared_at"]
    assert ep["seconds"] > 0
    assert body["total_seconds"] == ep["seconds"]


# ---------- 只读 ----------

def test_query_is_read_only(client):
    setup_two_episodes(client)
    before_alert = client.get("/devices/dev-1/alert").json()
    with db.session() as conn:
        before_count = conn.execute(
            "SELECT COUNT(*) AS c FROM device_alert_transitions WHERE device_id = 'dev-1'"
        ).fetchone()["c"]

    r = get_episodes(client, "dev-1", "2026-09-23T00:00:00Z", "2026-09-23T02:00:00Z")
    assert r.status_code == 200

    assert client.get("/devices/dev-1/alert").json() == before_alert
    with db.session() as conn:
        after_count = conn.execute(
            "SELECT COUNT(*) AS c FROM device_alert_transitions WHERE device_id = 'dev-1'"
        ).fetchone()["c"]
    assert after_count == before_count
