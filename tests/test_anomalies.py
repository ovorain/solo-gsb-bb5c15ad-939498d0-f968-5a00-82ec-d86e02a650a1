"""封存窗口稳健异常检测（中位数 / MAD）测试。"""

import pytest


def ingest(client, device_id, events):
    return client.post("/ingest", json={"device_id": device_id, "events": events})


def seal_first_window(client, device_id="dev-1", at="2026-09-22T00:02:00Z", value=1.0,
                      event_id="seal"):
    """推进水位线到 00:02:00，封存默认设备窗口 [00:00, 00:01)（lateness=30s）。"""
    return ingest(client, device_id, [
        {"event_id": event_id, "event_time": at, "value": value},
    ])


def anomalies(client, device_id="dev-1", window_start="2026-09-22T00:00:00Z", **params):
    return client.get(
        f"/devices/{device_id}/anomalies",
        params={"window_start": window_start, **params},
    )


def test_median_mad_flags_extreme_outlier(client):
    # 窗口 [00:00,00:01)：中位数 10.2，MAD 0.2；50.0 的修正 z 分数远超 3.5
    ingest(client, "dev-1", [
        {"event_id": "e1", "event_time": "2026-09-22T00:00:05Z", "value": 9.8},
        {"event_id": "e2", "event_time": "2026-09-22T00:00:15Z", "value": 10.0},
        {"event_id": "e3", "event_time": "2026-09-22T00:00:25Z", "value": 10.2},
        {"event_id": "e4", "event_time": "2026-09-22T00:00:35Z", "value": 10.4},
        {"event_id": "e5", "event_time": "2026-09-22T00:00:45Z", "value": 50.0},
    ])
    seal_first_window(client)

    r = anomalies(client)
    assert r.status_code == 200
    body = r.json()
    assert len(body) == 1
    out = body[0]
    assert out["event_id"] == "e5"
    assert out["event_time"] == "2026-09-22T00:00:45Z"
    assert out["value"] == 50.0
    # 0.6745 * |50 - 10.2| / 0.2
    assert out["score"] == pytest.approx(0.6745 * 39.8 / 0.2)

    # 稳健分数对其余近中位数事件不报警（最大仅 ~1.349）
    ids = {a["event_id"] for a in body}
    assert ids == {"e5"}


def test_threshold_param_filters_results(client):
    ingest(client, "dev-1", [
        {"event_id": "e1", "event_time": "2026-09-22T00:00:05Z", "value": 9.8},
        {"event_id": "e2", "event_time": "2026-09-22T00:00:15Z", "value": 10.0},
        {"event_id": "e3", "event_time": "2026-09-22T00:00:25Z", "value": 10.2},
        {"event_id": "e4", "event_time": "2026-09-22T00:00:35Z", "value": 10.4},
        {"event_id": "e5", "event_time": "2026-09-22T00:00:45Z", "value": 50.0},
    ])
    seal_first_window(client)

    # 阈值高于离群分数 => 无异常
    assert anomalies(client, threshold=200).json() == []

    # 阈值 1.0：离群点必中，9.8（1.349）也越线，共 2 条
    body = anomalies(client, threshold=1.0).json()
    assert {a["event_id"] for a in body} == {"e1", "e5"}

    # 阈值 1.5：只有离群点（其余最高 1.349）
    body = anomalies(client, threshold=1.5).json()
    assert {a["event_id"] for a in body} == {"e5"}

    # 结果按 event_time, event_id 稳定排序
    assert [a["event_id"] for a in anomalies(client, threshold=1.0).json()] == ["e1", "e5"]


def test_threshold_must_be_positive(client):
    ingest(client, "dev-1", [
        {"event_id": "e1", "event_time": "2026-09-22T00:00:05Z", "value": 10.0},
    ])
    seal_first_window(client)

    assert anomalies(client, threshold=0).status_code == 422
    assert anomalies(client, threshold=-3.5).status_code == 422


def test_zero_mad_all_non_median_values_are_anomalies(client):
    # 4 个事件中 3 个等于中位数 10.0 => MAD=0，50.0 无条件判为异常，score 为 null
    ingest(client, "dev-1", [
        {"event_id": "e1", "event_time": "2026-09-22T00:00:05Z", "value": 10.0},
        {"event_id": "e2", "event_time": "2026-09-22T00:00:15Z", "value": 10.0},
        {"event_id": "e3", "event_time": "2026-09-22T00:00:25Z", "value": 10.0},
        {"event_id": "e4", "event_time": "2026-09-22T00:00:35Z", "value": 50.0},
    ])
    seal_first_window(client)

    body = anomalies(client).json()
    assert len(body) == 1
    assert body[0]["event_id"] == "e4"
    assert body[0]["value"] == 50.0
    assert body[0]["score"] is None

    # 阈值在 MAD=0 分支不影响判定
    assert {a["event_id"] for a in anomalies(client, threshold=100).json()} == {"e4"}


def test_zero_mad_all_equal_means_no_anomaly(client):
    ingest(client, "dev-1", [
        {"event_id": "e1", "event_time": "2026-09-22T00:00:05Z", "value": 7.0},
        {"event_id": "e2", "event_time": "2026-09-22T00:00:15Z", "value": 7.0},
        {"event_id": "e3", "event_time": "2026-09-22T00:00:25Z", "value": 7.0},
    ])
    seal_first_window(client)
    assert anomalies(client).json() == []


def test_single_event_window_has_no_anomaly(client):
    ingest(client, "dev-1", [
        {"event_id": "e1", "event_time": "2026-09-22T00:00:05Z", "value": 42.0},
    ])
    seal_first_window(client)
    assert anomalies(client).json() == []


def test_open_window_returns_409(client):
    ingest(client, "dev-1", [
        {"event_id": "e1", "event_time": "2026-09-22T00:00:05Z", "value": 10.0},
    ])
    # 未推进水位线，窗口仍 open
    r = anomalies(client)
    assert r.status_code == 409


def test_missing_device_or_window_returns_404(client):
    # 设备不存在
    assert anomalies(client, device_id="ghost").status_code == 404

    ingest(client, "dev-1", [
        {"event_id": "e1", "event_time": "2026-09-22T00:00:05Z", "value": 10.0},
    ])
    seal_first_window(client)

    # 设备存在但指定窗口从无数据
    r = anomalies(client, window_start="2026-09-22T00:05:00Z")
    assert r.status_code == 404


def test_late_events_are_included_in_detection(client):
    ingest(client, "dev-1", [
        {"event_id": "e1", "event_time": "2026-09-22T00:00:05Z", "value": 10.0},
        {"event_id": "e2", "event_time": "2026-09-22T00:00:15Z", "value": 10.0},
        {"event_id": "e3", "event_time": "2026-09-22T00:00:25Z", "value": 11.0},
    ])
    seal_first_window(client)
    # 封存后迟到的极端值落入该窗口
    r = ingest(client, "dev-1", [
        {"event_id": "e4", "event_time": "2026-09-22T00:00:08Z", "value": 100.0},
    ])
    assert r.json()["results"][0]["status"] == "late"

    body = anomalies(client).json()
    assert [a["event_id"] for a in body] == ["e4"]
    assert body[0]["score"] == pytest.approx(0.6745 * 89.5 / 0.5)


def test_detection_is_read_only(client):
    ingest(client, "dev-1", [
        {"event_id": "e1", "event_time": "2026-09-22T00:00:05Z", "value": 9.8},
        {"event_id": "e2", "event_time": "2026-09-22T00:00:15Z", "value": 10.2},
        {"event_id": "e3", "event_time": "2026-09-22T00:00:25Z", "value": 50.0},
    ])
    seal_first_window(client)

    before = client.get("/devices/dev-1/windows").json()
    for threshold in (None, 1.0, 100.0):
        params = {} if threshold is None else {"threshold": threshold}
        anomalies(client, **params)
    # 409 / 404 路径同样不得写库
    ingest(client, "dev-2", [
        {"event_id": "e1", "event_time": "2026-09-22T00:00:05Z", "value": 10.0},
    ])
    anomalies(client, device_id="dev-2")
    anomalies(client, window_start="2026-09-22T00:05:00Z")

    after = client.get("/devices/dev-1/windows").json()
    assert after == before


def test_window_start_required(client):
    # 缺少必填 window_start => 422
    r = client.get("/devices/dev-1/anomalies", params={"threshold": 3.5})
    assert r.status_code == 422
