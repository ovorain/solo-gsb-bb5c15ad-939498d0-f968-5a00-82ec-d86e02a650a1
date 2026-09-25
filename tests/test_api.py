from fastapi.testclient import TestClient


def ingest(client, device_id, events):
    return client.post("/ingest", json={"device_id": device_id, "events": events})


def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


def test_ingest_accepted_and_window_aggregation(client):
    r = ingest(client, "dev-1", [
        {"event_id": "e1", "event_time": "2026-09-22T00:00:10Z", "value": 1.0},
        {"event_id": "e2", "event_time": "2026-09-22T00:00:20Z", "value": 2.0},
        {"event_id": "e3", "event_time": "2026-09-22T00:01:05Z", "value": 4.0},
    ])
    assert r.status_code == 200
    body = r.json()
    assert body["accepted"] == 3
    assert body["watermark"] == "2026-09-22T00:01:05Z"

    windows = client.get("/devices/dev-1/windows").json()
    assert len(windows) == 2
    w0, w1 = windows
    assert w0["window_start"] == "2026-09-22T00:00:00Z"
    assert w0["status"] == "open"
    assert (w0["count"], w0["sum"], w0["avg"], w0["min"], w0["max"]) == (2, 3.0, 1.5, 1.0, 2.0)
    assert w0["correction_count"] == 0
    assert (w1["count"], w1["sum"]) == (1, 4.0)


def test_duplicate_event_id_is_idempotent(client):
    events = [{"event_id": "e1", "event_time": "2026-09-22T00:00:10Z", "value": 1.0}]
    assert ingest(client, "dev-1", events).json()["accepted"] == 1

    r = ingest(client, "dev-1", events)
    body = r.json()
    assert body["duplicates"] == 1
    assert body["results"][0]["status"] == "duplicate"

    # 聚合结果不受重复提交影响
    windows = client.get("/devices/dev-1/windows").json()
    assert windows[0]["count"] == 1
    assert windows[0]["sum"] == 1.0


def test_late_event_after_window_sealed_creates_correction(client):
    # 窗口 [00:00, 00:01)，允许迟到 30s
    ingest(client, "dev-1", [
        {"event_id": "e1", "event_time": "2026-09-22T00:00:10Z", "value": 1.0},
    ])
    # 推进水位线到 00:02:00，封存窗口 [00:00, 00:01)（00:01:00 + 30s <= 00:02:00）
    ingest(client, "dev-1", [
        {"event_id": "e2", "event_time": "2026-09-22T00:02:00Z", "value": 10.0},
    ])
    windows = client.get("/devices/dev-1/windows").json()
    assert windows[0]["status"] == "sealed"
    assert windows[0]["sealed_at"] is not None

    # 迟到数据落入已封存窗口
    r = ingest(client, "dev-1", [
        {"event_id": "e3", "event_time": "2026-09-22T00:00:30Z", "value": 5.0},
    ])
    body = r.json()
    assert body["late"] == 1
    assert body["results"][0]["status"] == "late"

    windows = client.get("/devices/dev-1/windows").json()
    w0 = windows[0]
    assert w0["status"] == "sealed"
    assert w0["count"] == 2
    assert w0["sum"] == 6.0
    assert w0["correction_count"] == 1
    assert w0["late_event_count"] == 1

    corrections = client.get("/devices/dev-1/corrections").json()
    assert len(corrections) == 1
    assert corrections[0]["event_id"] == "e3"
    assert corrections[0]["value"] == 5.0
    assert corrections[0]["window_start"] == "2026-09-22T00:00:00Z"


def test_event_within_allowed_lateness_is_not_late(client):
    ingest(client, "dev-1", [
        {"event_id": "e1", "event_time": "2026-09-22T00:00:10Z", "value": 1.0},
        # 水位线推进到 00:01:20，窗口 [00:00,00:01) 封存阈值为 00:01:30，尚未封存
        {"event_id": "e2", "event_time": "2026-09-22T00:01:20Z", "value": 2.0},
    ])
    r = ingest(client, "dev-1", [
        {"event_id": "e3", "event_time": "2026-09-22T00:00:50Z", "value": 3.0},
    ])
    assert r.json()["results"][0]["status"] == "accepted"
    windows = client.get("/devices/dev-1/windows").json()
    assert windows[0]["status"] == "open"
    assert windows[0]["count"] == 2
    assert windows[0]["correction_count"] == 0


def test_device_config_and_window_size_lock(client):
    r = client.put("/devices/dev-1/config", json={
        "window_size_seconds": 300,
        "allowed_lateness_seconds": 60,
    })
    assert r.status_code == 200
    assert r.json()["window_size_seconds"] == 300
    assert r.json()["watermark"] is None

    # 允许迟到时间可随时调整
    r = client.put("/devices/dev-1/config", json={
        "window_size_seconds": 300,
        "allowed_lateness_seconds": 120,
    })
    assert r.json()["allowed_lateness_seconds"] == 120

    ingest(client, "dev-1", [
        {"event_id": "e1", "event_time": "2026-09-22T00:00:10Z", "value": 1.0},
    ])
    # 已有数据后禁止修改窗口大小
    r = client.put("/devices/dev-1/config", json={
        "window_size_seconds": 60,
        "allowed_lateness_seconds": 120,
    })
    assert r.status_code == 409


def test_device_info_and_404(client):
    assert client.get("/devices/ghost").status_code == 404
    assert client.get("/devices/ghost/windows").status_code == 404

    ingest(client, "dev-1", [
        {"event_id": "e1", "event_time": "2026-09-22T00:00:10Z", "value": 1.0},
        {"event_id": "e2", "event_time": "2026-09-22T00:02:00Z", "value": 2.0},
    ])
    info = client.get("/devices/dev-1").json()
    assert info["watermark"] == "2026-09-22T00:02:00Z"
    assert info["event_count"] == 2
    assert info["sealed_windows"] == 1
    assert info["open_windows"] == 1


def test_watermark_persisted_across_restarts(client, tmp_path, monkeypatch):
    ingest(client, "dev-1", [
        {"event_id": "e1", "event_time": "2026-09-22T00:05:00Z", "value": 1.0},
    ])
    # 模拟重启：用同一个数据库文件重建 app
    from app.main import app

    with TestClient(app) as c2:
        info = c2.get("/devices/dev-1").json()
        assert info["watermark"] == "2026-09-22T00:05:00Z"
        # 水位线不回退：旧时间戳的事件仍按迟到判定
        r = ingest(c2, "dev-1", [
            {"event_id": "e2", "event_time": "2026-09-22T00:00:10Z", "value": 9.0},
        ])
        assert r.json()["results"][0]["status"] == "late"
