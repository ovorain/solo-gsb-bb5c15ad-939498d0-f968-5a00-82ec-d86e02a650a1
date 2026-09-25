"""旧库组成员配置漂移纠偏测试（PUT /devices/{id}/config）。

旧版本服务未锁定组成员配置，可能留下"已入组但配置漂移"的设备：
- 漂移只落在配置字段（如允许迟到秒数被改、窗口大小被改但尚无窗口）：
  提交与所有所属组一致的配置应允许纠正（200），设备配置回到组配置；
- 漂移已落到窗口数据（漂移期间按错误窗口大小落盘了起点错格/跨度不符的窗口）：
  提交与组一致的配置也必须拒绝（409），且设备配置、窗口与组汇总全部保持原样。

健康库中组成员的窗口必然与组配置同网格，新核对不改变既有行为；
未入组设备仍按现有规则处理（新核对不适用）。
"""

import sqlite3
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from app import db

NOW = "2026-09-22T00:05:00+00:00"


def epoch(hour=0, minute=0, second=0):
    return datetime(2026, 9, 22, hour, minute, second, tzinfo=timezone.utc).timestamp()


def make_legacy_db(tmp_path, monkeypatch):
    """建好库文件并指向它，返回路径；随后直接 SQL 写入旧版漂移数据。"""
    db_file = tmp_path / "legacy.db"
    monkeypatch.setenv("TELEMETRY_DB_PATH", str(db_file))
    db.init_db()
    return db_file


def insert_device(conn, device_id, size, lateness, watermark=None):
    conn.execute(
        "INSERT INTO devices (device_id, window_size_seconds, allowed_lateness_seconds,"
        " watermark, created_at) VALUES (?, ?, ?, ?, ?)",
        (device_id, size, lateness, watermark, NOW),
    )


def insert_window(conn, device_id, start, end, status="sealed"):
    conn.execute(
        "INSERT INTO windows (device_id, window_start, window_end, count, sum, min, max,"
        " status, correction_count, late_event_count, sealed_at)"
        " VALUES (?, ?, ?, 2, 15.0, 5.0, 10.0, ?, 0, 0, ?)",
        (device_id, start, end, status, NOW if status == "sealed" else None),
    )


def insert_group(conn, group_id, members, size=60, lateness=30, aggregation="sum"):
    conn.execute(
        "INSERT INTO device_groups (group_id, window_size_seconds, allowed_lateness_seconds,"
        " aggregation, created_at) VALUES (?, ?, ?, ?, ?)",
        (group_id, size, lateness, aggregation, NOW),
    )
    conn.executemany(
        "INSERT INTO device_group_members (group_id, device_id, position) VALUES (?, ?, ?)",
        [(group_id, d, i) for i, d in enumerate(members)],
    )


def snapshot_tables():
    """直读库快照：设备配置、窗口、组汇总（绕过 API）。"""
    conn = sqlite3.connect(db.db_path())
    try:
        devices = conn.execute(
            "SELECT device_id, window_size_seconds, allowed_lateness_seconds"
            " FROM devices ORDER BY device_id"
        ).fetchall()
        windows = conn.execute(
            "SELECT * FROM windows ORDER BY device_id, window_start"
        ).fetchall()
        group_windows = conn.execute(
            "SELECT * FROM group_windows ORDER BY group_id, window_start"
        ).fetchall()
    finally:
        conn.close()
    return devices, windows, group_windows


def put_config(client, device_id, size, lateness):
    return client.put(
        f"/devices/{device_id}/config",
        json={"window_size_seconds": size, "allowed_lateness_seconds": lateness},
    )


def test_config_only_drift_corrected_back_to_group_config(tmp_path, monkeypatch):
    # 旧库：d1 已入组（组 60/30），允许迟到秒数被漂移成 45；窗口仍在 60s 网格上
    make_legacy_db(tmp_path, monkeypatch)
    s = epoch(0, 0, 0)
    with db.session() as conn:
        insert_device(conn, "d1", 60, 45, watermark=s + 120)
        insert_device(conn, "d2", 60, 30, watermark=s + 120)
        insert_group(conn, "g1", ("d1", "d2"))
        insert_window(conn, "d1", s, s + 60)
        insert_window(conn, "d2", s, s + 60)
        conn.execute(
            "INSERT INTO group_windows (group_id, window_start, window_end, status, value,"
            " member_count, sealed_member_count, revision, correction_sig, ready_at,"
            " created_at, updated_at)"
            " VALUES ('g1', ?, ?, 'ready', 30.0, 2, 2, 1, 0, ?, ?, ?)",
            (s, s + 60, NOW, NOW, NOW),
        )
    before = snapshot_tables()

    from app.main import app

    with TestClient(app) as client:
        r = put_config(client, "d1", 60, 30)
        assert r.status_code == 200
        body = r.json()
        assert body["window_size_seconds"] == 60
        assert body["allowed_lateness_seconds"] == 30

    after = snapshot_tables()
    # 仅配置字段被纠正回组配置；窗口与组汇总原样不动
    assert after[0] == [("d1", 60, 30), ("d2", 60, 30)]
    assert after[1] == before[1]
    assert after[2] == before[2]


def test_window_span_mismatch_blocks_correction(tmp_path, monkeypatch):
    # 旧库：d1 在组内把窗口大小漂移成 120 并已落盘 120s 跨度的窗口
    # （起点 00:00 同时能被 60 整除——只有跨度不符，专门覆盖跨度核对）
    make_legacy_db(tmp_path, monkeypatch)
    s = epoch(0, 0, 0)
    with db.session() as conn:
        insert_device(conn, "d1", 120, 30, watermark=s + 240)
        insert_device(conn, "d2", 60, 30, watermark=s + 120)
        insert_group(conn, "g1", ("d1", "d2"))
        insert_window(conn, "d1", s, s + 120)          # 漂移期间落盘的异跨度窗口
        insert_window(conn, "d2", s, s + 60)
        conn.execute(
            "INSERT INTO group_windows (group_id, window_start, window_end, status, value,"
            " member_count, sealed_member_count, revision, correction_sig, ready_at,"
            " created_at, updated_at)"
            " VALUES ('g1', ?, ?, 'pending', NULL, 2, 1, 0, 0, NULL, ?, ?)",
            (s, s + 60, NOW, NOW),
        )
    before = snapshot_tables()

    from app.main import app

    with TestClient(app) as client:
        r = put_config(client, "d1", 60, 30)
        assert r.status_code == 409
        assert "incompatible" in r.json()["detail"]
        # 重复提交依旧拒绝
        assert put_config(client, "d1", 60, 30).status_code == 409

    # 设备配置、窗口、组汇总全部保持原样
    assert snapshot_tables() == before


def test_window_start_misalignment_blocks_correction(tmp_path, monkeypatch):
    # 旧库：d1 漂移成 90s 窗口，落盘窗口起点不在 60s 网格上（90 % 60 = 30）
    make_legacy_db(tmp_path, monkeypatch)
    t = epoch(0, 1, 30)  # 90s 网格上的起点，不对齐 60s
    with db.session() as conn:
        insert_device(conn, "d1", 90, 30, watermark=t + 180)
        insert_device(conn, "d2", 60, 30)
        insert_group(conn, "g1", ("d1", "d2"))
        insert_window(conn, "d1", t, t + 90)
    before = snapshot_tables()

    from app.main import app

    with TestClient(app) as client:
        r = put_config(client, "d1", 60, 30)
        assert r.status_code == 409

    assert snapshot_tables() == before


def test_size_drift_without_windows_is_correctable(tmp_path, monkeypatch):
    # 旧库：d1 漂移成 120/45 但尚无任何窗口——漂移只在配置字段，允许纠正
    make_legacy_db(tmp_path, monkeypatch)
    with db.session() as conn:
        insert_device(conn, "d1", 120, 45)
        insert_device(conn, "d2", 60, 30)
        insert_group(conn, "g1", ("d1", "d2"))

    from app.main import app

    with TestClient(app) as client:
        r = put_config(client, "d1", 60, 30)
        assert r.status_code == 200
        assert r.json()["window_size_seconds"] == 60
        assert r.json()["allowed_lateness_seconds"] == 30

    devices, _, _ = snapshot_tables()
    assert devices == [("d1", 60, 30), ("d2", 60, 30)]


def test_group_mismatch_still_rejected_for_drifted_member(tmp_path, monkeypatch):
    # 旧库漂移设备：提交与组不一致的值仍按既有规则 409（不是窗口核对命中）
    make_legacy_db(tmp_path, monkeypatch)
    with db.session() as conn:
        insert_device(conn, "d1", 120, 45)
        insert_device(conn, "d2", 60, 30)
        insert_group(conn, "g1", ("d1", "d2"))
    before = snapshot_tables()

    from app.main import app

    with TestClient(app) as client:
        # 与组配置不同的窗口大小 / 允许迟到秒数都拒绝
        assert put_config(client, "d1", 60, 40).status_code == 409
        assert put_config(client, "d1", 120, 30).status_code == 409
        assert put_config(client, "d1", 120, 45).status_code == 409

    assert snapshot_tables() == before


def test_multi_group_member_windows_checked_against_shared_grid(tmp_path, monkeypatch):
    # d1 同属两个 60/30 组，提交值与两组都一致，但漂移窗口不相容 → 409
    make_legacy_db(tmp_path, monkeypatch)
    s = epoch(0, 0, 0)
    with db.session() as conn:
        insert_device(conn, "d1", 120, 30, watermark=s + 240)
        insert_device(conn, "d2", 60, 30)
        insert_group(conn, "g1", ("d1", "d2"))
        insert_group(conn, "g2", ("d1", "d2"), aggregation="avg")
        insert_window(conn, "d1", s, s + 120)
    before = snapshot_tables()

    from app.main import app

    with TestClient(app) as client:
        assert put_config(client, "d1", 60, 30).status_code == 409

    assert snapshot_tables() == before


def test_healthy_grouped_device_with_windows_idempotent_put_ok(client):
    # 健康库回归：已入组、窗口都在组网格上，幂等重复提交不受新核对影响
    for d in ("d1", "d2"):
        assert client.post("/ingest", json={
            "device_id": d,
            "events": [
                {"event_id": "e1", "event_time": "2026-09-22T00:00:10Z", "value": 10.0},
                {"event_id": "s1", "event_time": "2026-09-22T00:02:00Z", "value": 1.0},
            ],
        }).status_code == 200
    r = client.post("/groups", json={
        "group_id": "g1",
        "member_ids": ["d1", "d2"],
        "aggregation": "sum",
        "window_size_seconds": 60,
        "allowed_lateness_seconds": 30,
    })
    assert r.status_code == 201
    windows_before = client.get("/devices/d1/windows").json()
    group_before = client.get("/groups/g1/windows").json()

    r = put_config(client, "d1", 60, 30)
    assert r.status_code == 200
    assert client.get("/devices/d1/windows").json() == windows_before
    assert client.get("/groups/g1/windows").json() == group_before


def test_ungrouped_legacy_device_follows_existing_rules(tmp_path, monkeypatch):
    # 未入组设备不触发新核对：漂移窗口不影响既有规则
    # （有数据后禁改窗口大小 409；允许迟到秒数可随时改 200）
    make_legacy_db(tmp_path, monkeypatch)
    s = epoch(0, 0, 0)
    with db.session() as conn:
        insert_device(conn, "d1", 120, 30, watermark=s + 240)
        insert_window(conn, "d1", s, s + 120)

    from app.main import app

    with TestClient(app) as client:
        assert put_config(client, "d1", 60, 30).status_code == 409  # 有数据禁改窗口大小
        r = put_config(client, "d1", 120, 45)                       # 允许迟到秒数可改
        assert r.status_code == 200
        assert r.json()["allowed_lateness_seconds"] == 45

    devices, windows, _ = snapshot_tables()
    assert devices == [("d1", 120, 45)]
    assert len(windows) == 1
