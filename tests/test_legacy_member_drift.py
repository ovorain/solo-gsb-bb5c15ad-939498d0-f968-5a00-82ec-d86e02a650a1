"""旧库组成员配置纠偏的回归测试。

旧版本/外部写入可能留下"设备已入组、组配置为 60/30，但设备配置字段与其历史窗口
按另一窗口大小落盘"的漂移成员。PUT /devices/{id}/config 提交与全部所属组一致的
配置试图纠偏时，必须在同一写锁事务内先核对该设备已有窗口：

- 起点对齐（window_start 是目标 window_size_seconds 网格上的整数倍）且
  实际跨度（window_end - window_start）等于目标窗口大小 → 允许纠正仅配置字段漂移；
- 任一窗口不相容 → 409，设备配置、窗口、组窗口汇总及审计快照全部保持原样。

这里直接操作 SQLite 复现旧库漂移状态（历史问题本身就绕过了正常 API 不变量）。
"""

import os
import sqlite3
from datetime import datetime, timezone

import pytest


def put_config(client, device_id, size, lateness):
    return client.put(
        f"/devices/{device_id}/config",
        json={
            "window_size_seconds": size,
            "allowed_lateness_seconds": lateness,
        },
    )


def make_group(client, group_id="g1", members=("d1", "d2"), size=60, lateness=30,
               aggregation="sum"):
    return client.post(
        "/groups",
        json={
            "group_id": group_id,
            "member_ids": list(members),
            "aggregation": aggregation,
            "window_size_seconds": size,
            "allowed_lateness_seconds": lateness,
        },
    )


def ingest(client, device_id, events):
    return client.post("/ingest", json={"device_id": device_id, "events": events})


def raw_conn():
    return sqlite3.connect(os.environ["TELEMETRY_DB_PATH"])


def epoch(iso: str) -> float:
    return datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp()


def drift_device_config(size, lateness, device_id="d1"):
    """绕过 API 直接把成员配置改成漂移值（模拟旧库落盘）。"""
    with raw_conn() as conn:
        conn.execute(
            "UPDATE devices SET window_size_seconds = ?, allowed_lateness_seconds = ?"
            " WHERE device_id = ?",
            (size, lateness, device_id),
        )


def insert_legacy_window(start_iso: str, size: int, device_id="d1", value=1.0,
                         status="sealed"):
    """直接插入一个按旧窗口大小落盘的窗口行（起点/跨度均可能与组网格不相容）。"""
    start = epoch(start_iso)
    with raw_conn() as conn:
        conn.execute(
            "INSERT INTO windows (device_id, window_start, window_end, count, sum, min, max,"
            " status, correction_count, late_event_count, sealed_at)"
            " VALUES (?, ?, ?, 1, ?, ?, ?, ?, 0, 0, ?)",
            (
                device_id, start, start + size, value, value, value, status,
                "2026-09-22T00:05:00Z" if status == "sealed" else None,
            ),
        )


def device_config(device_id="d1"):
    with raw_conn() as conn:
        return conn.execute(
            "SELECT window_size_seconds, allowed_lateness_seconds FROM devices"
            " WHERE device_id = ?",
            (device_id,),
        ).fetchone()


def device_windows(device_id="d1"):
    with raw_conn() as conn:
        return conn.execute(
            "SELECT window_start, window_end, count, sum, status FROM windows"
            " WHERE device_id = ? ORDER BY window_start",
            (device_id,),
        ).fetchall()


def group_windows_snapshot(group_id="g1"):
    with raw_conn() as conn:
        rows = conn.execute(
            "SELECT window_start, window_end, status, value, sealed_member_count, revision"
            " FROM group_windows WHERE group_id = ? ORDER BY window_start",
            (group_id,),
        ).fetchall()
        n_snapshots = conn.execute(
            "SELECT COUNT(*) FROM group_window_snapshots WHERE group_id = ?", (group_id,)
        ).fetchone()[0]
        return rows, n_snapshots


@pytest.fixture()
def grouped_devices(client):
    """两个 60/30 设备组成 g1（无数据），返回 client。"""
    assert put_config(client, "d1", 60, 30).status_code == 200
    assert put_config(client, "d2", 60, 30).status_code == 200
    assert make_group(client).status_code == 201
    return client


# ---------- 不相容旧窗口：纠偏必须被 409 拦下，一切保持原样 ----------

def test_legacy_window_misaligned_and_wrong_span_blocks_correction(grouped_devices):
    client = grouped_devices

    # d2 先有一个落在组网格上的封存窗口：组创建补算后 g1 的 00:00 组窗口为 pending(1/2)
    ingest(client, "d2", [
        {"event_id": "d2-e1", "event_time": "2026-09-22T00:00:20Z", "value": 20.0},
        {"event_id": "d2-s1", "event_time": "2026-09-22T00:02:00Z", "value": 1.0},
    ])

    # 复现旧库漂移：d1 配置曾被改成 45 秒窗口，并按 45 秒落了窗口
    # （start=00:00:45 不整除 60、跨度 45 ≠ 60：起点与跨度均不相容）
    drift_device_config(45, 30)
    insert_legacy_window("2026-09-22T00:00:45Z", 45, value=10.0)

    wins_before = device_windows()
    gw_before, snaps_before = group_windows_snapshot()
    assert gw_before  # 组汇总已存在（pending）

    # 与组不一致的提交照旧先被 409 拒绝
    assert put_config(client, "d1", 60, 40).status_code == 409
    assert put_config(client, "d1", 120, 30).status_code == 409

    # 与组完全一致的纠偏提交：因存在不相容窗口也必须 409
    r = put_config(client, "d1", 60, 30)
    assert r.status_code == 409

    # 设备配置仍是漂移值，窗口、组汇总与审计快照原样不动
    assert device_config() == (45, 30)
    assert device_windows() == wins_before
    gw_after, snaps_after = group_windows_snapshot()
    assert gw_after == gw_before
    assert snaps_after == snaps_before


def test_legacy_window_wrong_span_but_grid_aligned_blocks_correction(grouped_devices):
    client = grouped_devices

    # 旧库曾按 120 秒落盘：start=00:02:00(=120) 在 60 秒网格上对齐，但跨度 120 ≠ 60
    drift_device_config(120, 30)
    insert_legacy_window("2026-09-22T00:02:00Z", 120, value=3.0)

    wins_before = device_windows()

    r = put_config(client, "d1", 60, 30)
    assert r.status_code == 409
    assert device_config() == (120, 30)
    assert device_windows() == wins_before


def test_legacy_open_window_incompatible_also_blocks_correction(grouped_devices):
    client = grouped_devices

    # 尚未封存的 open 窗口同样参与相容性核对
    drift_device_config(45, 30)
    insert_legacy_window("2026-09-22T00:00:45Z", 45, value=1.0, status="open")

    assert put_config(client, "d1", 60, 30).status_code == 409
    assert device_config() == (45, 30)


def test_incompatible_window_among_compatible_ones_blocks_correction(grouped_devices):
    client = grouped_devices

    # 一个相容窗口 + 一个不相容窗口：整笔纠偏仍拒绝，全部窗口原样
    drift_device_config(45, 30)
    insert_legacy_window("2026-09-22T00:00:00Z", 60, value=1.0)  # 相容（组网格、跨度 60）
    insert_legacy_window("2026-09-22T00:00:45Z", 45, value=1.0)  # 不相容

    wins_before = device_windows()
    assert put_config(client, "d1", 60, 30).status_code == 409
    assert device_config() == (45, 30)
    assert device_windows() == wins_before


# ---------- 窗口全部相容：允许纠正仅配置字段的历史漂移 ----------

def test_config_only_drift_is_corrected_when_windows_compatible(client):
    # d1/d2 在 60/30 下正常上报并封存 00:00 窗口，组汇总 ready value=30 rev=1
    ingest(client, "d1", [
        {"event_id": "e1", "event_time": "2026-09-22T00:00:10Z", "value": 10.0},
        {"event_id": "s1", "event_time": "2026-09-22T00:02:00Z", "value": 1.0},
    ])
    ingest(client, "d2", [
        {"event_id": "e1", "event_time": "2026-09-22T00:00:20Z", "value": 20.0},
        {"event_id": "s2", "event_time": "2026-09-22T00:02:00Z", "value": 2.0},
    ])
    assert make_group(client).status_code == 201
    gw = client.get("/groups/g1/windows/2026-09-22T00:00:00Z").json()
    assert gw["status"] == "ready" and gw["value"] == 30.0 and gw["revision"] == 1

    # 复现"仅配置字段漂移"：设备配置被旧库改成 120/40，但历史窗口仍是 60 秒网格落盘
    drift_device_config(120, 40)
    wins_before = device_windows()
    gw_before, snaps_before = group_windows_snapshot()

    # 与组一致的提交纠正漂移配置：200
    r = put_config(client, "d1", 60, 30)
    assert r.status_code == 200
    assert r.json()["window_size_seconds"] == 60
    assert r.json()["allowed_lateness_seconds"] == 30
    assert device_config() == (60, 30)

    # 窗口本身与组汇总/审计快照均未被纠偏动作改动
    assert device_windows() == wins_before
    gw_after, snaps_after = group_windows_snapshot()
    assert gw_after == gw_before
    assert snaps_after == snaps_before

    # 仅允许迟到秒数字段漂移同样可纠正
    drift_device_config(60, 99)
    assert put_config(client, "d1", 60, 30).status_code == 200
    assert device_config() == (60, 30)

    # 纠正后设备按组配置运行：迟到修正仍驱动组汇总原位升版（30 + 5 = 35，rev=2）
    r = ingest(client, "d1", [
        {"event_id": "late-1", "event_time": "2026-09-22T00:00:30Z", "value": 5.0},
    ])
    assert r.status_code == 200
    gw = client.get("/groups/g1/windows/2026-09-22T00:00:00Z").json()
    assert gw["value"] == 35.0 and gw["revision"] == 2


def test_drifted_member_without_windows_can_be_corrected(grouped_devices):
    client = grouped_devices

    # 成员尚无任何窗口时，漂移的配置字段可直接按组配置纠正
    drift_device_config(120, 40)
    assert device_windows() == []

    r = put_config(client, "d1", 60, 30)
    assert r.status_code == 200
    assert device_config() == (60, 30)


def test_ungrouped_device_with_legacy_windows_keeps_existing_rules(client):
    # 未入组设备：旧库里有 120 秒跨度窗口时，改窗口大小仍按既有规则 409
    assert put_config(client, "d1", 120, 30).status_code == 200
    ingest(client, "d1", [
        {"event_id": "e1", "event_time": "2026-09-22T00:00:10Z", "value": 1.0},
    ])
    assert put_config(client, "d1", 60, 30).status_code == 409
    # 允许迟到秒数仍可随时调整
    assert put_config(client, "d1", 120, 45).status_code == 200
    assert device_config() == (120, 45)
