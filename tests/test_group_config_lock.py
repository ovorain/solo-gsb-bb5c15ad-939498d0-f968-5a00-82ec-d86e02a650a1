"""组成员配置漂移修复测试。

- 已入组设备：PUT config 与任一所属组不一致（窗口大小/允许迟到秒数，成员尚无窗口亦然）
  返回 409 且不写入；相同配置幂等重复提交返回 200。
- 未入组设备：沿用既有规则（有数据后禁改窗口大小，允许迟到秒数可随时改）。
- 组创建与设备配置更新在写锁事务内串行：并发交错后不存在"组已存在但成员配置漂移"。
"""

import os
import sqlite3
import threading

import pytest


def put_config(client, device_id, size, lateness):
    return client.put(
        f"/devices/{device_id}/config",
        json={
            "window_size_seconds": size,
            "allowed_lateness_seconds": lateness,
        },
    )


def make_group(client, group_id, members, size=60, lateness=30, aggregation="sum"):
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


def db_config(device_id):
    """直接读库取设备配置（绕过 API，验证 409 确实未写入）。"""
    conn = sqlite3.connect(os.environ["TELEMETRY_DB_PATH"])
    try:
        row = conn.execute(
            "SELECT window_size_seconds, allowed_lateness_seconds"
            " FROM devices WHERE device_id = ?",
            (device_id,),
        ).fetchone()
    finally:
        conn.close()
    return row


def test_grouped_device_lateness_locked_even_without_windows(client):
    # 两个默认 60/30 的设备组组，成员尚无任何窗口
    assert put_config(client, "d1", 60, 30).status_code == 200
    assert put_config(client, "d2", 60, 30).status_code == 200
    assert make_group(client, "g1", ("d1", "d2")).status_code == 201

    # 允许迟到秒数不得随时改（修复前：此请求返回 200 并写入，导致封存时点不一致）
    r = put_config(client, "d1", 60, 40)
    assert r.status_code == 409
    assert db_config("d1") == (60, 30)  # 未写入

    # 窗口大小在成员尚无窗口时也不得改（修复前：无窗口时可改，导致同组窗口网格错开）
    r = put_config(client, "d1", 120, 30)
    assert r.status_code == 409
    assert db_config("d1") == (60, 30)

    # 两项同时不一致同样拒绝
    r = put_config(client, "d1", 120, 40)
    assert r.status_code == 409
    assert db_config("d1") == (60, 30)


def test_grouped_device_config_locked_after_data(client):
    ingest(client, "d1", [{"event_id": "e1", "event_time": "2026-09-22T00:00:10Z", "value": 1.0}])
    ingest(client, "d2", [{"event_id": "e1", "event_time": "2026-09-22T00:00:20Z", "value": 2.0}])
    assert make_group(client, "g1", ("d1", "d2")).status_code == 201

    r = put_config(client, "d1", 60, 40)
    assert r.status_code == 409
    assert db_config("d1") == (60, 30)

    r = put_config(client, "d1", 120, 30)
    assert r.status_code == 409
    assert db_config("d1") == (60, 30)


def test_grouped_device_same_config_is_idempotent(client):
    assert put_config(client, "d1", 60, 30).status_code == 200
    assert put_config(client, "d2", 60, 30).status_code == 200
    assert make_group(client, "g1", ("d1", "d2")).status_code == 201

    # 与所属组完全相同的配置允许重复提交（幂等），仍返回 200
    r = put_config(client, "d1", 60, 30)
    assert r.status_code == 200
    body = r.json()
    assert body["window_size_seconds"] == 60
    assert body["allowed_lateness_seconds"] == 30
    # 连续重复提交同样允许
    assert put_config(client, "d1", 60, 30).status_code == 200


def test_conflicting_group_create_rolls_back_and_config_still_writable(client):
    # 组创建校验失败（成员配置不一致）不应影响成员后续配置更新
    assert put_config(client, "d1", 120, 30).status_code == 200
    assert put_config(client, "d2", 60, 30).status_code == 200
    assert make_group(client, "g1", ("d1", "d2")).status_code == 409

    # 组未建成：d1 仍是未入组设备，允许迟到秒数可改、无数据时窗口大小也可改
    assert put_config(client, "d1", 120, 45).status_code == 200
    assert db_config("d1") == (120, 45)
    # 组确实不存在
    assert client.get("/groups/g1").status_code == 404


def test_ungrouped_device_keeps_existing_rules(client):
    # 未入组：无数据时窗口大小与允许迟到秒数都可改
    assert put_config(client, "d1", 300, 60).status_code == 200
    assert put_config(client, "d1", 300, 120).status_code == 200
    assert db_config("d1") == (300, 120)
    assert put_config(client, "d1", 60, 120).status_code == 200

    # 有数据后：窗口大小禁改 409，允许迟到秒数仍可改
    ingest(client, "d1", [{"event_id": "e1", "event_time": "2026-09-22T00:00:10Z", "value": 1.0}])
    assert put_config(client, "d1", 300, 120).status_code == 409
    assert put_config(client, "d1", 60, 45).status_code == 200
    assert db_config("d1") == (60, 45)


def test_multi_group_member_must_match_every_group(client):
    # d1 同时属于 g1、g2（两窗口配置相同）
    assert put_config(client, "d1", 60, 30).status_code == 200
    assert put_config(client, "d2", 60, 30).status_code == 200
    assert make_group(client, "g1", ("d1", "d2")).status_code == 201
    assert make_group(client, "g2", ("d1", "d2"), aggregation="avg").status_code == 201

    # 与两个组都不一致 → 409
    assert put_config(client, "d1", 60, 40).status_code == 409
    assert db_config("d1") == (60, 30)

    # 与两个组完全相同 → 幂等 200
    assert put_config(client, "d1", 60, 30).status_code == 200


def test_group_summary_consistent_after_blocked_config_update(client):
    # 端到端：被 409 拦下后，成员仍按组配置封存并出组汇总
    ingest(client, "d1", [{"event_id": "e1", "event_time": "2026-09-22T00:00:10Z", "value": 10.0}])
    ingest(client, "d2", [{"event_id": "e1", "event_time": "2026-09-22T00:00:20Z", "value": 20.0}])
    assert make_group(client, "g1", ("d1", "d2")).status_code == 201

    # 试图把 d1 允许迟到改大（若生效，d1 窗口会更晚封存、组汇总失真）
    assert put_config(client, "d1", 60, 300).status_code == 409

    # 00:02:00 的水位线事件按组配置（lateness=30）封存两个成员的 00:00 窗口
    ingest(client, "d1", [{"event_id": "s1", "event_time": "2026-09-22T00:02:00Z", "value": 1.0}])
    ingest(client, "d2", [{"event_id": "s2", "event_time": "2026-09-22T00:02:00Z", "value": 2.0}])
    w = client.get("/groups/g1/windows/2026-09-22T00:00:00Z").json()
    assert w["status"] == "ready"
    assert w["value"] == 30.0


@pytest.mark.parametrize("round_no", range(20))
def test_concurrent_group_create_vs_config_update(client, round_no):
    """组创建与成员配置更新并发交错后，不存在"组已建成但成员配置漂移"。

    写锁串行下只允许两种一致结局：
    1. 组先建成 → 成员配置被锁定（后续漂移更新一律 409）→ 成员配置 == 组配置；
    2. 配置先改（设备当时未入组）→ 组创建因校验失败回滚 → 组不存在。
    """
    gid = f"grp-{round_no}"
    d1, d2 = f"a-{round_no}", f"b-{round_no}"
    assert put_config(client, d1, 60, 30).status_code == 200
    assert put_config(client, d2, 60, 30).status_code == 200

    barrier = threading.Barrier(2)
    outcomes: dict[str, object] = {}

    def create():
        barrier.wait()
        outcomes["group"] = make_group(client, gid, (d1, d2)).status_code

    def reconfigure():
        barrier.wait()
        codes = []
        for _ in range(25):
            codes.append(put_config(client, d1, 120, 40).status_code)
        outcomes["config_codes"] = codes

    t1 = threading.Thread(target=create)
    t2 = threading.Thread(target=reconfigure)
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    group_exists = client.get(f"/groups/{gid}").status_code == 200
    d1_size, d1_lateness = db_config(d1)

    if group_exists:
        # 结局 1：组成员配置必须仍与组一致，且漂移更新只能拿到 409
        assert outcomes["group"] == 201
        assert (d1_size, d1_lateness) == (60, 30)
        assert set(outcomes["config_codes"]) <= {409}
    else:
        # 结局 2：组创建因成员配置已漂移而失败回滚，组不存在
        assert outcomes["group"] == 409
        assert (d1_size, d1_lateness) == (120, 40)
        assert 200 in outcomes["config_codes"]
