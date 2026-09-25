"""设备持久化聚合完整性核查 GET /devices/{device_id}/integrity 测试。

覆盖：健康数据无差异（含迟到事件）、缺少窗口行、无事件的窗口行（含全零行）、
五项聚合各自不一致、浮点 1e-9 相对/绝对容差、差异按 window_start 升序、
范围外数据不参与核查、自定义窗口大小网格、1000 窗口上限、422/404、只读语义
（不改动水位线/窗口聚合/修正记录/组汇总）。
"""

import sqlite3
from datetime import datetime, timezone

import pytest


def make_device(client, device_id, window_size_seconds=60, allowed_lateness_seconds=30):
    r = client.put(
        f"/devices/{device_id}/config",
        json={
            "window_size_seconds": window_size_seconds,
            "allowed_lateness_seconds": allowed_lateness_seconds,
        },
    )
    assert r.status_code == 200, r.text


def event(event_id, ts, value):
    return {"event_id": event_id, "event_time": ts, "value": value}


def ingest(client, device_id, events):
    r = client.post("/ingest", json={"device_id": device_id, "events": events})
    assert r.status_code == 200, r.text
    return r


def integrity(client, device_id, start, end):
    return client.get(
        f"/devices/{device_id}/integrity", params={"start": start, "end": end}
    )


def ts(text):
    return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()


def tamper(tmp_path, sql, params=()):
    """直接改库，模拟持久化聚合与原始事件不一致的异常状态。"""
    conn = sqlite3.connect(str(tmp_path / "test.db"))
    conn.execute(sql, params)
    conn.commit()
    conn.close()


def read_window_sum(tmp_path, device_id, window_start):
    conn = sqlite3.connect(str(tmp_path / "test.db"))
    row = conn.execute(
        "SELECT sum FROM windows WHERE device_id = ? AND window_start = ?",
        (device_id, window_start),
    ).fetchone()
    conn.close()
    return row[0]


def seed(client, device_id="dev-1"):
    """窗口 10:00（3 事件含 1 迟到，已封存）与 10:02（1 事件，open）。

    10:00 窗口：count=3, sum=64.4, min=20.9, max=22.0, late_event_count=1；
    10:02 窗口：count=1, sum=23.1, min=max=23.1, late_event_count=0。
    """
    make_device(client, device_id)
    ingest(client, device_id, [
        event("e1", "2026-09-22T10:00:10Z", 21.5),
        event("e2", "2026-09-22T10:00:40Z", 22.0),
        event("e3", "2026-09-22T10:02:00Z", 23.1),
    ])
    # 10:00 窗口已封存（10:01:00 + 30s <= 10:02:00），e4 迟到落入并产生修正
    r = ingest(client, device_id, [event("e4", "2026-09-22T10:00:25Z", 20.9)])
    assert r.json()["results"][0]["status"] == "late"


# ---------- 账实相符 ----------

def test_healthy_data_has_no_discrepancies(client):
    seed(client)
    r = integrity(client, "dev-1", "2026-09-22T10:00:00Z", "2026-09-22T10:10:00Z")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["device_id"] == "dev-1"
    assert body["start"] == "2026-09-22T10:00:00Z"
    assert body["end"] == "2026-09-22T10:10:00Z"
    assert body["window_size_seconds"] == 60
    assert body["windows_checked"] == 10
    assert body["discrepancy_count"] == 0
    assert body["discrepancies"] == []


def test_device_without_data_has_no_discrepancies(client):
    make_device(client, "dev-1")
    body = integrity(client, "dev-1", "2026-09-22T10:00:00Z", "2026-09-22T10:05:00Z").json()
    assert body["windows_checked"] == 5
    assert body["discrepancy_count"] == 0
    assert body["discrepancies"] == []


def test_custom_window_size_grid(client):
    make_device(client, "dev-big", window_size_seconds=300, allowed_lateness_seconds=60)
    ingest(client, "dev-big", [
        event("e1", "2026-09-22T00:00:10Z", 1.0),
        event("e2", "2026-09-22T00:04:50Z", 2.0),
    ])
    body = integrity(client, "dev-big", "2026-09-22T00:00:00Z", "2026-09-22T01:00:00Z").json()
    assert body["window_size_seconds"] == 300
    assert body["windows_checked"] == 12
    assert body["discrepancies"] == []
    # 对齐 60s 但不对齐 300s → 422
    assert integrity(
        client, "dev-big", "2026-09-22T00:01:00Z", "2026-09-22T01:00:00Z"
    ).status_code == 422


# ---------- 结构性差异：缺少窗口行 / 无事件的窗口行 ----------

def test_missing_window_row_is_reported(client, tmp_path):
    seed(client)
    tamper(
        tmp_path,
        "DELETE FROM windows WHERE device_id = 'dev-1' AND window_start = ?",
        (ts("2026-09-22T10:02:00Z"),),
    )
    body = integrity(client, "dev-1", "2026-09-22T10:00:00Z", "2026-09-22T10:10:00Z").json()
    assert body["discrepancy_count"] == 1
    (d,) = body["discrepancies"]
    assert d["window_start"] == "2026-09-22T10:02:00Z"
    assert d["window_end"] == "2026-09-22T10:03:00Z"
    assert d["kind"] == "missing_window_row"
    assert d["expected"] == {
        "count": 1, "sum": 23.1, "min": 23.1, "max": 23.1, "late_event_count": 0
    }
    assert d["actual"] is None
    assert d["mismatched_fields"] == ["count", "sum", "min", "max", "late_event_count"]


def test_window_row_without_events_is_reported(client, tmp_path):
    seed(client)
    tamper(
        tmp_path,
        "INSERT INTO windows (device_id, window_start, window_end, count, sum, min, max,"
        " status, correction_count, late_event_count)"
        " VALUES ('dev-1', ?, ?, 2, 10.0, 4.0, 6.0, 'open', 0, 1)",
        (ts("2026-09-22T10:05:00Z"), ts("2026-09-22T10:06:00Z")),
    )
    body = integrity(client, "dev-1", "2026-09-22T10:00:00Z", "2026-09-22T10:10:00Z").json()
    assert body["discrepancy_count"] == 1
    (d,) = body["discrepancies"]
    assert d["window_start"] == "2026-09-22T10:05:00Z"
    assert d["kind"] == "unexpected_window_row"
    # 无事件窗口的期望为全零聚合（min/max 为 null）
    assert d["expected"] == {
        "count": 0, "sum": 0.0, "min": None, "max": None, "late_event_count": 0
    }
    assert d["actual"] == {
        "count": 2, "sum": 10.0, "min": 4.0, "max": 6.0, "late_event_count": 1
    }
    assert d["mismatched_fields"] == ["count", "sum", "min", "max", "late_event_count"]


def test_all_zero_window_row_without_events_is_still_reported(client, tmp_path):
    """无事件的窗口行即使计数全为 0，也列为差异。"""
    seed(client)
    tamper(
        tmp_path,
        "INSERT INTO windows (device_id, window_start, window_end, count, sum, min, max,"
        " status, correction_count, late_event_count)"
        " VALUES ('dev-1', ?, ?, 0, 0.0, NULL, NULL, 'open', 0, 0)",
        (ts("2026-09-22T10:05:00Z"), ts("2026-09-22T10:06:00Z")),
    )
    body = integrity(client, "dev-1", "2026-09-22T10:00:00Z", "2026-09-22T10:10:00Z").json()
    assert body["discrepancy_count"] == 1
    (d,) = body["discrepancies"]
    assert d["kind"] == "unexpected_window_row"
    assert d["expected"] == d["actual"]
    assert d["mismatched_fields"] == []


# ---------- 聚合字段不一致 ----------

def test_count_and_sum_mismatch(client, tmp_path):
    seed(client)
    tamper(
        tmp_path,
        "UPDATE windows SET count = 4, sum = 100.0"
        " WHERE device_id = 'dev-1' AND window_start = ?",
        (ts("2026-09-22T10:00:00Z"),),
    )
    body = integrity(client, "dev-1", "2026-09-22T10:00:00Z", "2026-09-22T10:10:00Z").json()
    assert body["discrepancy_count"] == 1
    (d,) = body["discrepancies"]
    assert d["kind"] == "aggregate_mismatch"
    assert d["mismatched_fields"] == ["count", "sum"]
    assert d["expected"]["count"] == 3
    assert d["expected"]["sum"] == pytest.approx(64.4)
    assert d["expected"]["min"] == 20.9
    assert d["expected"]["max"] == 22.0
    assert d["expected"]["late_event_count"] == 1
    assert d["actual"]["count"] == 4
    assert d["actual"]["sum"] == 100.0
    assert d["actual"]["min"] == 20.9
    assert d["actual"]["max"] == 22.0
    assert d["actual"]["late_event_count"] == 1


def test_min_max_mismatch(client, tmp_path):
    seed(client)
    tamper(
        tmp_path,
        "UPDATE windows SET min = 0.0, max = 99.0"
        " WHERE device_id = 'dev-1' AND window_start = ?",
        (ts("2026-09-22T10:00:00Z"),),
    )
    (d,) = integrity(
        client, "dev-1", "2026-09-22T10:00:00Z", "2026-09-22T10:10:00Z"
    ).json()["discrepancies"]
    assert d["kind"] == "aggregate_mismatch"
    assert d["mismatched_fields"] == ["min", "max"]
    assert d["expected"]["min"] == 20.9
    assert d["expected"]["max"] == 22.0
    assert d["actual"]["min"] == 0.0
    assert d["actual"]["max"] == 99.0


def test_late_event_count_mismatch(client, tmp_path):
    seed(client)
    tamper(
        tmp_path,
        "UPDATE windows SET late_event_count = 0"
        " WHERE device_id = 'dev-1' AND window_start = ?",
        (ts("2026-09-22T10:00:00Z"),),
    )
    (d,) = integrity(
        client, "dev-1", "2026-09-22T10:00:00Z", "2026-09-22T10:10:00Z"
    ).json()["discrepancies"]
    assert d["mismatched_fields"] == ["late_event_count"]
    assert d["expected"]["late_event_count"] == 1
    assert d["actual"]["late_event_count"] == 0


# ---------- 浮点容差：相对或绝对误差 1e-9 ----------

def test_sum_within_relative_tolerance_is_not_reported(client, tmp_path):
    seed(client)
    w = ts("2026-09-22T10:00:00Z")
    stored = read_window_sum(tmp_path, "dev-1", w)
    # 相对误差 5e-10 < 1e-9：不判差异
    tamper(
        tmp_path,
        "UPDATE windows SET sum = ? WHERE device_id = 'dev-1' AND window_start = ?",
        (stored * (1 + 5e-10), w),
    )
    assert integrity(
        client, "dev-1", "2026-09-22T10:00:00Z", "2026-09-22T10:10:00Z"
    ).json()["discrepancies"] == []
    # 相对误差 5e-9 > 1e-9：判差异
    tamper(
        tmp_path,
        "UPDATE windows SET sum = ? WHERE device_id = 'dev-1' AND window_start = ?",
        (stored * (1 + 5e-9), w),
    )
    (d,) = integrity(
        client, "dev-1", "2026-09-22T10:00:00Z", "2026-09-22T10:10:00Z"
    ).json()["discrepancies"]
    assert d["mismatched_fields"] == ["sum"]


def test_sum_uses_absolute_tolerance_around_zero(client, tmp_path):
    make_device(client, "dev-1")
    ingest(client, "dev-1", [
        event("p1", "2026-09-22T10:00:10Z", 1.5),
        event("n1", "2026-09-22T10:00:20Z", -1.5),
    ])
    w = ts("2026-09-22T10:00:00Z")
    # 重算 sum 为 0.0；绝对误差 5e-10 <= 1e-9：不判差异
    tamper(
        tmp_path,
        "UPDATE windows SET sum = ? WHERE device_id = 'dev-1' AND window_start = ?",
        (5e-10, w),
    )
    assert integrity(
        client, "dev-1", "2026-09-22T10:00:00Z", "2026-09-22T10:10:00Z"
    ).json()["discrepancies"] == []
    # 绝对误差 2e-9 > 1e-9：判差异
    tamper(
        tmp_path,
        "UPDATE windows SET sum = ? WHERE device_id = 'dev-1' AND window_start = ?",
        (2e-9, w),
    )
    (d,) = integrity(
        client, "dev-1", "2026-09-22T10:00:00Z", "2026-09-22T10:10:00Z"
    ).json()["discrepancies"]
    assert d["mismatched_fields"] == ["sum"]
    assert d["expected"]["sum"] == 0.0
    assert d["actual"]["sum"] == 2e-9


# ---------- 排序与范围裁剪 ----------

def test_discrepancies_sorted_by_window_start(client, tmp_path):
    seed(client)
    # 三处差异乱序制造：10:02 缺行、10:00 聚合不一致、10:05 无事件窗口行
    tamper(
        tmp_path,
        "DELETE FROM windows WHERE device_id = 'dev-1' AND window_start = ?",
        (ts("2026-09-22T10:02:00Z"),),
    )
    tamper(
        tmp_path,
        "UPDATE windows SET sum = 100.0 WHERE device_id = 'dev-1' AND window_start = ?",
        (ts("2026-09-22T10:00:00Z"),),
    )
    tamper(
        tmp_path,
        "INSERT INTO windows (device_id, window_start, window_end, count, sum, min, max,"
        " status, correction_count, late_event_count)"
        " VALUES ('dev-1', ?, ?, 1, 7.0, 7.0, 7.0, 'open', 0, 0)",
        (ts("2026-09-22T10:05:00Z"), ts("2026-09-22T10:06:00Z")),
    )
    body = integrity(client, "dev-1", "2026-09-22T10:00:00Z", "2026-09-22T10:10:00Z").json()
    assert body["discrepancy_count"] == 3
    assert [d["window_start"] for d in body["discrepancies"]] == [
        "2026-09-22T10:00:00Z",
        "2026-09-22T10:02:00Z",
        "2026-09-22T10:05:00Z",
    ]
    assert [d["kind"] for d in body["discrepancies"]] == [
        "aggregate_mismatch",
        "missing_window_row",
        "unexpected_window_row",
    ]


def test_data_outside_range_is_not_checked(client, tmp_path):
    seed(client)
    ingest(client, "dev-1", [event("e9", "2026-09-22T11:00:00Z", 5.0)])
    # 删除范围外的 11:00 窗口行：不影响 [10:00, 10:10) 的核查
    tamper(
        tmp_path,
        "DELETE FROM windows WHERE device_id = 'dev-1' AND window_start = ?",
        (ts("2026-09-22T11:00:00Z"),),
    )
    body = integrity(client, "dev-1", "2026-09-22T10:00:00Z", "2026-09-22T10:10:00Z").json()
    assert body["discrepancies"] == []
    # 覆盖 11:00 的核查能发现该缺行
    body2 = integrity(client, "dev-1", "2026-09-22T10:00:00Z", "2026-09-22T11:01:00Z").json()
    assert [d["window_start"] for d in body2["discrepancies"]] == ["2026-09-22T11:00:00Z"]
    assert body2["discrepancies"][0]["kind"] == "missing_window_row"


# ---------- 参数校验与错误码 ----------

def test_missing_device_returns_404(client):
    r = integrity(client, "ghost", "2026-09-22T10:00:00Z", "2026-09-22T10:10:00Z")
    assert r.status_code == 404


def test_inverted_or_empty_range_is_422(client):
    seed(client)
    assert integrity(
        client, "dev-1", "2026-09-22T11:00:00Z", "2026-09-22T10:00:00Z"
    ).status_code == 422
    assert integrity(
        client, "dev-1", "2026-09-22T10:00:00Z", "2026-09-22T10:00:00Z"
    ).status_code == 422


def test_misaligned_boundaries_are_422(client):
    seed(client)
    assert integrity(
        client, "dev-1", "2026-09-22T10:00:30Z", "2026-09-22T10:10:00Z"
    ).status_code == 422
    assert integrity(
        client, "dev-1", "2026-09-22T10:00:00Z", "2026-09-22T10:09:59Z"
    ).status_code == 422


def test_range_covering_more_than_1000_windows_is_422(client):
    make_device(client, "dev-1")
    # 恰好 1000 个窗口（1000 * 60s = 16h40m）：正常
    r = integrity(client, "dev-1", "2026-09-22T00:00:00Z", "2026-09-22T16:40:00Z")
    assert r.status_code == 200, r.text
    assert r.json()["windows_checked"] == 1000
    # 1001 个窗口：422
    assert integrity(
        client, "dev-1", "2026-09-22T00:00:00Z", "2026-09-22T16:41:00Z"
    ).status_code == 422


def test_missing_required_params_are_422(client):
    seed(client)
    assert client.get("/devices/dev-1/integrity").status_code == 422
    assert client.get(
        "/devices/dev-1/integrity", params={"start": "2026-09-22T10:00:00Z"}
    ).status_code == 422
    assert client.get(
        "/devices/dev-1/integrity", params={"end": "2026-09-22T10:10:00Z"}
    ).status_code == 422


# ---------- 只读语义 ----------

def test_query_is_readonly(client):
    seed(client)
    # 再挂一个组汇总，验证核查不改动组汇总
    make_device(client, "dev-2")
    ingest(client, "dev-2", [
        event("x1", "2026-09-22T10:00:20Z", 1.0),
        event("x2", "2026-09-22T10:02:00Z", 2.0),
    ])
    r = client.post("/groups", json={
        "group_id": "g1",
        "member_ids": ["dev-1", "dev-2"],
        "aggregation": "sum",
        "window_size_seconds": 60,
        "allowed_lateness_seconds": 30,
    })
    assert r.status_code == 201, r.text

    before_device = client.get("/devices/dev-1").json()
    before_windows = client.get("/devices/dev-1/windows").json()
    before_corrections = client.get("/devices/dev-1/corrections").json()
    before_group_windows = client.get("/groups/g1/windows").json()

    for _ in range(3):
        integrity(client, "dev-1", "2026-09-22T10:00:00Z", "2026-09-22T10:10:00Z")
    # 404 / 422 路径同样不得写库
    integrity(client, "ghost", "2026-09-22T10:00:00Z", "2026-09-22T10:10:00Z")
    integrity(client, "dev-1", "2026-09-22T10:00:30Z", "2026-09-22T10:10:00Z")

    assert client.get("/devices/dev-1").json() == before_device
    assert client.get("/devices/dev-1/windows").json() == before_windows
    assert client.get("/devices/dev-1/corrections").json() == before_corrections
    assert client.get("/groups/g1/windows").json() == before_group_windows
