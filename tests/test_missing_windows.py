"""设备缺报区间查询 GET /devices/{device_id}/missing-windows 测试。

覆盖：连续缺报段的起止与缺失窗口数、最早有数据窗口的下一个窗口起算、
水位线封存条件裁剪（未封存不报缺）、半开区间边界、迟到事件填空后缺报段
自动缩短/拆分、空隙计算（不逐窗遍历）、空数据空列表、422/404、只读。
"""


def make_device(client, device_id, window_size_seconds=60, allowed_lateness_seconds=30):
    r = client.put(
        f"/devices/{device_id}/config",
        json={
            "window_size_seconds": window_size_seconds,
            "allowed_lateness_seconds": allowed_lateness_seconds,
        },
    )
    assert r.status_code == 200, r.text


def ingest(client, device_id, event_id, ts, value=1.0):
    r = client.post(
        "/ingest",
        json={"device_id": device_id, "events": [
            {"event_id": event_id, "event_time": ts, "value": value}
        ]},
    )
    assert r.status_code == 200, r.text
    return r


def missing(client, device_id, start, end):
    return client.get(
        f"/devices/{device_id}/missing-windows",
        params={"start": start, "end": end},
    )


def seed_gap(client, device_id="dev-1"):
    """窗口记录：00:00、00:03、00:04；水位线 00:04:00（lateness=30s）。

    封存条件覆盖到的最大窗口起点为 00:02，故 00:01、00:02 两个空窗缺报。
    """
    make_device(client, device_id)
    ingest(client, device_id, "e1", "2026-09-22T00:00:10Z")
    ingest(client, device_id, "e2", "2026-09-22T00:03:10Z")
    ingest(client, device_id, "e3", "2026-09-22T00:04:00Z")


# ---------- 基本缺报段计算 ----------

def test_single_gap_segment(client):
    seed_gap(client)
    r = missing(client, "dev-1", "2026-09-22T00:00:00Z", "2026-09-22T00:10:00Z")
    assert r.status_code == 200, r.text
    assert r.json() == [
        {
            "start": "2026-09-22T00:01:00Z",
            "end": "2026-09-22T00:03:00Z",
            "missing_windows": 2,
        }
    ]


def test_counting_starts_after_earliest_data_window(client):
    """设备最早有数据窗口为 00:00，其之前（含 00:00 自身）一律不追缺。"""
    seed_gap(client)
    r = missing(client, "dev-1", "2026-09-21T23:00:00Z", "2026-09-22T00:10:00Z")
    assert r.json() == [
        {
            "start": "2026-09-22T00:01:00Z",
            "end": "2026-09-22T00:03:00Z",
            "missing_windows": 2,
        }
    ]


def test_half_open_boundaries_clip_segments(client):
    seed_gap(client)
    # 与缺报段完全重合
    assert missing(client, "dev-1", "2026-09-22T00:01:00Z", "2026-09-22T00:03:00Z").json() == [
        {"start": "2026-09-22T00:01:00Z", "end": "2026-09-22T00:03:00Z", "missing_windows": 2}
    ]
    # start 落在段内：段起点被裁剪到 start
    assert missing(client, "dev-1", "2026-09-22T00:02:00Z", "2026-09-22T00:10:00Z").json() == [
        {"start": "2026-09-22T00:02:00Z", "end": "2026-09-22T00:03:00Z", "missing_windows": 1}
    ]
    # end 为半开上界：00:02 之后不纳入
    assert missing(client, "dev-1", "2026-09-22T00:00:00Z", "2026-09-22T00:02:00Z").json() == [
        {"start": "2026-09-22T00:01:00Z", "end": "2026-09-22T00:02:00Z", "missing_windows": 1}
    ]


def test_unsealed_positions_are_never_reported(client):
    """水位线尚未越过封存条件的空窗不报缺（即使查询 end 更靠后）。"""
    make_device(client, "dev-1")
    ingest(client, "dev-1", "e1", "2026-09-22T00:00:10Z")
    # 水位线 00:03:10：00:01 已满足封存条件（00:02:00+30s <= 00:03:10），
    # 00:02 尚未满足（00:03:00+30s > 00:03:10）
    ingest(client, "dev-1", "e2", "2026-09-22T00:03:10Z")

    r = missing(client, "dev-1", "2026-09-22T00:00:00Z", "2026-09-22T01:00:00Z")
    assert r.json() == [
        {"start": "2026-09-22T00:01:00Z", "end": "2026-09-22T00:02:00Z", "missing_windows": 1}
    ]


def test_trailing_gap_when_device_stops_reporting(client):
    """设备停止上报后，最后一个窗口到封存水位线之间的空窗也是缺报段。"""
    seed_gap(client)
    # 水位线推进到 00:10:00：封存条件覆盖到窗口起点 00:08
    ingest(client, "dev-1", "e4", "2026-09-22T00:10:00Z")

    r = missing(client, "dev-1", "2026-09-22T00:00:00Z", "2026-09-22T00:12:00Z")
    assert r.json() == [
        {"start": "2026-09-22T00:01:00Z", "end": "2026-09-22T00:03:00Z", "missing_windows": 2},
        {"start": "2026-09-22T00:05:00Z", "end": "2026-09-22T00:09:00Z", "missing_windows": 4},
    ]


def test_late_event_filling_empty_window_splits_segment(client):
    """迟到事件首次填入空窗后，后续查询自动缩短/拆分缺报段。"""
    seed_gap(client)
    ingest(client, "dev-1", "e4", "2026-09-22T00:10:00Z")
    before = missing(client, "dev-1", "2026-09-22T00:00:00Z", "2026-09-22T00:12:00Z").json()
    assert [s["missing_windows"] for s in before] == [2, 4]

    # 迟到事件落入空窗 00:06（该窗口此前没有任何记录）
    r = ingest(client, "dev-1", "e5", "2026-09-22T00:06:15Z", value=9.0)
    assert r.json()["results"][0]["status"] == "late"

    after = missing(client, "dev-1", "2026-09-22T00:00:00Z", "2026-09-22T00:12:00Z").json()
    assert after == [
        {"start": "2026-09-22T00:01:00Z", "end": "2026-09-22T00:03:00Z", "missing_windows": 2},
        {"start": "2026-09-22T00:05:00Z", "end": "2026-09-22T00:06:00Z", "missing_windows": 1},
        {"start": "2026-09-22T00:07:00Z", "end": "2026-09-22T00:09:00Z", "missing_windows": 2},
    ]

    # 空窗被填为迟到窗口：新建窗口记录并封存，已有窗口聚合不被追改
    windows = {w["window_start"]: w for w in client.get("/devices/dev-1/windows").json()}
    filled = windows["2026-09-22T00:06:00Z"]
    assert filled["status"] == "sealed"
    assert (filled["count"], filled["sum"], filled["correction_count"]) == (1, 9.0, 1)
    assert windows["2026-09-22T00:03:00Z"]["count"] == 1
    assert windows["2026-09-22T00:03:00Z"]["correction_count"] == 0

    # 再次填满整个缺报段后，该段消失
    ingest(client, "dev-1", "e6", "2026-09-22T00:01:30Z")
    ingest(client, "dev-1", "e7", "2026-09-22T00:02:30Z")
    assert missing(client, "dev-1", "2026-09-22T00:00:00Z", "2026-09-22T00:03:00Z").json() == []


def test_custom_window_size_grid(client):
    """缺报段按设备自身窗口大小对齐与计数。"""
    make_device(client, "dev-big", window_size_seconds=300, allowed_lateness_seconds=60)
    ingest(client, "dev-big", "e1", "2026-09-22T00:00:10Z")
    # 水位线 00:16:00：封存条件覆盖到窗口起点 00:10（00:10+300s+60s <= 00:16:00）
    ingest(client, "dev-big", "e2", "2026-09-22T00:16:00Z")

    r = missing(client, "dev-big", "2026-09-22T00:00:00Z", "2026-09-22T00:20:00Z")
    assert r.json() == [
        {"start": "2026-09-22T00:05:00Z", "end": "2026-09-22T00:15:00Z", "missing_windows": 2}
    ]


def test_segments_are_isolated_per_device(client):
    seed_gap(client, "dev-1")
    seed_gap(client, "dev-2")
    # dev-2 补上 00:01 的窗口，缺报段与 dev-1 不同
    ingest(client, "dev-2", "e9", "2026-09-22T00:01:40Z")

    assert missing(client, "dev-1", "2026-09-22T00:00:00Z", "2026-09-22T00:10:00Z").json() == [
        {"start": "2026-09-22T00:01:00Z", "end": "2026-09-22T00:03:00Z", "missing_windows": 2}
    ]
    assert missing(client, "dev-2", "2026-09-22T00:00:00Z", "2026-09-22T00:10:00Z").json() == [
        {"start": "2026-09-22T00:02:00Z", "end": "2026-09-22T00:03:00Z", "missing_windows": 1}
    ]


# ---------- 空结果 ----------

def test_device_without_any_data_returns_empty_list(client):
    make_device(client, "dev-1")
    r = missing(client, "dev-1", "2026-09-22T00:00:00Z", "2026-09-22T01:00:00Z")
    assert r.status_code == 200
    assert r.json() == []


def test_fully_covered_range_returns_empty_list(client):
    make_device(client, "dev-1")
    ingest(client, "dev-1", "e1", "2026-09-22T00:00:10Z")
    ingest(client, "dev-1", "e2", "2026-09-22T00:01:10Z")
    ingest(client, "dev-1", "e3", "2026-09-22T00:02:00Z")
    assert missing(client, "dev-1", "2026-09-22T00:00:00Z", "2026-09-22T00:10:00Z").json() == []


def test_range_before_first_data_returns_empty_list(client):
    seed_gap(client)
    assert missing(client, "dev-1", "2026-09-21T22:00:00Z", "2026-09-21T23:00:00Z").json() == []


# ---------- 参数校验与错误码 ----------

def test_missing_device_returns_404(client):
    r = missing(client, "ghost", "2026-09-22T00:00:00Z", "2026-09-22T01:00:00Z")
    assert r.status_code == 404


def test_inverted_or_empty_range_is_422(client):
    seed_gap(client)
    assert missing(
        client, "dev-1", "2026-09-22T01:00:00Z", "2026-09-22T00:00:00Z"
    ).status_code == 422
    assert missing(
        client, "dev-1", "2026-09-22T00:00:00Z", "2026-09-22T00:00:00Z"
    ).status_code == 422


def test_misaligned_boundaries_are_422(client):
    seed_gap(client)
    # 未对齐 60s 网格
    assert missing(
        client, "dev-1", "2026-09-22T00:00:30Z", "2026-09-22T01:00:00Z"
    ).status_code == 422
    assert missing(
        client, "dev-1", "2026-09-22T00:00:00Z", "2026-09-22T00:59:59Z"
    ).status_code == 422


def test_misaligned_boundaries_are_422_for_custom_window_size(client):
    make_device(client, "dev-big", window_size_seconds=300, allowed_lateness_seconds=60)
    # 对齐 60s 但不对齐 300s
    assert missing(
        client, "dev-big", "2026-09-22T00:01:00Z", "2026-09-22T01:00:00Z"
    ).status_code == 422
    assert missing(
        client, "dev-big", "2026-09-22T00:05:00Z", "2026-09-22T01:00:00Z"
    ).status_code == 200


def test_missing_required_params_are_422(client):
    seed_gap(client)
    assert client.get("/devices/dev-1/missing-windows").status_code == 422
    assert client.get(
        "/devices/dev-1/missing-windows", params={"start": "2026-09-22T00:00:00Z"}
    ).status_code == 422
    assert client.get(
        "/devices/dev-1/missing-windows", params={"end": "2026-09-22T01:00:00Z"}
    ).status_code == 422


# ---------- 只读语义 ----------

def test_query_is_readonly(client):
    seed_gap(client)
    before_windows = client.get("/devices/dev-1/windows").json()
    before_device = client.get("/devices/dev-1").json()
    for _ in range(3):
        missing(client, "dev-1", "2026-09-22T00:00:00Z", "2026-09-22T00:10:00Z")
    # 404 / 422 路径同样不得写库
    missing(client, "ghost", "2026-09-22T00:00:00Z", "2026-09-22T01:00:00Z")
    missing(client, "dev-1", "2026-09-22T00:00:30Z", "2026-09-22T01:00:00Z")
    assert client.get("/devices/dev-1/windows").json() == before_windows
    assert client.get("/devices/dev-1").json() == before_device
