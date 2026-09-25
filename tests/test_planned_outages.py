"""设备计划停报模块测试。

覆盖：
- 登记 POST /devices/{id}/planned-outages：201、对齐/end>start 校验（422）、
  设备不存在 404、reason 规范化、持久化（重启后仍在）、允许重叠登记；
- 列出 GET /devices/{id}/planned-outages：按 start 升序、设备隔离、404；
- missing-windows 的 exclude_planned 参数：默认 false 行为不变；为 true 时
  先按原规则求缺报再扣除合并后的重叠计划（整段覆盖、中段拆分、边界触碰不重叠、
  查询范围外不参与、重叠计划不重复扣除、未封存位置仍不报缺、窗口数正确、只读）。
"""


def make_device(client, device_id="dev-1", window_size_seconds=60, allowed_lateness_seconds=30):
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


def register(client, device_id, start, end, reason=None):
    body = {"start": start, "end": end}
    if reason is not None:
        body["reason"] = reason
    return client.post(f"/devices/{device_id}/planned-outages", json=body)


def list_planned(client, device_id):
    return client.get(f"/devices/{device_id}/planned-outages")


def missing(client, device_id, start, end, exclude_planned=None):
    params = {"start": start, "end": end}
    if exclude_planned is not None:
        params["exclude_planned"] = str(exclude_planned).lower()
    return client.get(f"/devices/{device_id}/missing-windows", params=params)


def seed_gap(client, device_id="dev-1"):
    """窗口记录：00:00、00:03、00:04；水位线 00:04:00（lateness=30s）。

    封存条件覆盖到窗口起点 00:02，故原始缺报段为 [00:01, 00:03)（2 个窗口）。
    """
    make_device(client, device_id)
    ingest(client, device_id, "e1", "2026-09-22T00:00:10Z")
    ingest(client, device_id, "e2", "2026-09-22T00:03:10Z")
    ingest(client, device_id, "e3", "2026-09-22T00:04:00Z")


# ---------- 登记 ----------

def test_register_planned_outage_returns_201_and_persists(client):
    make_device(client)
    r = register(
        client, "dev-1",
        "2026-09-22T00:01:00Z", "2026-09-22T00:03:00Z",
        reason="  scheduled maintenance ",
    )
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["device_id"] == "dev-1"
    assert body["start"] == "2026-09-22T00:01:00Z"
    assert body["end"] == "2026-09-22T00:03:00Z"
    assert body["reason"] == "scheduled maintenance"
    assert isinstance(body["id"], int) and body["id"] > 0
    assert body["created_at"]

    rows = list_planned(client, "dev-1").json()
    assert len(rows) == 1 and rows[0]["id"] == body["id"]


def test_register_without_reason_stores_null(client):
    make_device(client)
    r = register(client, "dev-1", "2026-09-22T00:01:00Z", "2026-09-22T00:03:00Z")
    assert r.status_code == 201
    assert r.json()["reason"] is None
    # 纯空白 reason 同样归一化为 null
    r2 = register(client, "dev-1", "2026-09-22T00:04:00Z", "2026-09-22T00:05:00Z", reason="   ")
    assert r2.status_code == 201
    assert r2.json()["reason"] is None


def test_register_missing_device_is_404(client):
    assert register(
        client, "ghost", "2026-09-22T00:00:00Z", "2026-09-22T00:01:00Z"
    ).status_code == 404


def test_register_requires_end_greater_than_start(client):
    make_device(client)
    # end == start（零长区间）→ 422
    assert register(
        client, "dev-1", "2026-09-22T00:01:00Z", "2026-09-22T00:01:00Z"
    ).status_code == 422
    # end < start → 422
    assert register(
        client, "dev-1", "2026-09-22T00:03:00Z", "2026-09-22T00:01:00Z"
    ).status_code == 422
    # 校验失败不落任何记录
    assert list_planned(client, "dev-1").json() == []


def test_register_requires_alignment_to_device_window_grid(client):
    make_device(client)
    # 未对齐 60s 网格 → 422
    assert register(
        client, "dev-1", "2026-09-22T00:00:30Z", "2026-09-22T00:01:30Z"
    ).status_code == 422
    # start 对齐但 end 未对齐 → 422
    assert register(
        client, "dev-1", "2026-09-22T00:00:00Z", "2026-09-22T00:00:30Z"
    ).status_code == 422
    assert list_planned(client, "dev-1").json() == []


def test_register_alignment_uses_device_window_size(client):
    make_device(client, "dev-big", window_size_seconds=300, allowed_lateness_seconds=60)
    # 对齐 60s 但不对齐 300s → 422
    assert register(
        client, "dev-big", "2026-09-22T00:01:00Z", "2026-09-22T00:06:00Z"
    ).status_code == 422
    # 对齐 300s → 201
    assert register(
        client, "dev-big", "2026-09-22T00:00:00Z", "2026-09-22T00:05:00Z"
    ).status_code == 201


def test_register_allows_overlapping_intervals(client):
    make_device(client)
    assert register(
        client, "dev-1", "2026-09-22T00:01:00Z", "2026-09-22T00:04:00Z"
    ).status_code == 201
    # 与上一条重叠：允许登记
    assert register(
        client, "dev-1", "2026-09-22T00:02:00Z", "2026-09-22T00:05:00Z"
    ).status_code == 201
    rows = list_planned(client, "dev-1").json()
    assert [r["start"] for r in rows] == [
        "2026-09-22T00:01:00Z",
        "2026-09-22T00:02:00Z",
    ]


def test_planned_outages_persist_across_restart(client, tmp_path, monkeypatch):
    make_device(client)
    register(client, "dev-1", "2026-09-22T00:01:00Z", "2026-09-22T00:03:00Z", reason="maint")

    # 重新建表/迁移后旧库中的计划停报仍在
    from app import db
    db.init_db()
    rows = list_planned(client, "dev-1").json()
    assert [(r["start"], r["end"], r["reason"]) for r in rows] == [
        ("2026-09-22T00:01:00Z", "2026-09-22T00:03:00Z", "maint"),
    ]


# ---------- 列出 ----------

def test_list_empty_for_device_without_outages(client):
    make_device(client)
    assert list_planned(client, "dev-1").json() == []


def test_list_missing_device_is_404(client):
    assert list_planned(client, "ghost").status_code == 404


def test_list_is_isolated_per_device_and_sorted(client):
    make_device(client, "dev-1")
    make_device(client, "dev-2")
    register(client, "dev-1", "2026-09-22T00:05:00Z", "2026-09-22T00:06:00Z")
    register(client, "dev-1", "2026-09-22T00:01:00Z", "2026-09-22T00:02:00Z")
    register(client, "dev-2", "2026-09-22T00:01:00Z", "2026-09-22T00:02:00Z")

    d1 = list_planned(client, "dev-1").json()
    d2 = list_planned(client, "dev-2").json()
    assert [r["start"] for r in d1] == ["2026-09-22T00:01:00Z", "2026-09-22T00:05:00Z"]
    assert len(d2) == 1 and d2[0]["device_id"] == "dev-2"


# ---------- exclude_planned 扣除 ----------

def test_exclude_planned_defaults_false(client):
    seed_gap(client)
    register(client, "dev-1", "2026-09-22T00:01:00Z", "2026-09-22T00:03:00Z")
    # 不带参数 / 显式 false：计划不影响缺报结果
    expected = [
        {"start": "2026-09-22T00:01:00Z", "end": "2026-09-22T00:03:00Z", "missing_windows": 2}
    ]
    assert missing(client, "dev-1", "2026-09-22T00:00:00Z", "2026-09-22T00:10:00Z").json() == expected
    assert missing(
        client, "dev-1", "2026-09-22T00:00:00Z", "2026-09-22T00:10:00Z",
        exclude_planned=False,
    ).json() == expected


def test_exclude_planned_removes_fully_covered_gap(client):
    seed_gap(client)
    register(client, "dev-1", "2026-09-22T00:01:00Z", "2026-09-22T00:03:00Z")
    r = missing(
        client, "dev-1", "2026-09-22T00:00:00Z", "2026-09-22T00:10:00Z",
        exclude_planned=True,
    )
    assert r.status_code == 200, r.text
    assert r.json() == []


def test_exclude_planned_splits_gap_into_remaining_segments(client):
    """计划覆盖缺报段中段：首尾保留，窗口数按剩余长度计。"""
    seed_gap(client)
    # 原始缺口 [00:01, 00:03)，计划只盖 00:02 这一窗
    register(client, "dev-1", "2026-09-22T00:02:00Z", "2026-09-22T00:03:00Z")
    r = missing(
        client, "dev-1", "2026-09-22T00:00:00Z", "2026-09-22T00:10:00Z",
        exclude_planned=True,
    )
    assert r.json() == [
        {"start": "2026-09-22T00:01:00Z", "end": "2026-09-22T00:02:00Z", "missing_windows": 1}
    ]

    # 计划落在段中部，两侧都保留
    r2 = register(client, "dev-1", "2026-09-22T00:05:00Z", "2026-09-22T00:07:00Z")
    assert r2.status_code == 201
    ingest(client, "dev-1", "e4", "2026-09-22T00:10:00Z")  # 新缺口 [00:05, 00:09)（4 窗）
    r3 = missing(
        client, "dev-1", "2026-09-22T00:00:00Z", "2026-09-22T00:12:00Z",
        exclude_planned=True,
    )
    assert r3.json() == [
        {"start": "2026-09-22T00:01:00Z", "end": "2026-09-22T00:02:00Z", "missing_windows": 1},
        {"start": "2026-09-22T00:07:00Z", "end": "2026-09-22T00:09:00Z", "missing_windows": 2},
    ]


def test_touching_boundaries_are_not_overlap(client):
    """计划端点与缺报段/查询范围端点触碰（半开）不算重叠，不扣除。"""
    seed_gap(client)
    # 计划 [00:00, 00:01)：右端点正好触碰缺口起点 00:01 → 不重叠
    register(client, "dev-1", "2026-09-22T00:00:00Z", "2026-09-22T00:01:00Z")
    # 计划 [00:03, 00:04)：左端点正好触碰缺口终点 00:03 → 不重叠
    register(client, "dev-1", "2026-09-22T00:03:00Z", "2026-09-22T00:04:00Z")
    r = missing(
        client, "dev-1", "2026-09-22T00:00:00Z", "2026-09-22T00:10:00Z",
        exclude_planned=True,
    )
    assert r.json() == [
        {"start": "2026-09-22T00:01:00Z", "end": "2026-09-22T00:03:00Z", "missing_windows": 2}
    ]

    # 计划与查询范围仅端点触碰：[00:10, 00:11) 触碰查询 end=00:10 → 不参与扣除
    register(client, "dev-1", "2026-09-22T00:10:00Z", "2026-09-22T00:11:00Z")
    r2 = missing(
        client, "dev-1", "2026-09-22T00:00:00Z", "2026-09-22T00:10:00Z",
        exclude_planned=True,
    )
    assert [s["missing_windows"] for s in r2.json()] == [2]


def test_touching_planned_intervals_each_deduct_themselves(client):
    """两条仅端点相接的计划不算互相重叠，但各自覆盖的窗口都扣除（并集连续）。"""
    seed_gap(client)
    register(client, "dev-1", "2026-09-22T00:01:00Z", "2026-09-22T00:02:00Z")
    register(client, "dev-1", "2026-09-22T00:02:00Z", "2026-09-22T00:03:00Z")
    r = missing(
        client, "dev-1", "2026-09-22T00:00:00Z", "2026-09-22T00:10:00Z",
        exclude_planned=True,
    )
    assert r.json() == []


def test_overlapping_planned_intervals_deducted_once(client):
    """对齐网格上互相重叠的多条计划，重叠部分合并后只扣一次。"""
    seed_gap(client, "dev-1")
    # 三条计划两两重叠，首尾均超出缺口边界；并集为连续的一大段
    register(client, "dev-1", "2026-09-22T00:00:00Z", "2026-09-22T00:02:00Z")
    register(client, "dev-1", "2026-09-22T00:01:00Z", "2026-09-22T00:03:00Z")
    register(client, "dev-1", "2026-09-22T00:02:00Z", "2026-09-22T00:04:00Z")
    r = missing(
        client, "dev-1", "2026-09-22T00:00:00Z", "2026-09-22T00:10:00Z",
        exclude_planned=True,
    )
    # 并集 [00:00, 00:04) 完全覆盖缺口 [00:01, 00:03)；若重复扣除会出现负/零长段，
    # 实际结果恰好为空，证明重叠窗口只扣了一次
    assert r.json() == []


def test_partial_overlap_at_query_range_edges(client):
    """计划横跨查询范围边界：只扣除范围内相交部分。"""
    seed_gap(client)
    # 计划从查询范围之前开始，延伸进缺口
    register(client, "dev-1", "2026-09-21T23:00:00Z", "2026-09-22T00:02:00Z")
    r = missing(
        client, "dev-1", "2026-09-22T00:00:00Z", "2026-09-22T00:10:00Z",
        exclude_planned=True,
    )
    assert r.json() == [
        {"start": "2026-09-22T00:02:00Z", "end": "2026-09-22T00:03:00Z", "missing_windows": 1}
    ]

    # 计划延伸到查询范围之后，相交部分照扣；查询 end 之外不影响
    register(client, "dev-1", "2026-09-22T00:02:00Z", "2026-09-22T02:00:00Z")
    r2 = missing(
        client, "dev-1", "2026-09-22T00:00:00Z", "2026-09-22T00:10:00Z",
        exclude_planned=True,
    )
    assert r2.json() == []


def test_planned_out_of_query_range_does_not_affect_result(client):
    """完全落在查询范围外（且不触碰相交）的计划不参与扣除。"""
    seed_gap(client)
    register(client, "dev-1", "2026-09-22T01:00:00Z", "2026-09-22T02:00:00Z")
    r = missing(
        client, "dev-1", "2026-09-22T00:00:00Z", "2026-09-22T00:10:00Z",
        exclude_planned=True,
    )
    assert r.json() == [
        {"start": "2026-09-22T00:01:00Z", "end": "2026-09-22T00:03:00Z", "missing_windows": 2}
    ]


def test_exclude_planned_does_not_report_unsealed_positions(client):
    """扣除计划不改变封存口径：水位线未越过封存条件的窗口即使没计划也不报缺。"""
    make_device(client)
    ingest(client, "dev-1", "e1", "2026-09-22T00:00:10Z")
    ingest(client, "dev-1", "e2", "2026-09-22T00:03:10Z")  # 仅 00:01 达封存条件
    # 计划盖在尚未达封存条件的 00:02 上：它本来就不是缺报，扣除后也不该凭空产生段
    register(client, "dev-1", "2026-09-22T00:02:00Z", "2026-09-22T00:03:00Z")
    r = missing(
        client, "dev-1", "2026-09-22T00:00:00Z", "2026-09-22T01:00:00Z",
        exclude_planned=True,
    )
    assert r.json() == [
        {"start": "2026-09-22T00:01:00Z", "end": "2026-09-22T00:02:00Z", "missing_windows": 1}
    ]


def test_exclude_planned_isolated_per_device(client):
    seed_gap(client, "dev-1")
    seed_gap(client, "dev-2")
    # 只为 dev-2 登记计划
    register(client, "dev-2", "2026-09-22T00:01:00Z", "2026-09-22T00:03:00Z")
    assert missing(
        client, "dev-1", "2026-09-22T00:00:00Z", "2026-09-22T00:10:00Z", exclude_planned=True
    ).json() == [
        {"start": "2026-09-22T00:01:00Z", "end": "2026-09-22T00:03:00Z", "missing_windows": 2}
    ]
    assert missing(
        client, "dev-2", "2026-09-22T00:00:00Z", "2026-09-22T00:10:00Z", exclude_planned=True
    ).json() == []


def test_exclude_planned_custom_window_size(client):
    """扣除段与窗口计数按设备自身窗口大小。"""
    make_device(client, "dev-big", window_size_seconds=300, allowed_lateness_seconds=60)
    ingest(client, "dev-big", "e1", "2026-09-22T00:00:10Z")
    ingest(client, "dev-big", "e2", "2026-09-22T00:16:00Z")  # 缺口 [00:05, 00:15)（2 窗）
    # 计划盖掉 00:10 这一个 5 分钟窗
    register(client, "dev-big", "2026-09-22T00:10:00Z", "2026-09-22T00:15:00Z")
    r = missing(
        client, "dev-big", "2026-09-22T00:00:00Z", "2026-09-22T00:20:00Z",
        exclude_planned=True,
    )
    assert r.json() == [
        {"start": "2026-09-22T00:05:00Z", "end": "2026-09-22T00:10:00Z", "missing_windows": 1}
    ]


def test_exclude_planned_404_and_422_still_apply(client):
    seed_gap(client)
    assert missing(
        client, "ghost", "2026-09-22T00:00:00Z", "2026-09-22T00:10:00Z", exclude_planned=True
    ).status_code == 404
    assert missing(
        client, "dev-1", "2026-09-22T00:00:30Z", "2026-09-22T00:10:00Z", exclude_planned=True
    ).status_code == 422
    assert missing(
        client, "dev-1", "2026-09-22T00:10:00Z", "2026-09-22T00:00:00Z", exclude_planned=True
    ).status_code == 422


def test_exclude_planned_query_is_readonly(client):
    seed_gap(client)
    register(client, "dev-1", "2026-09-22T00:01:00Z", "2026-09-22T00:03:00Z")
    before_planned = list_planned(client, "dev-1").json()
    before_windows = client.get("/devices/dev-1/windows").json()
    for _ in range(3):
        missing(
            client, "dev-1", "2026-09-22T00:00:00Z", "2026-09-22T00:10:00Z",
            exclude_planned=True,
        )
    assert list_planned(client, "dev-1").json() == before_planned
    assert client.get("/devices/dev-1/windows").json() == before_windows
