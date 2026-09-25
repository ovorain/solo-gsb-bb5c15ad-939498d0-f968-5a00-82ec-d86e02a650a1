"""跨设备窗口检索 GET /windows/search 测试。

覆盖：排序、时间范围半开区间、状态/标签筛选、稳定游标分页（翻页期间新增数据
不重复/不遗漏）、游标非法/篡改/过期/与筛选条件不一致返回 400、参数校验 422。
"""


def make_device(client, device_id):
    r = client.put(f"/devices/{device_id}/config", json={})
    assert r.status_code == 200


def ingest(client, device_id, event_id, ts, value=1.0):
    r = client.post(
        "/ingest",
        json={"device_id": device_id, "events": [
            {"event_id": event_id, "event_time": ts, "value": value}
        ]},
    )
    assert r.status_code == 200, r.text
    return r


def put_labels(client, device_id, labels):
    r = client.put(f"/devices/{device_id}/labels", json={"labels": labels})
    assert r.status_code == 200, r.text


def search(client, **params):
    return client.get("/windows/search", params=params)


def keys(body):
    return [(w["window_start"], w["device_id"]) for w in body["windows"]]


def walk_all_pages(client, page_params=None, max_pages=100):
    """从首页开始按 next_cursor 翻完，返回 (全部键, 每一页的键列表)。"""
    page_params = page_params or {}
    seen = []
    pages = []
    cursor = None
    for _ in range(max_pages):
        params = dict(page_params)
        if cursor is not None:
            params["cursor"] = cursor
        body = search(client, **params).json()
        page = keys(body)
        pages.append(page)
        seen.extend(page)
        cursor = body["next_cursor"]
        if cursor is None:
            break
    return seen, pages


def seed(client):
    """窗口集合（按 window_start, device_id）：
    (00:00, d1 sealed), (00:00, d2 open), (00:01, d3 open), (00:02, d1 open)
    """
    for d in ("d1", "d2", "d3"):
        make_device(client, d)
    put_labels(client, "d1", [{"key": "room", "value": "Hall-A"}])
    put_labels(client, "d2", [{"key": "room", "value": "Hall-A"}])
    put_labels(client, "d3", [{"key": "room", "value": "Hall-B"}])

    ingest(client, "d1", "e1", "2026-09-22T00:00:10Z")
    # 水位线推进到 00:02:00 => d1 的 [00:00, 00:01) 封存
    ingest(client, "d1", "e2", "2026-09-22T00:02:00Z")
    ingest(client, "d2", "e3", "2026-09-22T00:00:20Z")
    ingest(client, "d3", "e4", "2026-09-22T00:01:05Z")


EXPECTED = [
    ("2026-09-22T00:00:00Z", "d1"),
    ("2026-09-22T00:00:00Z", "d2"),
    ("2026-09-22T00:01:00Z", "d3"),
    ("2026-09-22T00:02:00Z", "d1"),
]


# ---------- 排序与筛选 ----------

def test_search_returns_window_aggregations_sorted_by_start_then_device(client):
    seed(client)
    body = search(client).json()
    assert body["next_cursor"] is None  # 不足一页
    assert keys(body) == EXPECTED

    first = body["windows"][0]
    # 返回窗口聚合与设备标识（结构与 GET /devices/{id}/windows 一致）
    assert first["device_id"] == "d1"
    assert first["window_start"] == "2026-09-22T00:00:00Z"
    assert first["window_end"] == "2026-09-22T00:01:00Z"
    assert first["status"] == "sealed"
    assert (first["count"], first["sum"], first["avg"]) == (1, 1.0, 1.0)
    assert first["sealed_at"] is not None

    # 同一 window_start 下按 device_id 升序
    tied = [k for k in keys(body) if k[0] == "2026-09-22T00:00:00Z"]
    assert [d for _, d in tied] == ["d1", "d2"]


def test_search_time_range_is_half_open_on_window_start(client):
    seed(client)
    # [00:00, 00:01)：只命中 window_start=00:00 的两个窗口
    body = search(
        client,
        start="2026-09-22T00:00:00Z",
        end="2026-09-22T00:01:00Z",
    ).json()
    assert keys(body) == EXPECTED[:2]

    # 仅给 start（开区间上界不限）
    body = search(client, start="2026-09-22T00:01:00Z").json()
    assert keys(body) == EXPECTED[2:]

    # 仅给 end：window_start < end
    body = search(client, end="2026-09-22T00:02:00Z").json()
    assert keys(body) == EXPECTED[:3]

    # 范围没有任何窗口
    body = search(
        client,
        start="2026-09-23T00:00:00Z",
        end="2026-09-23T01:00:00Z",
    ).json()
    assert body["windows"] == [] and body["next_cursor"] is None


def test_search_status_filter(client):
    seed(client)
    sealed = search(client, status="sealed").json()
    assert keys(sealed) == [("2026-09-22T00:00:00Z", "d1")]

    opened = search(client, status="open").json()
    assert keys(opened) == [
        ("2026-09-22T00:00:00Z", "d2"),
        ("2026-09-22T00:01:00Z", "d3"),
        ("2026-09-22T00:02:00Z", "d1"),
    ]


def test_search_label_key_and_value_filter(client):
    seed(client)
    body = search(client, label_key="room", label_value="Hall-A").json()
    assert keys(body) == [
        ("2026-09-22T00:00:00Z", "d1"),
        ("2026-09-22T00:00:00Z", "d2"),
        ("2026-09-22T00:02:00Z", "d1"),
    ]


def test_search_label_key_only_filter(client):
    seed(client)
    body = search(client, label_key="room").json()
    assert keys(body) == EXPECTED  # 三台设备都有 room 键


def test_search_label_filter_normalized_like_devices_list(client):
    seed(client)
    # 键名去空白转小写、值去首尾空白后再匹配
    body = search(client, **{"label_key": " ROOM ", "label_value": " Hall-B "}).json()
    assert keys(body) == [("2026-09-22T00:01:00Z", "d3")]

    # 不存在的键/值
    assert search(client, label_key="nope").json()["windows"] == []
    assert search(client, label_key="room", label_value="X").json()["windows"] == []


def test_search_label_value_without_key_is_422(client):
    seed(client)
    assert search(client, label_value="Hall-A").status_code == 422


def test_search_empty_database(client):
    body = search(client).json()
    assert body == {"windows": [], "next_cursor": None}


# ---------- 参数校验 ----------

def test_search_inverted_time_range_is_422(client):
    seed(client)
    r = search(
        client,
        start="2026-09-22T00:02:00Z",
        end="2026-09-22T00:00:00Z",
    )
    assert r.status_code == 422

    # end == start（空区间）同样拒绝
    r = search(
        client,
        start="2026-09-22T00:00:00Z",
        end="2026-09-22T00:00:00Z",
    )
    assert r.status_code == 422


def test_search_limit_bounds(client):
    seed(client)
    assert search(client, limit=0).status_code == 422
    assert search(client, limit=1001).status_code == 422


# ---------- 游标分页 ----------

def test_cursor_pagination_walks_every_row_exactly_once(client):
    seed(client)
    seen, pages = walk_all_pages(client, {"limit": 2})
    assert seen == EXPECTED
    assert [len(p) for p in pages] == [2, 2]
    # 首页还有剩余数据时带游标，末页 next_cursor 为 null（walk 已验证末页终止）
    assert search(client, limit=2).json()["next_cursor"] is not None

    seen, pages = walk_all_pages(client, {"limit": 3})
    assert seen == EXPECTED
    assert [len(p) for p in pages] == [3, 1]


def test_last_page_next_cursor_is_null(client):
    seed(client)
    body = search(client, limit=100).json()
    assert len(body["windows"]) == 4
    assert body["next_cursor"] is None


def test_new_rows_inserted_during_pagination_do_not_duplicate_or_lose(client):
    """翻页期间写入新窗口（rowid 超出首页快照上界）：进行中的遍历保持稳定。"""
    seed(client)

    first = search(client, limit=2).json()
    cursor = first["next_cursor"]
    assert cursor is not None

    # 翻页过程中新增两种数据：一个更晚的窗口（d3@00:05）与一个更早时间点的新设备窗口（d9@00:00）
    make_device(client, "d9")
    ingest(client, "d9", "e9", "2026-09-22T00:00:05Z")
    ingest(client, "d3", "e5", "2026-09-22T00:05:00Z")

    walk = keys(first)
    while cursor is not None:
        body = search(client, limit=2, cursor=cursor).json()
        walk.extend(keys(body))
        cursor = body["next_cursor"]

    # 进行中的遍历仍精确返回首页时刻的快照：不重复、不遗漏，也不混入新行
    assert sorted(walk) == sorted(EXPECTED)
    assert len(walk) == len(set(walk)) == 4

    # 不带游标重新检索才能看到新增数据（d9@00:00 按排序插入到中间）
    fresh = keys(search(client).json())
    assert ("2026-09-22T00:00:00Z", "d9") in fresh
    assert ("2026-09-22T00:05:00Z", "d3") in fresh
    assert len(fresh) == 6
    assert fresh == sorted(fresh)


def test_cursor_is_opaque_and_unforgeable(client):
    seed(client)
    # 完全非法的字符串
    assert search(client, cursor="garbage").status_code == 400
    assert search(client, cursor="a.b").status_code == 400

    good = search(client, limit=1).json()["next_cursor"]
    payload, sig = good.split(".")

    # 篡改签名
    forged_sig = ("0" if sig[0] != "0" else "1") + sig[1:]
    assert search(client, cursor=payload + "." + forged_sig).status_code == 400

    # 篡改载荷但保留原签名
    flipped = "A" if payload[0] != "A" else "B"
    assert search(client, cursor=flipped + payload[1:] + "." + sig).status_code == 400


def test_cursor_is_bound_to_filter_conditions(client):
    """游标只能在与首页完全一致的筛选条件下使用；规范化后等价的写法允许继续。"""
    seed(client)
    # 多封存一个 Hall-A 设备窗口（d2 推进水位线封存其 00:00 窗口），保证首页 limit=1 有下一页
    ingest(client, "d2", "e6", "2026-09-22T00:02:00Z")
    cursor = search(
        client,
        start="2026-09-22T00:00:00Z",
        end="2026-09-22T00:02:00Z",
        status="sealed",
        label_key="room",
        label_value="Hall-A",
        limit=1,
    ).json()["next_cursor"]
    assert cursor is not None

    # 丢掉 status 筛选 => 400
    assert search(
        client,
        cursor=cursor,
        start="2026-09-22T00:00:00Z",
        end="2026-09-22T00:02:00Z",
        label_key="room",
        label_value="Hall-A",
    ).status_code == 400

    # 时间范围不同 => 400
    assert search(client, cursor=cursor, status="sealed").status_code == 400

    # 标签值不同 => 400
    assert search(
        client,
        cursor=cursor,
        start="2026-09-22T00:00:00Z",
        end="2026-09-22T00:02:00Z",
        status="sealed",
        label_key="room",
        label_value="Hall-B",
    ).status_code == 400

    # 规范化后等价的键/值写法与同一筛选条件 => 可以继续翻页
    ok = search(
        client,
        cursor=cursor,
        start="2026-09-22T00:00:00Z",
        end="2026-09-22T00:02:00Z",
        status="sealed",
        **{"label_key": " ROOM ", "label_value": " Hall-A "},
    )
    assert ok.status_code == 200, ok.text
    # 第二页为另一个 Hall-A 封存窗口（d1 已在首页返回）
    assert keys(ok.json()) == [("2026-09-22T00:00:00Z", "d2")]
    assert ok.json()["next_cursor"] is None


def test_limit_can_change_between_pages(client):
    """limit 不参与游标绑定：翻页时可以换页大小。"""
    seed(client)
    first = search(client, limit=1).json()
    assert len(first["windows"]) == 1
    second = search(client, limit=10, cursor=first["next_cursor"]).json()
    assert keys(second) == EXPECTED[1:]
    assert second["next_cursor"] is None


def test_expired_cursor_returns_400(client, monkeypatch):
    import app.main as main

    seed(client)
    cursor = search(client, limit=1).json()["next_cursor"]
    assert cursor is not None

    # 令游标立即过期（decode 时按模块全局量判断）
    monkeypatch.setattr(main, "SEARCH_CURSOR_TTL_SECONDS", -1)
    r = search(client, limit=1, cursor=cursor)
    assert r.status_code == 400
    assert "expired" in r.json()["detail"]


def test_search_is_readonly(client):
    """检索不改变任何窗口聚合与状态。"""
    seed(client)
    before = search(client).json()["windows"]
    for _ in range(3):
        search(client, start="2026-09-22T00:00:00Z", status="sealed")
    after = search(client).json()["windows"]
    assert before == after


def test_existing_device_windows_endpoint_still_works(client):
    """现有接口保持兼容。"""
    seed(client)
    rows = client.get("/devices/d1/windows").json()
    assert [w["window_start"] for w in rows] == [
        "2026-09-22T00:00:00Z",
        "2026-09-22T00:02:00Z",
    ]
    assert rows[0]["status"] == "sealed"
    assert client.get("/devices/d1/windows", params={"status": "open"}).json() == [
        w for w in rows if w["status"] == "open"
    ]
