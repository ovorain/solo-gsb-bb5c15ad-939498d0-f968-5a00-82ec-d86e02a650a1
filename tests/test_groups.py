"""设备组同步窗口汇总测试。"""


def ingest(client, device_id, events):
    return client.post("/ingest", json={"device_id": device_id, "events": events})


def seal_window(client, device_id, at="2026-09-22T00:02:00Z", value=99.0, event_id=None):
    """推进水位线到 00:02:00，封存窗口 [00:00, 00:01)（默认 lateness=30s）。"""
    event_id = event_id or f"seal-{device_id}"
    return ingest(client, device_id, [
        {"event_id": event_id, "event_time": at, "value": value},
    ])


def make_group(client, group_id="g1", members=("d1", "d2"), aggregation="sum",
               window_size=60, lateness=30):
    return client.post("/groups", json={
        "group_id": group_id,
        "member_ids": list(members),
        "aggregation": aggregation,
        "window_size_seconds": window_size,
        "allowed_lateness_seconds": lateness,
    })


def test_create_group_validation_errors(client):
    # 成员不存在
    r = make_group(client, members=("ghost",))
    assert r.status_code == 404

    ingest(client, "d1", [{"event_id": "e1", "event_time": "2026-09-22T00:00:10Z", "value": 1.0}])
    ingest(client, "d2", [{"event_id": "e1", "event_time": "2026-09-22T00:00:10Z", "value": 1.0}])

    # 窗口配置与组要求不一致
    r = make_group(client, members=("d1", "d2"), window_size=120)
    assert r.status_code == 409

    # 成员重复
    r = make_group(client, members=("d1", "d1"))
    assert r.status_code == 400

    # 正常创建
    r = make_group(client)
    assert r.status_code == 201
    body = r.json()
    assert body["member_ids"] == ["d1", "d2"]
    assert body["aggregation"] == "sum"

    # 重复创建
    assert make_group(client).status_code == 409

    assert client.get("/groups/ghost").status_code == 404


def test_group_window_pending_until_all_members_sealed(client):
    ingest(client, "d1", [{"event_id": "e1", "event_time": "2026-09-22T00:00:10Z", "value": 10.0}])
    ingest(client, "d2", [{"event_id": "e1", "event_time": "2026-09-22T00:00:20Z", "value": 20.0}])
    assert make_group(client).status_code == 201

    ws = client.get("/groups/g1/windows").json()
    assert len(ws) == 1
    w = ws[0]
    assert w["status"] == "pending"
    assert w["value"] is None  # 绝不用部分成员提前计算
    assert w["revision"] == 0
    assert (w["member_count"], w["sealed_member_count"]) == (2, 0)

    # 仅封存 d1 的窗口：仍 pending
    seal_window(client, "d1", event_id="s1")
    w = client.get("/groups/g1/windows").json()[0]
    assert w["status"] == "pending"
    assert w["value"] is None
    assert w["sealed_member_count"] == 1

    # 可按 status 过滤
    pending = client.get("/groups/g1/windows?status=pending").json()
    assert {w["window_start"] for w in pending} == {
        "2026-09-22T00:00:00Z",  # d2 未封存
        "2026-09-22T00:02:00Z",  # 封存事件自身所在窗口，d2 尚无数据
    }
    assert client.get("/groups/g1/windows?status=ready").json() == []


def test_group_window_ready_sum_after_all_sealed(client):
    ingest(client, "d1", [{"event_id": "e1", "event_time": "2026-09-22T00:00:10Z", "value": 10.0}])
    ingest(client, "d2", [{"event_id": "e1", "event_time": "2026-09-22T00:00:20Z", "value": 20.0}])
    make_group(client)

    seal_window(client, "d1", event_id="s1")
    seal_window(client, "d2", event_id="s2")

    ws = client.get("/groups/g1/windows").json()
    w0 = [w for w in ws if w["window_start"] == "2026-09-22T00:00:00Z"][0]
    assert w0["status"] == "ready"
    assert w0["value"] == 30.0  # 10 + 20
    assert w0["revision"] == 1
    assert w0["sealed_member_count"] == 2
    assert w0["ready_at"] is not None

    # 单窗口查询（路径参数 ISO 时间）
    single = client.get("/groups/g1/windows/2026-09-22T00:00:00Z").json()
    assert single["status"] == "ready"
    assert single["value"] == 30.0

    # ready 后 pending 明细为空
    assert client.get("/groups/g1/windows/2026-09-22T00:00:00Z/pending").json() == []

    # 没有任何成员数据的窗口 404
    assert client.get("/groups/g1/windows/2030-01-01T00:00:00Z").status_code == 404
    assert client.get("/groups/g1/windows/2030-01-01T00:00:00Z/pending").status_code == 404


def test_group_avg_is_mean_of_member_window_avgs(client):
    # d1 窗口内 2 个事件（avg 15），d2 窗口内 1 个事件（avg 40）
    ingest(client, "d1", [
        {"event_id": "a", "event_time": "2026-09-22T00:00:10Z", "value": 10.0},
        {"event_id": "b", "event_time": "2026-09-22T00:00:20Z", "value": 20.0},
    ])
    ingest(client, "d2", [
        {"event_id": "c", "event_time": "2026-09-22T00:00:20Z", "value": 40.0},
    ])
    make_group(client, aggregation="avg")

    seal_window(client, "d1", event_id="s1")
    seal_window(client, "d2", event_id="s2")

    w0 = client.get("/groups/g1/windows/2026-09-22T00:00:00Z").json()
    assert w0["status"] == "ready"
    assert w0["value"] == (15.0 + 40.0) / 2


def test_pending_member_details_missing_and_open(client):
    # d3 仅预注册配置、无任何窗口数据，用于构造 missing 场景
    client.put("/devices/d3/config", json={"window_size_seconds": 60, "allowed_lateness_seconds": 30})
    ingest(client, "d1", [{"event_id": "e1", "event_time": "2026-09-22T00:00:10Z", "value": 1.0}])
    ingest(client, "d2", [{"event_id": "e1", "event_time": "2026-09-22T00:00:20Z", "value": 2.0}])
    make_group(client, members=("d1", "d2", "d3"))

    # d3 尚无 00:00 窗口数据：missing；d1/d2 有 open 窗口
    pending = client.get("/groups/g1/windows/2026-09-22T00:00:00Z/pending").json()
    by_device = {p["device_id"]: p for p in pending}
    assert by_device["d1"]["reason"] == "open"
    assert by_device["d1"]["window_status"] == "open"
    assert by_device["d2"]["reason"] == "open"
    assert by_device["d3"]["reason"] == "missing"
    assert by_device["d3"]["window_status"] is None
    assert by_device["d3"]["window_end"] == "2026-09-22T00:01:00Z"

    # 封存 d1、d2，仅剩 d3 missing
    seal_window(client, "d1", event_id="s1")
    seal_window(client, "d2", event_id="s2")
    pending = client.get("/groups/g1/windows/2026-09-22T00:00:00Z/pending").json()
    assert [p["device_id"] for p in pending] == ["d3"]
    assert pending[0]["reason"] == "missing"

    # d3 上报但不封存：open
    ingest(client, "d3", [{"event_id": "e2", "event_time": "2026-09-22T00:00:30Z", "value": 4.0}])
    pending = client.get("/groups/g1/windows/2026-09-22T00:00:00Z/pending").json()
    assert pending[0]["reason"] == "open"

    # 全部封存后 ready；d3 的 00:00 窗口只含 e2(4.0)
    seal_window(client, "d3", event_id="s3")
    w = client.get("/groups/g1/windows/2026-09-22T00:00:00Z").json()
    assert w["status"] == "ready"
    assert w["value"] == 1.0 + 2.0 + 4.0


def test_late_correction_recomputes_and_bumps_revision(client):
    ingest(client, "d1", [{"event_id": "e1", "event_time": "2026-09-22T00:00:10Z", "value": 10.0}])
    ingest(client, "d2", [{"event_id": "e1", "event_time": "2026-09-22T00:00:20Z", "value": 20.0}])
    make_group(client)

    seal_window(client, "d1", event_id="s1")
    seal_window(client, "d2", event_id="s2")
    w = client.get("/groups/g1/windows/2026-09-22T00:00:00Z").json()
    assert w["value"] == 30.0
    assert w["revision"] == 1
    ready_at = w["ready_at"]

    # d1 迟到事件修正成员窗口 → 组汇总原位重算，revision=2，ready_at 不变
    ingest(client, "d1", [
        {"event_id": "late-1", "event_time": "2026-09-22T00:00:30Z", "value": 5.0},
    ])
    w = client.get("/groups/g1/windows/2026-09-22T00:00:00Z").json()
    assert w["status"] == "ready"
    assert w["value"] == 35.0
    assert w["revision"] == 2
    assert w["ready_at"] == ready_at

    # 同一 event_id 重试：幂等去重，不产生新修正，revision 不重复升版
    ingest(client, "d1", [
        {"event_id": "late-1", "event_time": "2026-09-22T00:00:30Z", "value": 5.0},
    ])
    w = client.get("/groups/g1/windows/2026-09-22T00:00:00Z").json()
    assert w["value"] == 35.0
    assert w["revision"] == 2

    # 另一个迟到事件 → revision=3
    ingest(client, "d1", [
        {"event_id": "late-2", "event_time": "2026-09-22T00:00:40Z", "value": 1.0},
    ])
    w = client.get("/groups/g1/windows/2026-09-22T00:00:00Z").json()
    assert w["value"] == 36.0
    assert w["revision"] == 3


def test_late_correction_before_ready_does_not_inflate_revision(client):
    # 先封存 d1，迟到修正先发生；之后 d2 才封存 → ready 时 revision 直接从 1 起
    ingest(client, "d1", [{"event_id": "e1", "event_time": "2026-09-22T00:00:10Z", "value": 10.0}])
    ingest(client, "d2", [{"event_id": "e1", "event_time": "2026-09-22T00:00:20Z", "value": 20.0}])
    make_group(client)

    seal_window(client, "d1", event_id="s1")
    ingest(client, "d1", [
        {"event_id": "late-1", "event_time": "2026-09-22T00:00:30Z", "value": 5.0},
    ])
    # d1 迟到修正时组仍 pending
    w = client.get("/groups/g1/windows/2026-09-22T00:00:00Z").json()
    assert w["status"] == "pending"

    seal_window(client, "d2", event_id="s2")
    w = client.get("/groups/g1/windows/2026-09-22T00:00:00Z").json()
    assert w["status"] == "ready"
    assert w["value"] == 35.0
    assert w["revision"] == 1


def test_group_created_after_members_already_sealed_backfills(client):
    ingest(client, "d1", [{"event_id": "e1", "event_time": "2026-09-22T00:00:10Z", "value": 10.0}])
    ingest(client, "d2", [{"event_id": "e1", "event_time": "2026-09-22T00:00:20Z", "value": 20.0}])
    seal_window(client, "d1", event_id="s1")
    seal_window(client, "d2", event_id="s2")

    # 组在成员窗口全部封存后才创建：创建即补算 ready
    assert make_group(client).status_code == 201
    ws = client.get("/groups/g1/windows").json()
    ready = [w for w in ws if w["window_start"] == "2026-09-22T00:00:00Z"]
    assert len(ready) == 1
    assert ready[0]["status"] == "ready"
    assert ready[0]["value"] == 30.0
    assert ready[0]["revision"] == 1


def test_group_requires_matching_window_config(client):
    # 用 PUT config 建一个 300s 窗口的设备，另一个用默认 60s
    client.put("/devices/d1/config", json={"window_size_seconds": 300, "allowed_lateness_seconds": 30})
    ingest(client, "d2", [{"event_id": "e1", "event_time": "2026-09-22T00:00:20Z", "value": 20.0}])
    r = make_group(client, members=("d1", "d2"), window_size=60)
    assert r.status_code == 409
    r = make_group(client, members=("d1", "d2"), window_size=300)
    assert r.status_code == 409
