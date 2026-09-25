"""设备可版本化标签测试：全量替换、规范化、重复键拒绝、乐观并发与按标签筛选。"""

import threading

from fastapi.testclient import TestClient

from app import db


def make_device(client, device_id="dev-1"):
    r = client.put(f"/devices/{device_id}/config", json={})
    assert r.status_code == 200


def put_labels(client, device_id, labels, if_version=None):
    body = {"labels": labels}
    if if_version is not None:
        body["if_version"] = if_version
    return client.put(f"/devices/{device_id}/labels", json=body)


# ---------- 基本读写与版本 ----------

def test_get_labels_on_never_labelled_device_is_version_zero(client):
    make_device(client)
    r = client.get("/devices/dev-1/labels")
    assert r.status_code == 200
    body = r.json()
    assert body["version"] == 0
    assert body["labels"] == {}
    assert body["updated_at"] is None


def test_put_creates_labels_at_version_one(client):
    make_device(client)
    r = put_labels(client, "dev-1", [
        {"key": "room", "value": "hall-A"},
        {"key": "floor", "value": "3"},
    ])
    assert r.status_code == 200
    body = r.json()
    assert body["version"] == 1
    assert body["labels"] == {"floor": "3", "room": "hall-A"}
    assert body["updated_at"] is not None

    got = client.get("/devices/dev-1/labels").json()
    assert got["version"] == 1
    assert got["labels"] == {"floor": "3", "room": "hall-A"}
    assert got["updated_at"] == body["updated_at"]


def test_put_is_full_replacement_and_version_monotonic(client):
    make_device(client)
    put_labels(client, "dev-1", [{"key": "room", "value": "hall-A"}])
    r = put_labels(client, "dev-1", [{"key": "zone", "value": "z1"}])
    assert r.status_code == 200
    assert r.json()["version"] == 2
    # 旧标签 room 已被整批替换掉
    assert client.get("/devices/dev-1/labels").json()["labels"] == {"zone": "z1"}


def test_put_empty_list_clears_labels_and_bumps_version(client):
    make_device(client)
    put_labels(client, "dev-1", [{"key": "room", "value": "hall-A"}])
    r = put_labels(client, "dev-1", [])
    assert r.status_code == 200
    assert r.json()["version"] == 2
    assert r.json()["labels"] == {}
    assert client.get("/devices/dev-1/labels").json()["labels"] == {}


def test_put_bumps_version_even_when_content_identical(client):
    make_device(client)
    put_labels(client, "dev-1", [{"key": "room", "value": "a"}])
    r = put_labels(client, "dev-1", [{"key": "room", "value": "a"}])
    assert r.status_code == 200
    assert r.json()["version"] == 2


# ---------- 规范化与校验 ----------

def test_keys_normalized_stripped_and_lowercased_values_stripped(client):
    make_device(client)
    r = put_labels(client, "dev-1", [
        {"key": "  Room-1 ", "value": "  Hall A "},
        {"key": "model.id", "value": "x1"},
        {"key": "sn_0", "value": "abc"},
        {"key": "1abc", "value": "digit-prefix-allowed"},
    ])
    assert r.status_code == 200, r.text
    assert set(r.json()["labels"]) == {"1abc", "model.id", "room-1", "sn_0"}
    assert r.json()["labels"]["room-1"] == "Hall A"  # 值不转小写，仅去首尾空白


def test_duplicate_key_after_normalization_rejected_without_change(client):
    make_device(client)
    r = put_labels(client, "dev-1", [
        {"key": "Room", "value": "a"},
        {"key": " room ", "value": "b"},
    ])
    assert r.status_code == 400
    assert "duplicate" in r.json()["detail"]
    # 版本与数据不变
    assert client.get("/devices/dev-1/labels").json()["version"] == 0

    put_labels(client, "dev-1", [{"key": "room", "value": "a"}])
    r = put_labels(client, "dev-1", [
        {"key": "ROOM", "value": "b"},
        {"key": "room", "value": "c"},
    ])
    assert r.status_code == 400
    body = client.get("/devices/dev-1/labels").json()
    assert body["version"] == 1
    assert body["labels"] == {"room": "a"}


def test_invalid_keys_and_values_rejected(client):
    make_device(client)
    for bad_key in ["_room", "-room", ".room", "ro om", "ro/om", "café", "   "]:
        r = put_labels(client, "dev-1", [{"key": bad_key, "value": "v"}])
        assert r.status_code == 400, (bad_key, r.status_code)

    for bad_value in ["", "   "]:
        r = put_labels(client, "dev-1", [{"key": "room", "value": bad_value}])
        assert r.status_code == 400, (bad_value, r.status_code)

    assert client.get("/devices/dev-1/labels").json()["version"] == 0


def test_more_than_twenty_labels_rejected(client):
    make_device(client)
    labels = [{"key": f"k{i}", "value": "v"} for i in range(21)]
    assert put_labels(client, "dev-1", labels).status_code == 422

    ok = [{"key": f"k{i}", "value": "v"} for i in range(20)]
    assert put_labels(client, "dev-1", ok).status_code == 200


def test_labels_404_for_unknown_device(client):
    assert client.get("/devices/ghost/labels").status_code == 404
    assert put_labels(client, "ghost", [{"key": "a", "value": "b"}]).status_code == 404


# ---------- 乐观并发 ----------

def test_if_version_zero_accepted_for_first_write(client):
    make_device(client)
    r = put_labels(client, "dev-1", [{"key": "a", "value": "b"}], if_version=0)
    assert r.status_code == 200
    assert r.json()["version"] == 1


def test_stale_if_version_conflicts_and_changes_nothing(client):
    make_device(client)
    put_labels(client, "dev-1", [{"key": "a", "value": "1"}])  # version 1

    r = put_labels(client, "dev-1", [{"key": "a", "value": "2"}], if_version=0)
    assert r.status_code == 409
    assert "1" in r.json()["detail"]  # 错误信息带上当前版本

    # 用最新版本重试成功
    r = put_labels(client, "dev-1", [{"key": "a", "value": "2"}], if_version=1)
    assert r.status_code == 200
    assert r.json()["version"] == 2
    assert client.get("/devices/dev-1/labels").json()["labels"] == {"a": "2"}


def test_invalid_payload_with_stale_version_kept_at_400_and_unchanged(client):
    make_device(client)
    put_labels(client, "dev-1", [{"key": "a", "value": "1"}])
    # 请求载荷本身非法（重复键）时先 400；即使 if_version 已过期，也绝不写数据
    r = put_labels(client, "dev-1", [
        {"key": "a", "value": "x"}, {"key": "a", "value": "y"}
    ], if_version=99)
    assert r.status_code == 400
    body = client.get("/devices/dev-1/labels").json()
    assert body["version"] == 1
    assert body["labels"] == {"a": "1"}


def test_concurrent_writers_exactly_one_wins(client):
    """两个写者带同一 if_version 并发提交：写锁串行化后一个成功、另一个 409。"""
    make_device(client)
    put_labels(client, "dev-1", [{"key": "a", "value": "0"}])  # version 1

    results: list[int] = []
    barrier = threading.Barrier(2)

    def worker(value):
        barrier.wait()
        r = put_labels(client, "dev-1", [{"key": "a", "value": value}], if_version=1)
        results.append(r.status_code)

    t1 = threading.Thread(target=worker, args=("1",))
    t2 = threading.Thread(target=worker, args=("2",))
    t1.start(); t2.start(); t1.join(); t2.join()

    assert sorted(results) == [200, 409]
    body = client.get("/devices/dev-1/labels").json()
    assert body["version"] == 2
    assert body["labels"]["a"] in {"1", "2"}


# ---------- 按当前标签筛选设备 ----------

def seed_labelled_devices(client):
    make_device(client, "dev-1")
    make_device(client, "dev-2")
    make_device(client, "dev-3")
    put_labels(client, "dev-1", [
        {"key": "room", "value": "Hall-A"},
        {"key": "floor", "value": "3"},
    ])
    put_labels(client, "dev-2", [{"key": "room", "value": "Hall-A"}])
    put_labels(client, "dev-3", [{"key": "room", "value": "Hall-B"}])


def test_list_devices_without_filter_returns_all(client):
    seed_labelled_devices(client)
    ids = [d["device_id"] for d in client.get("/devices").json()]
    assert ids == ["dev-1", "dev-2", "dev-3"]


def test_filter_by_key_and_value(client):
    seed_labelled_devices(client)
    r = client.get("/devices", params={"label_key": "room", "label_value": "Hall-A"})
    assert [d["device_id"] for d in r.json()] == ["dev-1", "dev-2"]

    # 查询键名同样规范化；查询值去首尾空白
    r = client.get("/devices", params={"label_key": " ROOM ", "label_value": " Hall-B "})
    assert [d["device_id"] for d in r.json()] == ["dev-3"]

    r = client.get("/devices", params={"label_key": "floor", "label_value": "3"})
    assert [d["device_id"] for d in r.json()] == ["dev-1"]

    # 值不匹配
    r = client.get("/devices", params={"label_key": "room", "label_value": "nowhere"})
    assert r.json() == []


def test_filter_by_key_only(client):
    seed_labelled_devices(client)
    r = client.get("/devices", params={"label_key": "floor"})
    assert [d["device_id"] for d in r.json()] == ["dev-1"]

    r = client.get("/devices", params={"label_key": "missing"})
    assert r.json() == []


def test_filter_value_without_key_is_422(client):
    seed_labelled_devices(client)
    assert client.get("/devices", params={"label_value": "Hall-A"}).status_code == 422


def test_filter_sees_only_current_labels(client):
    make_device(client, "dev-1")
    make_device(client, "dev-2")
    put_labels(client, "dev-1", [{"key": "room", "value": "Hall-A"}])
    put_labels(client, "dev-2", [{"key": "room", "value": "Hall-A"}])
    # dev-1 改成别的房间：旧版本标签不再参与筛选
    put_labels(client, "dev-1", [{"key": "room", "value": "Hall-Z"}])

    r = client.get("/devices", params={"label_key": "room", "label_value": "Hall-A"})
    assert [d["device_id"] for d in r.json()] == ["dev-2"]


# ---------- 旧库迁移 ----------

def test_legacy_database_gets_migrated_with_version_zero(tmp_path, monkeypatch):
    # 构造不含 label_version / labels_updated_at 列的旧库
    db_file = tmp_path / "legacy.db"
    monkeypatch.setenv("TELEMETRY_DB_PATH", str(db_file))
    import sqlite3

    conn = sqlite3.connect(str(db_file))
    conn.execute(
        "CREATE TABLE devices (device_id TEXT PRIMARY KEY, window_size_seconds INTEGER"
        " NOT NULL DEFAULT 60, allowed_lateness_seconds INTEGER NOT NULL DEFAULT 30,"
        " watermark REAL, created_at TEXT NOT NULL)"
    )
    conn.execute(
        "INSERT INTO devices (device_id, created_at) VALUES ('dev-1', '2026-01-01T00:00:00Z')"
    )
    conn.commit()
    conn.close()

    db.init_db()  # 就地迁移：补列、建标签表
    with db.session() as c:
        row = c.execute("SELECT label_version, labels_updated_at FROM devices").fetchone()
        assert row["label_version"] == 0 and row["labels_updated_at"] is None

    from app.main import app

    with TestClient(app) as c:
        body = c.get("/devices/dev-1/labels").json()
        assert body["version"] == 0 and body["labels"] == {}

        r = put_labels(c, "dev-1", [{"key": "room", "value": "Hall-A"}])
        assert r.status_code == 200
        assert r.json()["version"] == 1

        # 迁移后的列在筛选接口上同样工作
        r = c.get("/devices", params={"label_key": "room", "label_value": "Hall-A"})
        assert [d["device_id"] for d in r.json()] == ["dev-1"]
