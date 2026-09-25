"""设备每日同窗基线偏移查询 GET /devices/{device_id}/daily-baseline 测试。

覆盖：UTC 逐日同格回看、仅用已封存窗口（缺失/未封存跳过、不用临近窗口代替）、
中位数/MAD 基线与偏移分数、样本不足 3 个的 insufficient_samples 状态、
MAD=0 的同值/异值退化处理、严格大于阈值才越界、迟到修正后再次查询反映最新聚合、
窗口秒数可整除一天校验、days(3..30)/threshold(>0) 参数校验、404/409、只读。
"""

import sqlite3
from datetime import datetime, timezone

import pytest

TARGET = "2026-09-22T08:00:00Z"
TARGET_DAY = "2026-09-22"
# 目标日前 1..5 天（UTC）
HIST_DAYS = ["2026-09-21", "2026-09-20", "2026-09-19", "2026-09-18", "2026-09-17"]


def make_device(client, device_id="dev-1", window_size_seconds=60, allowed_lateness_seconds=30):
    r = client.put(
        f"/devices/{device_id}/config",
        json={
            "window_size_seconds": window_size_seconds,
            "allowed_lateness_seconds": allowed_lateness_seconds,
        },
    )
    assert r.status_code == 200, r.text


def ingest(client, device_id, events):
    return client.post("/ingest", json={"device_id": device_id, "events": events})


def daily_baseline(client, device_id="dev-1", window_start=TARGET, **params):
    return client.get(
        f"/devices/{device_id}/daily-baseline",
        params={"window_start": window_start, **params},
    )


def event(eid, ts, value):
    return {"event_id": eid, "event_time": ts, "value": value}


def seed_history(client, avgs, device_id="dev-1", position="08:00"):
    """在目标日前各天同一窗位写入单事件窗口；avgs 按时间从旧到新给出
    （HIST_DAYS 按新到旧排列，故与其逆序配对）。"""
    events = []
    for day, avg in zip(reversed(HIST_DAYS), avgs):
        events.append(event(f"h-{day}", f"{day}T{position}:10Z", avg))
    return ingest(client, device_id, events)


def seal_target(client, device_id="dev-1", at=f"{TARGET_DAY}T09:01:00Z", value=1.0, eid="seal"):
    """推进水位线封存目标窗口 [08:00,08:01)（60s 窗、lateness=30s）：
    水位线 09:01:00 时 08:00 窗口满足 end+30=09:00:30 <= 09:01:00；09:01 窗口保持 open。"""
    return ingest(client, device_id, [event(eid, at, value)])


def seed_with_target(client, hist_avgs, target_avg, days=5, device_id="dev-1"):
    """写入各天 08:00 历史窗口 + 目标窗口，最后推进水位线全部封存。
    hist_avgs 按时间从旧到新给出（与 window_start 升序一致）。"""
    events = []
    for day, avg in zip(reversed(HIST_DAYS[:days]), hist_avgs):
        events.append(event(f"h-{day}", f"{day}T08:00:10Z", avg))
    events.append(event("t-1", f"{TARGET_DAY}T08:00:10Z", target_avg))
    events.append(event("seal", f"{TARGET_DAY}T09:01:00Z", 1.0))
    return ingest(client, device_id, events)


# ---------- 基本打分 ----------

def test_baseline_median_mad_and_score(client):
    # 历史 5 天均值 10,11,12,13,14：中位数 12，MAD（|v-12| 的中位数）= 2
    make_device(client)
    seed_with_target(client, [10, 11, 12, 13, 14], 20.0)

    r = daily_baseline(client, days=5, threshold=3.0)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "ok"
    assert body["sample_count"] == 5
    assert body["target_avg"] == 20.0
    assert body["baseline_median"] == 12.0
    # |v-12| = [2,1,0,1,2]，中位数为 1
    assert body["baseline_mad"] == 1.0
    assert body["deviation"] == pytest.approx(8.0)
    assert body["score"] == pytest.approx(8.0)  # |20-12|/1，无 0.6745 归一化
    assert body["breached"] is True
    assert body["threshold"] == 3.0

    # 样本按 window_start 升序，offset_days 随之由大到小
    assert [s["offset_days"] for s in body["samples"]] == [5, 4, 3, 2, 1]
    assert [s["avg"] for s in body["samples"]] == [10.0, 11.0, 12.0, 13.0, 14.0]
    for s in body["samples"]:
        assert s["window_end"] == f"{s['window_start'][:11]}08:01:00Z"


def test_score_not_breached_when_within_threshold(client):
    make_device(client)
    seed_with_target(client, [10, 11, 12, 13, 14], 13.0)  # dev=1, MAD=1, score=1

    body = daily_baseline(client, days=5, threshold=3.0).json()
    assert body["score"] == pytest.approx(1.0)
    assert body["breached"] is False


def test_score_equal_threshold_is_not_breached(client):
    # score 严格大于 threshold 才越界：等于阈值不越界
    make_device(client)
    seed_with_target(client, [10, 11, 12, 13, 14], 18.0)  # dev=6, MAD=1 => score=6
    body = daily_baseline(client, days=5, threshold=6.0).json()
    assert body["score"] == pytest.approx(6.0)
    assert body["breached"] is False
    body = daily_baseline(client, days=5, threshold=5.999).json()
    assert body["breached"] is True


def test_even_sample_count_uses_averaged_median(client):
    # 4 个有效样本：中位数取中间两个均值
    make_device(client)
    seed_with_target(client, [10, 12, 14, 16], 30.0, days=4)
    body = daily_baseline(client, days=4, threshold=1.0).json()
    assert body["sample_count"] == 4
    assert body["baseline_median"] == 13.0  # (12+14)/2
    # |10-13|,|12-13|,|14-13|,|16-13| => 3,1,1,3 => MAD=2
    assert body["baseline_mad"] == 2.0
    assert body["score"] == pytest.approx(8.5)


# ---------- 样本不足 ----------

def test_insufficient_samples_status_with_two_windows(client):
    make_device(client)
    seed_with_target(client, [10, 12], 50.0, days=2)  # 只埋 2 天历史

    r = daily_baseline(client, days=5, threshold=3.0)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "insufficient_samples"
    assert body["sample_count"] == 2
    assert body["target_avg"] == 50.0
    assert body["baseline_median"] is None
    assert body["baseline_mad"] is None
    assert body["deviation"] is None
    assert body["score"] is None
    assert body["breached"] is None
    assert [s["avg"] for s in body["samples"]] == [10.0, 12.0]


def test_insufficient_samples_with_no_history(client):
    make_device(client)
    events = [
        event("t-1", f"{TARGET_DAY}T08:00:10Z", 9.0),
        event("seal", f"{TARGET_DAY}T09:01:00Z", 1.0),
    ]
    ingest(client, "dev-1", events)

    body = daily_baseline(client, days=3, threshold=1.0).json()
    assert body["status"] == "insufficient_samples"
    assert body["sample_count"] == 0
    assert body["samples"] == []
    assert body["score"] is None and body["breached"] is None


# ---------- 缺失 / 未封存样本跳过，且不用临近窗口代替 ----------

def test_missing_same_position_days_are_skipped_not_substituted(client):
    make_device(client)
    events = []
    # 第 3 天（2026-09-19）08:00 窗位缺失：改为在同日 08:01、08:05 等临近位置有数据
    for i, day in enumerate(HIST_DAYS):
        if day == "2026-09-19":
            events.append(event(f"adj-{day}-1", f"{day}T08:01:10Z", 999.0))
            events.append(event(f"adj-{day}-2", f"{day}T08:05:10Z", 998.0))
        else:
            events.append(event(f"h-{day}", f"{day}T08:00:10Z", float(10 + i)))
    events.append(event("t-1", f"{TARGET_DAY}T08:00:10Z", 12.0))
    events.append(event("seal", f"{TARGET_DAY}T09:01:00Z", 1.0))
    ingest(client, "dev-1", events)

    body = daily_baseline(client, days=5, threshold=100.0).json()
    # 只有 4 个同格已封存样本；临近窗口 999/998 绝不能混入
    assert body["sample_count"] == 4
    assert sorted(s["avg"] for s in body["samples"]) == [10.0, 11.0, 13.0, 14.0]
    assert all(s["avg"] < 100 for s in body["samples"])


def test_unsealed_history_window_is_skipped(client, tmp_path, monkeypatch):
    make_device(client)
    seed_with_target(client, [10, 11, 12, 13, 14], 20.0)

    # 直接把第 3 天 08:00 窗口改回 open：它必须被跳过
    conn = sqlite3.connect(str(tmp_path / "test.db"))
    conn.execute(
        "UPDATE windows SET status = 'open' WHERE device_id = 'dev-1'"
        " AND window_start = ?",
        (datetime(2026, 9, 19, 8, 0, tzinfo=timezone.utc).timestamp(),),
    )
    conn.commit()
    conn.close()

    body = daily_baseline(client, days=5, threshold=3.0).json()
    assert body["sample_count"] == 4
    assert sorted(s["avg"] for s in body["samples"]) == [10.0, 11.0, 13.0, 14.0]


def test_only_requested_lookback_range_counts(client):
    make_device(client)
    # 埋 5 天历史，但 days=3 只取前 3 天（21/20/19）
    seed_with_target(client, [10, 11, 12, 13, 14], 5.0)
    body = daily_baseline(client, days=3, threshold=100.0).json()
    assert body["sample_count"] == 3
    # days=3 只回看前 3 天（21/20/19），其均值为 14,13,12，升序为 12,13,14
    assert [s["avg"] for s in body["samples"]] == [12.0, 13.0, 14.0]
    assert body["days"] == 3


# ---------- MAD=0 退化处理 ----------

def test_zero_mad_same_value_scores_zero(client):
    make_device(client)
    seed_with_target(client, [5, 5, 5], 5.0, days=3)
    body = daily_baseline(client, days=3, threshold=0.001).json()
    assert body["status"] == "ok"
    assert body["baseline_median"] == 5.0
    assert body["baseline_mad"] == 0.0
    assert body["deviation"] == 0.0
    assert body["score"] == 0.0
    assert body["breached"] is False


def test_zero_mad_float_representation_same_value_scores_zero(client):
    # 3 个 0.1 的均值为 0.10000000000000002，与历史中位数 0.1 数学同值：
    # 不得因浮点表示误差误判为异值偏移
    make_device(client)
    events = [
        event("h1", "2026-09-21T08:00:10Z", 0.1),
        event("h2", "2026-09-20T08:00:10Z", 0.1),
        event("h3", "2026-09-19T08:00:10Z", 0.1),
        event("t1", f"{TARGET_DAY}T08:00:10Z", 0.1),
        event("t2", f"{TARGET_DAY}T08:00:20Z", 0.1),
        event("t3", f"{TARGET_DAY}T08:00:30Z", 0.1),
        event("seal", f"{TARGET_DAY}T09:01:00Z", 1.0),
    ]
    ingest(client, "dev-1", events)
    body = daily_baseline(client, days=3, threshold=0.001).json()
    assert body["baseline_mad"] == 0.0
    assert body["target_avg"] == pytest.approx(0.1)
    assert body["score"] == 0.0
    assert body["breached"] is False


def test_zero_mad_different_value_scores_null_and_breached(client):
    make_device(client)
    seed_with_target(client, [5, 5, 5], 9.0, days=3)
    body = daily_baseline(client, days=3, threshold=100.0).json()
    assert body["baseline_mad"] == 0.0
    assert body["deviation"] == pytest.approx(4.0)
    assert body["score"] is None
    # 异值偏移无论阈值多大都判越界
    assert body["breached"] is True


# ---------- 迟到修正 ----------

def test_late_correction_changes_target_avg_on_requery(client):
    # 初次：目标窗口只有 100，历史 [10,11,12,13,14]，中位数 12、MAD 2 => score=44
    make_device(client)
    seed_with_target(client, [10, 11, 12, 13, 14], 100.0)
    first = daily_baseline(client, days=5, threshold=3.0).json()
    assert first["target_avg"] == 100.0
    assert first["score"] == pytest.approx(88.0)
    assert first["breached"] is True

    # 迟到事件落入已封存目标窗口：均值更新为 (100+14)/2 = 57 => |57-12|/1 = 45
    r = ingest(client, "dev-1", [event("late-1", f"{TARGET_DAY}T08:00:20Z", 14.0)])
    assert r.json()["results"][0]["status"] == "late"

    second = daily_baseline(client, days=5, threshold=3.0).json()
    assert second["target_avg"] == 57.0
    assert second["score"] == pytest.approx(45.0)
    assert second["breached"] is True


def test_late_correction_can_clear_breach(client):
    make_device(client)
    seed_with_target(client, [10, 11, 12, 13, 14], 100.0)
    assert daily_baseline(client, days=5, threshold=3.0).json()["breached"] is True

    # 两条迟到事件把目标均值拉回基线附近：(100+14+12)/3 = 42 => dev 30, MAD 1, score 30 仍越界
    ingest(client, "dev-1", [
        event("late-1", f"{TARGET_DAY}T08:00:20Z", 14.0),
        event("late-2", f"{TARGET_DAY}T08:00:21Z", 12.0),
    ])
    body = daily_baseline(client, days=5, threshold=40.0).json()
    assert body["target_avg"] == 42.0
    assert body["score"] == pytest.approx(30.0)
    assert body["breached"] is False


def test_late_event_filling_missing_history_adds_sample(client):
    # 只有 2 天历史 => 样本不足；迟到事件补入第 3 天同窗位后样本变 3 个
    make_device(client)
    seed_with_target(client, [10, 11], 50.0, days=2)
    assert daily_baseline(client, days=3, threshold=1.0).json()["status"] == "insufficient_samples"

    # 向缺失的 2026-09-19T08:00 窗口迟到补数（水位线已远在其后，新窗口随批次封存）
    ingest(client, "dev-1", [event("late-h", "2026-09-19T08:00:10Z", 12.0)])
    body = daily_baseline(client, days=3, threshold=1.0).json()
    assert body["status"] == "ok"
    assert body["sample_count"] == 3
    assert [s["offset_days"] for s in body["samples"]] == [3, 2, 1]


def test_late_correction_to_history_refreshes_baseline(client):
    # 历史第 2 天窗口初次为 11，迟到加入 15 后均值变为 13；中位数/MAD 随之变化
    make_device(client)
    seed_with_target(client, [10, 11, 12], 30.0, days=3)
    first = daily_baseline(client, days=3, threshold=1.0).json()
    assert first["baseline_median"] == 11.0

    ingest(client, "dev-1", [event("late-h", "2026-09-20T08:00:20Z", 15.0)])
    second = daily_baseline(client, days=3, threshold=1.0).json()
    # 历史均值变为 [10, 13, 12] => 中位数 12
    assert sorted(s["avg"] for s in second["samples"]) == [10.0, 12.0, 13.0]
    assert second["baseline_median"] == 12.0
    assert second["target_avg"] == 30.0


# ---------- 窗口大小与目标窗口状态 ----------

def test_window_size_not_dividing_day_returns_422(client):
    make_device(client, window_size_seconds=61)
    # 即使目标窗口不存在，也先按窗口配置拒绝
    r = daily_baseline(client, days=3, threshold=1.0)
    assert r.status_code == 422
    assert "divide one day" in r.json()["detail"]


def test_hourly_windows_share_utc_position_across_days(client):
    # 3600s 可整除 86400：每天 08:00 窗位回看
    make_device(client, window_size_seconds=3600)
    events = [
        event("h1", "2026-09-21T08:00:30Z", 10.0),
        event("h2", "2026-09-20T08:00:30Z", 11.0),
        event("h3", "2026-09-19T08:00:30Z", 12.0),
        event("t1", f"{TARGET_DAY}T08:00:30Z", 40.0),
        # 水位线推进到 09:01:00（lateness=30）封存 08:00 窗口
        event("seal", f"{TARGET_DAY}T09:01:00Z", 1.0),
    ]
    ingest(client, "dev-1", events)
    body = daily_baseline(client, days=3, threshold=3.0).json()
    assert body["sample_count"] == 3
    assert body["baseline_median"] == 11.0
    assert body["baseline_mad"] == 1.0
    assert body["score"] == pytest.approx(29.0)
    assert body["breached"] is True
    assert body["window_end"] == f"{TARGET_DAY}T09:00:00Z"


def test_full_day_window_size(client):
    # 86400s 窗口：窗位只能在 UTC 午夜；MAD=0 异值 => score=null 且越界
    make_device(client, window_size_seconds=86400, allowed_lateness_seconds=30)
    events = [
        event("h1", "2026-09-21T12:00:00Z", 5.0),
        event("h2", "2026-09-20T12:00:00Z", 5.0),
        event("h3", "2026-09-19T12:00:00Z", 5.0),
        event("t1", "2026-09-22T12:00:00Z", 9.0),
        # 水位线越过 09-23T00:00:30 封存 09-22 全天窗口
        event("seal", "2026-09-23T00:00:30Z", 1.0),
    ]
    ingest(client, "dev-1", events)
    body = daily_baseline(
        client, window_start="2026-09-22T00:00:00Z", days=3, threshold=100.0
    ).json()
    assert body["sample_count"] == 3
    assert body["baseline_mad"] == 0.0
    assert body["score"] is None
    assert body["breached"] is True


def test_target_window_open_returns_409(client):
    make_device(client)
    ingest(client, "dev-1", [event("t1", f"{TARGET_DAY}T08:00:10Z", 10.0)])
    # 水位线停在 08:00:10，目标窗口仍 open
    r = daily_baseline(client, days=3, threshold=1.0)
    assert r.status_code == 409
    assert "not sealed" in r.json()["detail"]


def test_target_window_missing_returns_404(client):
    make_device(client)
    seed_history(client, [10, 11, 12])
    seal_target(client)
    r = daily_baseline(client, days=3, threshold=1.0)
    assert r.status_code == 404
    assert "window not found" in r.json()["detail"]


def test_device_not_found_returns_404(client):
    r = daily_baseline(client, device_id="ghost", days=3, threshold=1.0)
    assert r.status_code == 404


# ---------- 参数校验 ----------

def test_days_range_validation(client):
    make_device(client)
    seed_with_target(client, [10, 11, 12], 20.0, days=3)
    assert daily_baseline(client, days=3, threshold=1.0).status_code == 200
    assert daily_baseline(client, days=30, threshold=1.0).status_code == 200
    assert daily_baseline(client, days=2, threshold=1.0).status_code == 422
    assert daily_baseline(client, days=31, threshold=1.0).status_code == 422


def test_threshold_must_be_positive(client):
    make_device(client)
    seed_with_target(client, [10, 11, 12], 20.0, days=3)
    assert daily_baseline(client, days=3, threshold=0).status_code == 422
    assert daily_baseline(client, days=3, threshold=-1.5).status_code == 422


def test_required_params_missing_returns_422(client):
    make_device(client)
    assert client.get("/devices/dev-1/daily-baseline").status_code == 422
    assert client.get(
        "/devices/dev-1/daily-baseline", params={"window_start": TARGET}
    ).status_code == 422
    assert client.get(
        "/devices/dev-1/daily-baseline",
        params={"window_start": TARGET, "days": 3},
    ).status_code == 422
    assert client.get(
        "/devices/dev-1/daily-baseline",
        params={"window_start": TARGET, "threshold": 1.0},
    ).status_code == 422


# ---------- 只读 ----------

def test_daily_baseline_is_read_only(client):
    make_device(client)
    seed_with_target(client, [10, 11, 12, 13, 14], 100.0)

    before = client.get("/devices/dev-1/windows").json()
    for kwargs in (
        {"days": 5, "threshold": 3.0},
        {"days": 3, "threshold": 0.5},
        {"days": 30, "threshold": 100.0},
    ):
        daily_baseline(client, **kwargs)
    # 错误路径同样不得写库
    daily_baseline(client, days=2, threshold=1.0)
    daily_baseline(client, device_id="ghost", days=3, threshold=1.0)

    after = client.get("/devices/dev-1/windows").json()
    assert after == before
