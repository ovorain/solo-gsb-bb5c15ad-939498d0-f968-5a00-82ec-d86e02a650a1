"""SQLite 数据访问层。

数据库路径通过环境变量 TELEMETRY_DB_PATH 配置，默认 ./telemetry.db。
每次连接时读取环境变量，便于测试隔离与容器挂载持久化。
"""

from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from typing import Iterator

SCHEMA = """
CREATE TABLE IF NOT EXISTS devices (
    device_id                TEXT PRIMARY KEY,
    window_size_seconds      INTEGER NOT NULL DEFAULT 60,
    allowed_lateness_seconds INTEGER NOT NULL DEFAULT 30,
    watermark                REAL,          -- 已见最大 event_time（epoch 秒），持久化水位线
    label_version            INTEGER NOT NULL DEFAULT 0,  -- 标签集版本：每次成功 PUT +1
    labels_updated_at        TEXT,          -- 最近一次标签写入时间（从未写入为 NULL）
    created_at               TEXT NOT NULL
);

-- 设备当前标签（仅保存最新版本的键值；旧版本随版本号被覆盖替换，不保留历史）
CREATE TABLE IF NOT EXISTS device_labels (
    device_id   TEXT NOT NULL,
    label_key   TEXT NOT NULL,               -- 规范化后的键名（去首尾空白、小写）
    label_value TEXT NOT NULL,               -- 去首尾空白后的值
    updated_at  TEXT NOT NULL,
    PRIMARY KEY (device_id, label_key),      -- 规范化后重复键在同事务替换，不会落两行
    FOREIGN KEY (device_id) REFERENCES devices (device_id)
);

CREATE INDEX IF NOT EXISTS idx_device_labels_key_value
    ON device_labels (label_key, label_value);

CREATE TABLE IF NOT EXISTS events (
    device_id   TEXT NOT NULL,
    event_id    TEXT NOT NULL,
    event_time  REAL NOT NULL,              -- epoch 秒
    value       REAL NOT NULL,
    status      TEXT NOT NULL,              -- accepted | late
    ingested_at TEXT NOT NULL,
    PRIMARY KEY (device_id, event_id)       -- 幂等去重约束
);

CREATE TABLE IF NOT EXISTS windows (
    device_id         TEXT NOT NULL,
    window_start      REAL NOT NULL,
    window_end        REAL NOT NULL,
    count             INTEGER NOT NULL DEFAULT 0,
    sum               REAL NOT NULL DEFAULT 0,
    min               REAL,
    max               REAL,
    status            TEXT NOT NULL DEFAULT 'open',   -- open | sealed
    correction_count  INTEGER NOT NULL DEFAULT 0,
    late_event_count  INTEGER NOT NULL DEFAULT 0,
    sealed_at         TEXT,
    PRIMARY KEY (device_id, window_start)
);

CREATE TABLE IF NOT EXISTS corrections (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id    TEXT NOT NULL,
    event_id     TEXT NOT NULL,
    window_start REAL NOT NULL,
    event_time   REAL NOT NULL,
    value        REAL NOT NULL,
    created_at   TEXT NOT NULL
);

-- 设备组：固定成员，成员设备的窗口配置（大小/允许迟到）必须一致
CREATE TABLE IF NOT EXISTS device_groups (
    group_id                 TEXT PRIMARY KEY,
    window_size_seconds      INTEGER NOT NULL,
    allowed_lateness_seconds INTEGER NOT NULL,
    aggregation              TEXT NOT NULL CHECK (aggregation IN ('sum', 'avg')),
    created_at               TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS device_group_members (
    group_id  TEXT NOT NULL,
    device_id TEXT NOT NULL,
    position  INTEGER NOT NULL,              -- 保留创建时的成员顺序
    PRIMARY KEY (group_id, device_id)
);

CREATE INDEX IF NOT EXISTS idx_group_members_device
    ON device_group_members (device_id);

-- 组同步窗口汇总：pending（有成员窗口未封存）→ ready（全部封存）
CREATE TABLE IF NOT EXISTS group_windows (
    group_id            TEXT NOT NULL,
    window_start        REAL NOT NULL,
    window_end          REAL NOT NULL,
    status              TEXT NOT NULL DEFAULT 'pending',  -- pending | ready
    value               REAL,                              -- pending 时为 NULL，禁止部分成员提前计算
    member_count        INTEGER NOT NULL,                 -- 固定成员总数
    sealed_member_count INTEGER NOT NULL DEFAULT 0,
    revision            INTEGER NOT NULL DEFAULT 0,       -- ready 后从 1 起，每次成员修正原位 +1
    correction_sig      INTEGER NOT NULL DEFAULT 0,       -- Σ 成员窗口 correction_count，用于判定是否升版
    ready_at            TEXT,
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL,
    PRIMARY KEY (group_id, window_start)
);

-- 组窗口修订审计：组窗口首次 ready、以及迟到修正令 revision 升版时，
-- 在同一事务追加一份不可变快照（append-only），后续修正只能追加新版本、不得覆盖旧快照。
CREATE TABLE IF NOT EXISTS group_window_snapshots (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    group_id     TEXT NOT NULL,
    window_start REAL NOT NULL,
    revision     INTEGER NOT NULL,                -- 对应 group_windows.revision
    value        REAL NOT NULL,                   -- 该修订当时的组汇总值
    kind         TEXT NOT NULL DEFAULT 'revision'
                 CHECK (kind IN ('revision', 'baseline')),  -- baseline=旧库启动时按当前版本补的基线
    created_at   TEXT NOT NULL,
    UNIQUE (group_id, window_start, revision)     -- 重复上报不得重复留档
);

CREATE TABLE IF NOT EXISTS group_window_snapshot_members (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_id      INTEGER NOT NULL,
    group_id         TEXT NOT NULL,
    device_id        TEXT NOT NULL,
    position         INTEGER NOT NULL,            -- 固定成员的创建顺序
    count            INTEGER NOT NULL,            -- 成员窗口当时的 count
    sum              REAL NOT NULL,               -- 成员窗口当时的 sum
    avg              REAL,                        -- 成员窗口当时的 avg（sum/count）
    correction_count INTEGER NOT NULL,            -- 成员窗口当时的 correction_count
    UNIQUE (snapshot_id, device_id)
);

CREATE INDEX IF NOT EXISTS idx_gws_members_snapshot
    ON group_window_snapshot_members (snapshot_id, position);

-- 审计表只允许 INSERT：任何 UPDATE/DELETE 直接中止，保证历史快照不可变。
CREATE TRIGGER IF NOT EXISTS trg_gws_no_update
BEFORE UPDATE ON group_window_snapshots
BEGIN
    SELECT RAISE(ABORT, 'group_window_snapshots is append-only');
END;

CREATE TRIGGER IF NOT EXISTS trg_gws_no_delete
BEFORE DELETE ON group_window_snapshots
BEGIN
    SELECT RAISE(ABORT, 'group_window_snapshots is append-only');
END;

CREATE TRIGGER IF NOT EXISTS trg_gwsm_no_update
BEFORE UPDATE ON group_window_snapshot_members
BEGIN
    SELECT RAISE(ABORT, 'group_window_snapshot_members is append-only');
END;

CREATE TRIGGER IF NOT EXISTS trg_gwsm_no_delete
BEFORE DELETE ON group_window_snapshot_members
BEGIN
    SELECT RAISE(ABORT, 'group_window_snapshot_members is append-only');
END;

-- 设备级高值告警规则：每设备至多一条。trigger_threshold 为触发线（高值），
-- recovery_threshold 为较低的恢复线；合法性（有限数且 recovery < trigger）在应用层校验，
-- 非法请求直接 400、不开启写事务，旧规则绝不会被覆盖。
CREATE TABLE IF NOT EXISTS device_alert_rules (
    device_id          TEXT PRIMARY KEY,
    trigger_threshold  REAL NOT NULL,
    recovery_threshold REAL NOT NULL,
    created_at         TEXT NOT NULL,
    updated_at         TEXT NOT NULL,
    FOREIGN KEY (device_id) REFERENCES devices (device_id)
);

-- 设备告警状态机当前状态（持久化）：not_alerting -> alerting -> not_alerting ……
-- 仅在首次 value >= trigger_threshold 时进入告警（alarm_count +1），
-- 仅在 value <= recovery_threshold 时解除（recovery_count +1），
-- 处于两线之间的中间区间时保持原状态，避免阈值附近反复抖动。
CREATE TABLE IF NOT EXISTS device_alert_states (
    device_id      TEXT PRIMARY KEY,
    alerting       INTEGER NOT NULL DEFAULT 0,   -- 1=告警中，0=正常
    alarm_count    INTEGER NOT NULL DEFAULT 0,  -- 累计进入告警次数（每次首次越线 +1）
    recovery_count INTEGER NOT NULL DEFAULT 0,  -- 累计解除告警次数
    last_value     REAL,                        -- 最近一个驱动过状态机的事件值
    last_event_id  TEXT,
    last_event_time REAL,                        -- 最近事件的 event_time（epoch 秒）
    last_transition TEXT,                        -- entered | cleared；尚无状态变化为 NULL
    entered_at     TEXT,                         -- 最近一次进入告警的处理时间
    cleared_at     TEXT,                         -- 最近一次解除告警的处理时间
    updated_at     TEXT,
    FOREIGN KEY (device_id) REFERENCES devices (device_id)
);

-- 状态转换流水：只追加。(device_id, event_id) 唯一约束兜底：
-- 重试批次中的事件在 ingest 幂等去重阶段即被跳过，即使绕过也无法重复产生状态变化。
CREATE TABLE IF NOT EXISTS device_alert_transitions (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id  TEXT NOT NULL,
    event_id   TEXT NOT NULL,
    event_time REAL NOT NULL,
    value      REAL NOT NULL,
    kind       TEXT NOT NULL CHECK (kind IN ('entered', 'cleared')),
    created_at TEXT NOT NULL,
    UNIQUE (device_id, event_id)
);

CREATE INDEX IF NOT EXISTS idx_alert_transitions_device
    ON device_alert_transitions (device_id, id);

-- 设备计划停报（已批准停机）时段：供运维区分"已批准停机"与"异常缺报"。
-- 时段为半开区间 [start_time, end_time)，起止在应用层按设备窗口大小对齐，
-- CHECK 兜底 end > start；允许重叠登记多条，查询缺报时先按严格重叠（端点触碰不算）
-- 合并再扣除，重叠计划不会被重复扣除。与其他业务数据同库存放，随 /data 卷持久化。
CREATE TABLE IF NOT EXISTS planned_outages (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id  TEXT NOT NULL,
    start_time REAL NOT NULL,              -- epoch 秒，对齐设备窗口网格
    end_time   REAL NOT NULL,              -- epoch 秒，对齐设备窗口网格，严格大于 start_time
    reason     TEXT,                       -- 可选报备说明（去首尾空白；纯空白存 NULL）
    created_at TEXT NOT NULL,
    CHECK (end_time > start_time),
    FOREIGN KEY (device_id) REFERENCES devices (device_id)
);

CREATE INDEX IF NOT EXISTS idx_planned_outages_device_time
    ON planned_outages (device_id, start_time, end_time);
"""


def db_path() -> str:
    return os.environ.get("TELEMETRY_DB_PATH", "telemetry.db")


def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(db_path(), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db() -> None:
    with connect() as conn:
        conn.executescript(SCHEMA)
        _migrate(conn)


def _migrate(conn: sqlite3.Connection) -> None:
    """旧库就地迁移（幂等）：为 devices 表补标签版本列。

    CREATE TABLE IF NOT EXISTS 不会给已存在的旧表加列，这里按 PRAGMA 检测结果补。
    """
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(devices)").fetchall()}
    if "label_version" not in cols:
        conn.execute(
            "ALTER TABLE devices ADD COLUMN label_version INTEGER NOT NULL DEFAULT 0"
        )
    if "labels_updated_at" not in cols:
        conn.execute("ALTER TABLE devices ADD COLUMN labels_updated_at TEXT")


@contextmanager
def session() -> Iterator[sqlite3.Connection]:
    """事务会话：正常退出提交，异常回滚，始终关闭连接。"""
    conn = connect()
    try:
        with conn:
            yield conn
    finally:
        conn.close()


@contextmanager
def rollback_session() -> Iterator[sqlite3.Connection]:
    """预演（dry-run）会话：进入即 BEGIN IMMEDIATE，退出时**一律 ROLLBACK**。

    与 immediate_session 一样立即加写锁：预演期间读取的快照不会被其他写事务改变，
    逐事件推进的去重/水位线/封存判断与随即在无并发写入时真正提交完全一致；
    预演结束（或处理中抛异常）后所有写入随回滚撤销，设备、事件、窗口、修正、
    告警状态/流水及组汇总均不留痕。
    """
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
        finally:
            conn.rollback()
    finally:
        conn.close()


@contextmanager
def immediate_session() -> Iterator[sqlite3.Connection]:
    """立即加写锁的事务会话：进入即 BEGIN IMMEDIATE，正常退出提交，异常回滚。

    用于"读当前版本 → 校验 → 写入"的乐观并发场景：写锁保证同一时刻只有一个
    写事务，读到的版本在事务内不会被其他写者改变。与 Python 3.12 之前的
    sqlite3 隐式 BEGIN 行为兼容（这里显式 BEGIN，不依赖 autocommit 语义）。
    """
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
        except BaseException:
            conn.rollback()
            raise
        else:
            conn.commit()
    finally:
        conn.close()
