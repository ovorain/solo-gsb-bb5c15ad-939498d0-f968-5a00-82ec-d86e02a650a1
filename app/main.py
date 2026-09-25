"""传感器遥测汇聚 API。

核心语义：
- 每个设备维护一条持久化的事件时间水位线 watermark = 已见最大 event_time。
- 时间窗为按 window_size_seconds 对齐 epoch 的滚动窗口。
- 当 watermark >= window_end + allowed_lateness_seconds 时，窗口封存（sealed）。
- 重复 (device_id, event_id) 幂等去重，不影响聚合结果。
- 窗口封存后到达的事件标记为 late，仍计入聚合，同时生成一条修正记录，
  窗口的 correction_count 随之递增。

提交预演（dry-run，只读语义）：
- POST /ingest/preview 接收与 POST /ingest 完全相同的设备与事件批次，在同一数据库快照
  （BEGIN IMMEDIATE 写锁事务）上按请求顺序推进去重、水位线、封存、迟到修正、告警状态机
  与组汇总对账，返回逐事件预计 accepted/duplicate/late、预计水位线，以及本批次触及窗口
  （至少落入一个非重复事件）预演后的 count/sum/status/correction_count。
- 批内重复 event_id 只计一次；前一事件抬高水位线后，后一旧事件可能因此变成 late；
  结果口径与随即在无并发写入时真正提交同一批次一致。预演与正式提交共用同一份批次处理
  逻辑（_run_ingest_batch），避免两套判断漂移。
- 整个预演事务结束时一律 ROLLBACK：设备（含不存在设备的自动注册）、事件、窗口、修正、
  告警状态/转换流水、组汇总及不可变审计快照均不留痕。

设备可版本化标签：
- PUT /devices/{device_id}/labels 以**全量替换**语义写入不超过 20 个 key/value；
  键名在同一 SQLite 写事务内规范化（去首尾空白、小写、^[a-z0-9][a-z0-9_.-]*$），
  值只去首尾空白；规范化后重复键或非法键名返回 400 且不改动数据。
- 每次成功写入都把 devices.label_version 加一（单调递增，从 1 起），旧标签同事务删除。
- GET /devices/{device_id}/labels 返回当前标签与版本（从未写入时版本 0、标签为空）。
- GET /devices?label_key=&label_value= 按**当前**标签筛选设备（只给 label_value
  不给 label_key 返回 422）。
- PUT 请求带 if_version 时做乐观并发校验：必须等于服务端当前版本，
  不匹配返回 409 且不写入任何数据（写锁事务保证读版本与写入的一致性）。

设备组同步窗口汇总：
- 设备组为固定成员组，创建时所有成员设备必须存在且窗口配置一致
  （window_size_seconds / allowed_lateness_seconds），聚合方式为 sum 或 avg。
- 入组期间成员配置被锁定：PUT /devices/{id}/config 对已入组设备提交与任一所属组
  不一致的窗口大小或允许迟到秒数时一律返回 409 且不写入（成员尚无窗口也不得改窗口
  大小，允许迟到秒数也不得随时改），避免同组窗口时间范围或封存时点错开导致组汇总
  失真；提交与所属组完全相同的配置是幂等重复提交，返回 200。未入组设备沿用既有规则
  （已有窗口数据后不得改窗口大小，允许迟到秒数可随时调整）。
- 旧库组成员纠偏：提交值与所有所属组一致时，写入前在同一写锁事务内核对该设备
  已有窗口的起点对齐与实际跨度；任一窗口与目标 window_size_seconds 不相容
  （漂移期间落盘的异网格窗口）即返回 409，设备配置、窗口与组汇总保持原样；
  无冲突时允许纠正仅配置字段的历史漂移。
- 组创建的成员校验/落盘与设备配置更新都在 BEGIN IMMEDIATE 写锁事务内完成、彼此
  串行，避免并发交错留下"已入组但配置漂移"的成员。
- 同一 window_start 下仅当全部成员的设备窗口均已封存（sealed）时，
  才生成组汇总（ready）；任一成员窗口未封存（或尚无窗口）时保持 pending，
  value 为 NULL，绝不用部分成员提前计算。
- 成员窗口因迟到事件发生修正后，已生成的组汇总原位重算且 revision 加一；
- 修正以成员窗口 correction_count 之和为签名：同一 event_id 重试是幂等去重，
  不会产生新修正，签名不变，revision 不重复升版。
- 写入路径的组对账为增量口径：每次 ingest 只对账本批实际新增或迟到修正触及的窗口，
  以及水位线推进而刚封存的窗口；同组同窗只处理一次，其余无变化的历史窗口不读取、
  不参与本次写入对账（历史增长后单个新事件或幂等重试不再全量重扫旧窗口）。
  纯重复批次（全部事件去重跳过、水位线未变）不产生任何组窗口读写。
  组创建时保留全量初始化：遍历成员全部历史窗口一次性补算 pending/ready。

组窗口修订审计（append-only 快照）：
- 组窗口首次 ready、以及迟到修正令 revision 升版时，在**同一事务**追加一份不可变快照，
  记录当时的 group value 与每个固定成员窗口的 count、sum、avg、correction_count。
- (group_id, window_start, revision) 唯一约束保证重复上报不会重复留档；
  审计表带禁止 UPDATE/DELETE 的触发器，后续修正只能追加新版本、旧快照永不被覆盖。
- 服务初始化时只对旧库中**已 ready** 的组窗口补一份**当前 revision** 的 baseline，
  绝不伪造更早的历史版本。
- 提供修订列表与两版本 diff 接口；diff 按成员顺序给出前后贡献与数值差，仅列发生变化的成员，
  非法版本返回 422、不存在的组/窗口/修订返回 404。

封存窗口稳健异常检测（只读）：
- GET /devices/{device_id}/anomalies?window_start=&threshold= 仅对**已封存**窗口内事件
  （含落入该窗口的迟到事件）做基于中位数与 MAD（median absolute deviation）的稳健打分：
  score = 0.6745 * |x - median| / MAD，只返回 score > threshold 的事件，threshold 默认 3.5 且必须 > 0。
- MAD 为 0（超过半数事件等于中位数）时，除以 0 无定义：按需求把所有**不等于中位数**的事件
  一律判为异常返回，其 score 记为 null；全部等于中位数则无异常。
- 设备或窗口不存在返回 404，窗口尚未封存（open）返回 409；接口纯只读，不改变任何现有聚合。

跨设备窗口检索（稳定游标分页，只读）：
- GET /windows/search 跨设备检索窗口，可按窗口时间范围（对 window_start 取半开区间 [start, end)）、
  open/sealed 状态及设备**当前**标签（label_key/label_value，语义与 GET /devices 一致）精确筛选，
  按 window_start、device_id 升序返回窗口聚合与设备标识。
- 分页采用与筛选条件绑定的不透明游标：游标内嵌 (window_start, device_id) 键集位置、
  首次查询时的行快照上界（windows 表 MAX(rowid)）与规范化后的全部筛选条件，
  并以 HMAC-SHA256 签名防篡改、默认 1 小时过期。翻页期间**新增**的窗口行 rowid 超出快照上界，
  不会进入本次遍历，因此新增数据既不会在翻页中重复出现也不会挤掉/插入造成遗漏
  （不带游标重新发起检索即可看到新增数据）。
- 游标签名非法、载荷损坏、已过期，或与本次请求的筛选条件不一致，一律返回 400。

设备级高值告警（双阈值滞回状态机）：
- PUT /devices/{device_id}/alert-rule 为设备设置触发阈值 trigger_threshold 与较低的恢复阈值
  recovery_threshold；阈值必须是有限数且 recovery < trigger，任一不合法返回 400 且**不开启写事务、
  绝不覆盖旧规则**；设备不存在返回 404。
- GET /devices/{device_id}/alert 查询当前告警状态；设备无规则返回 404（无规则即没有状态机）。
- 事件写入（含迟到事件）时在与聚合相同的 ingest 事务内逐事件驱动状态转换：
  value >= 触发线且当前未告警 → 进入告警（首次越线才触发）；
  value <= 恢复线且当前告警中 → 解除告警；
  恢复线 < value < 触发线的中间区间保持原状态（滞回，避免阈值附近反复抖动）。
- 状态与规则持久化在 SQLite 中，重启不丢失；每次转换追加只增流水，
  (device_id, event_id) 唯一约束兜底——批次重试的重复事件在幂等去重阶段即跳过，不会重复产生状态变化。

设备告警周期查询（只读）：
- GET /devices/{device_id}/alert-episodes?start=&end= 供值班人员查看指定 UTC 时间段
  （半开区间 [start, end)）内的告警周期与总告警秒数；start/end 必填且 end > start，
  否则返回 422；设备或告警规则不存在返回 404，无相交周期返回空明细与 0。
- 按告警转换流水的自增顺序把 entered、cleared 配对成周期，周期起止时刻取流水
  记录的处理时刻 created_at；迟到事件只按处理顺序追加流水，不按 event_time 重排。
- 只返回与 [start, end) 相交的周期，逐条给出进入/解除时刻与裁剪后的区间内秒数；
  尚未解除的周期只计至 end 与查询时刻的较早者；total_seconds 为各明细秒数之和。

设备缺报区间查询（只读）：
- GET /devices/{device_id}/missing-windows?start=&end= 查询半开区间 [start, end) 内的连续缺报段，
  返回每段的起止时间与缺失窗口数；start/end 必须对齐设备窗口大小，end <= start 或未对齐返回 422，
  设备不存在返回 404，设备尚无数据返回空列表。
- 一个对齐窗口位置计入缺报需同时满足：不早于设备最早有数据窗口的下一个窗口、
  水位线已越过其封存条件（watermark >= window_end + allowed_lateness，未封存窗口绝不报缺）、
  且 windows 表中没有该位置的窗口记录。
- 按现存窗口之间的空隙计算（空隙宽度 / 窗口大小 = 缺失窗口数），不逐窗遍历跨度；
  迟到事件首次填入空窗后新增窗口记录，后续查询自动缩短或拆分缺报段，不追改已有聚合。
- exclude_planned=true 时先按上述规则求出原始缺报段，再把查询范围内**重叠**的计划停报
  时段（端点触碰不算重叠）合并后从缺报段中扣除，返回剩余连续缺报段；重叠计划不会被
  重复扣除，全部区间运算按段算术完成、不逐窗遍历。

设备计划停报（已批准停机报备）：
- POST /devices/{device_id}/planned-outages 按设备登记半开停报区间 [start, end)，
  start/end 必须对齐该设备窗口大小且 end > start（未对齐/倒置返回 422，设备不存在 404），
  可携带可选 reason；允许重叠登记多条，登记即持久化在 SQLite 中。
- GET /devices/{device_id}/planned-outages 列出该设备全部计划停报时段，按 start 升序。
- GET /devices/{device_id}/missing-windows 增加 exclude_planned 参数（默认 false）：
  为 true 时先按原有水位线、现存窗口与迟到补报规则求缺报，再把与查询范围严格重叠
  （端点触碰不算）的计划区间合并并扣除，计划只可能让缺报段缩短或拆分，绝不产生新缺报；
  合并保证互相重叠的多条计划在扣除时并集只扣一次，不重复扣除。

设备每日同窗基线偏移查询（只读）：
- GET /devices/{device_id}/daily-baseline?window_start=&days=&threshold= 以目标窗口为基准，
  按 UTC 每天同一窗位（window_start - k*86400, k=1..days）回看历史窗口，仅当对应窗口
  存在且已封存（sealed，含迟到修正后的最新聚合）时计为有效样本；缺失或未封存位置一律跳过，
  绝不用临近窗口代替。
- 仅在目标窗口已封存、且窗口秒数可整除一天（86400 % window_size_seconds == 0，保证逐日同格）
  时计算：窗口秒数不可整除返回 422，设备不存在返回 404，目标窗口不存在返回 404、未封存返回 409。
- 基线取有效窗口均值的中位数与 MAD（绝对偏差中位数，不做 0.6745 归一化），偏移分数
  score = |目标均值 - 中位数| / MAD，score > threshold 判越界；days 取 3..30、threshold 必须 > 0。
- 有效样本不足 3 个时返回 status=insufficient_samples（不计算分数、不判越界，breached=null）。
- MAD 为 0 时：目标均值等于中位数（同值偏移）记 score=0、breached=false；异值偏移除以 0 无定义，
  score 返回 null 并标记 breached=true。
- 迟到修正原位更新窗口聚合，再次查询立即反映最新的 count/sum/avg；接口纯只读。

设备持久化聚合完整性核查（只读）：
- GET /devices/{device_id}/integrity?start=&end= 供运维核查指定窗口范围内的持久化聚合：
  在单次 SQLite 只读事务内按事件时间把原始事件归窗（与写入路径同一归窗口径），
  重算 count/sum/min/max/late_event_count，与 windows 表逐窗对账；
  有事件但缺少窗口行、或存在无事件的窗口行，同样列为差异。
- 差异项按 window_start 升序返回，每项给出期望值（原始事件重算）与实际值（windows 表，
  缺行为 null）及不一致字段；浮点聚合按相对或绝对误差 1e-9 判定一致。
- 范围边界必须对齐设备窗口大小且单次最多覆盖 1000 个窗口；设备不存在返回 404，
  范围非法（倒置、未对齐、超上限）返回 422；核查纯只读，不改动水位线、修正记录与组汇总。
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import math
import os
import re
import sqlite3
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Literal, Optional

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

from . import db

DEFAULT_WINDOW_SIZE_SECONDS = 60
DEFAULT_ALLOWED_LATENESS_SECONDS = 30
MAX_GROUP_MEMBERS = 500
MAX_DEVICE_LABELS = 20
MAX_LABEL_KEY_LENGTH = 63
MAX_LABEL_VALUE_LENGTH = 256
# 跨设备窗口检索：分页大小与游标有效期（秒）；游标在有效期内可稳定翻完首次检索的快照
SEARCH_DEFAULT_LIMIT = 100
SEARCH_MAX_LIMIT = 1000
SEARCH_CURSOR_TTL_SECONDS = 3600
# 规范化后的合法键名：小写字母/数字开头，其余允许下划线、点、连字符
LABEL_KEY_RE = re.compile(r"^[a-z0-9][a-z0-9_.-]*$")


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_db()
    backfill_baseline_snapshots()
    yield


app = FastAPI(title="Sensor Telemetry Aggregation API", version="1.12.0", lifespan=lifespan)

# 稳健异常检测：基于中位数/MAD 的修正 z 分数，0.6745 为正态分布下 MAD ≈ 0.6745σ 的换算系数
DEFAULT_ANOMALY_THRESHOLD = 3.5
MAD_NORMALIZER = 0.6745
# 每日同窗基线偏移：UTC 一天秒数、回看天数范围与最少有效样本数
SECONDS_PER_DAY = 86400
MIN_BASELINE_DAYS = 3
MAX_BASELINE_DAYS = 30
MIN_BASELINE_SAMPLES = 3
# 持久化聚合完整性核查：单次核查最多覆盖的窗口数；浮点聚合对账容差（相对或绝对误差）
INTEGRITY_MAX_WINDOWS = 1000
INTEGRITY_FLOAT_TOLERANCE = 1e-9


# ---------- Pydantic 模型 ----------

class EventIn(BaseModel):
    event_id: str = Field(min_length=1, max_length=128)
    event_time: datetime  # ISO 8601；不带时区时按 UTC 处理
    value: float


class IngestRequest(BaseModel):
    device_id: str = Field(min_length=1, max_length=128)
    events: list[EventIn] = Field(min_length=1, max_length=10000)


class EventResult(BaseModel):
    event_id: str
    status: Literal["accepted", "duplicate", "late"]
    window_start: Optional[datetime] = None


class IngestResponse(BaseModel):
    device_id: str
    watermark: Optional[datetime]
    accepted: int
    duplicates: int
    late: int
    results: list[EventResult]


class PreviewWindowOut(BaseModel):
    # 预演后本批次触及窗口（至少落入一个非重复事件）的状态
    window_start: datetime
    window_end: datetime
    status: Literal["open", "sealed"]
    count: int
    sum: float
    correction_count: int


class IngestPreviewResponse(BaseModel):
    device_id: str
    # 预演推进后的预计水位线（设备从无数据、且批次全为重复时保持 null）
    watermark: Optional[datetime]
    accepted: int
    duplicates: int
    late: int
    # 逐事件预计结果，顺序与请求一致；判定随批次内顺序推进（前一事件抬高
    # 水位线后，后一旧事件可能因此成为 late；批内重复 event_id 只计一次）
    results: list[EventResult]
    # 本批次触及窗口在预演后的 count/sum/status/correction_count，按 window_start 升序
    windows: list[PreviewWindowOut]


class DeviceConfig(BaseModel):
    window_size_seconds: int = Field(default=DEFAULT_WINDOW_SIZE_SECONDS, gt=0, le=86400)
    allowed_lateness_seconds: int = Field(default=DEFAULT_ALLOWED_LATENESS_SECONDS, ge=0, le=86400)


class DeviceOut(BaseModel):
    device_id: str
    window_size_seconds: int
    allowed_lateness_seconds: int
    watermark: Optional[datetime]
    event_count: int
    open_windows: int
    sealed_windows: int
    created_at: datetime


class LabelItem(BaseModel):
    key: str = Field(min_length=1, max_length=256)
    value: str = Field(min_length=0, max_length=256)


class LabelUpdateRequest(BaseModel):
    labels: list[LabelItem] = Field(min_length=0, max_length=MAX_DEVICE_LABELS)
    # 乐观并发：客户端基于上次读到的版本提交；缺省表示不校验
    if_version: Optional[int] = Field(default=None, ge=0)


class LabelsOut(BaseModel):
    device_id: str
    version: int
    labels: dict[str, str]
    updated_at: Optional[datetime] = None


class WindowOut(BaseModel):
    device_id: str
    window_start: datetime
    window_end: datetime
    status: Literal["open", "sealed"]
    count: int
    sum: float
    avg: Optional[float]
    min: Optional[float]
    max: Optional[float]
    correction_count: int
    late_event_count: int
    sealed_at: Optional[datetime]


class WindowSearchResponse(BaseModel):
    # 窗口聚合与设备标识，按 window_start、device_id 升序
    windows: list[WindowOut]
    # 下一页游标（不透明，仅与本次筛选条件绑定）；没有更多数据时为 null
    next_cursor: Optional[str] = None


class CorrectionOut(BaseModel):
    id: int
    device_id: str
    event_id: str
    window_start: datetime
    event_time: datetime
    value: float
    created_at: datetime


class MissingWindowSegmentOut(BaseModel):
    # 一段连续缺报区间：半开 [start, end)，start 为首个缺失窗口起点，end 为末个缺失窗口终点
    start: datetime
    end: datetime
    missing_windows: int


class PlannedOutageIn(BaseModel):
    # 半开停报区间 [start, end)：起止必须对齐设备窗口大小且 end > start
    start: datetime
    end: datetime
    reason: Optional[str] = Field(default=None, max_length=500)


class PlannedOutageOut(BaseModel):
    id: int
    device_id: str
    start: datetime
    end: datetime
    reason: Optional[str] = None
    created_at: datetime


class AnomalyOut(BaseModel):
    event_id: str
    event_time: datetime
    value: float
    # 修正 z 分数；MAD 为 0 时分数无定义（除以 0），非中位值一律判为异常，score 返回 null
    score: Optional[float] = None


class DailyBaselineSampleOut(BaseModel):
    # 一个有效回看样本：目标窗位往前第 offset_days 天的同窗已封存窗口均值
    window_start: datetime
    window_end: datetime
    offset_days: int
    avg: float


class DailyBaselineOut(BaseModel):
    device_id: str
    window_start: datetime
    window_end: datetime
    days: int
    # ok：样本充足且完成打分；insufficient_samples：有效样本不足 3 个（不判越界）
    status: Literal["ok", "insufficient_samples"]
    sample_count: int
    samples: list[DailyBaselineSampleOut]  # 按 window_start 升序（仅含有效样本）
    target_avg: Optional[float] = None      # 目标窗口均值（窗口至少含 1 个事件）
    # 基线：有效窗口均值的中位数与 MAD（样本不足时为 null）
    baseline_median: Optional[float] = None
    baseline_mad: Optional[float] = None
    # |目标均值 - 中位数|；样本不足时为 null
    deviation: Optional[float] = None
    # 偏移分数 = deviation / MAD；MAD=0 且异值偏移时除以 0 无定义，返回 null
    score: Optional[float] = None
    threshold: float
    # 越界判定；样本不足（无法判定）时为 null
    breached: Optional[bool] = None


class IntegrityAggregateOut(BaseModel):
    # 一个窗口的五项对账聚合：count/sum/min/max/late_event_count
    count: int
    sum: float
    min: Optional[float] = None
    max: Optional[float] = None
    late_event_count: int


class IntegrityDiscrepancyOut(BaseModel):
    # 一处窗口对账差异：期望值为原始事件重算结果，实际值为 windows 表记录（缺行时为 null）
    window_start: datetime
    window_end: datetime
    # missing_window_row=有事件但缺少窗口行；unexpected_window_row=存在无事件的窗口行；
    # aggregate_mismatch=两侧都有记录但聚合不一致
    kind: Literal["missing_window_row", "unexpected_window_row", "aggregate_mismatch"]
    expected: IntegrityAggregateOut
    actual: Optional[IntegrityAggregateOut] = None
    mismatched_fields: list[str]


class IntegrityReportOut(BaseModel):
    device_id: str
    start: datetime
    end: datetime
    window_size_seconds: int
    windows_checked: int
    discrepancy_count: int
    discrepancies: list[IntegrityDiscrepancyOut]  # 按 window_start 升序


class GroupCreate(BaseModel):
    group_id: str = Field(min_length=1, max_length=128)
    member_ids: list[str] = Field(min_length=1, max_length=MAX_GROUP_MEMBERS)
    aggregation: Literal["sum", "avg"]
    window_size_seconds: int = Field(gt=0, le=86400)
    allowed_lateness_seconds: int = Field(ge=0, le=86400)


class GroupOut(BaseModel):
    group_id: str
    member_ids: list[str]
    aggregation: Literal["sum", "avg"]
    window_size_seconds: int
    allowed_lateness_seconds: int
    created_at: datetime


class GroupWindowOut(BaseModel):
    group_id: str
    window_start: datetime
    window_end: datetime
    status: Literal["pending", "ready"]
    value: Optional[float] = None
    member_count: int
    sealed_member_count: int
    revision: int
    ready_at: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime


class PendingMemberOut(BaseModel):
    device_id: str
    window_start: Optional[datetime] = None
    window_end: datetime
    window_status: Optional[Literal["open", "sealed"]] = None  # None = 成员尚无该窗口
    reason: Literal["missing", "open"]


class GroupWindowRevisionOut(BaseModel):
    revision: int
    value: float
    kind: Literal["revision", "baseline"]
    created_at: datetime
    member_count: int


class RevisionMemberStateOut(BaseModel):
    device_id: str
    position: int
    count: int
    sum: float
    avg: Optional[float] = None
    correction_count: int
    contribution: float  # 该成员当时对 group value 的贡献：sum 聚合为 sum，avg 聚合为窗口 avg


class RevisionMemberDeltaOut(BaseModel):
    device_id: str
    position: int
    before: RevisionMemberStateOut
    after: RevisionMemberStateOut
    count_delta: int
    sum_delta: float
    avg_delta: Optional[float] = None
    correction_count_delta: int
    contribution_delta: float


class RevisionDiffOut(BaseModel):
    group_id: str
    window_start: datetime
    aggregation: Literal["sum", "avg"]
    from_revision: int
    to_revision: int
    from_value: float
    to_value: float
    value_delta: float
    members: list[RevisionMemberDeltaOut]  # 仅含发生变化的成员，按成员顺序排列


class AlertRuleIn(BaseModel):
    trigger_threshold: float   # 进入告警的高值触发线
    recovery_threshold: float  # 解除告警的恢复线，必须严格低于触发线


class AlertRuleOut(BaseModel):
    device_id: str
    trigger_threshold: float
    recovery_threshold: float
    created_at: datetime
    updated_at: datetime


class AlertOut(BaseModel):
    device_id: str
    rule: AlertRuleOut
    # 是否正处于告警中（首次越过触发线后置 true，降至恢复线后回到 false）
    alerting: bool
    alarm_count: int
    recovery_count: int
    last_value: Optional[float] = None
    last_event_id: Optional[str] = None
    last_event_time: Optional[datetime] = None
    # 最近一次状态转换：entered（进入告警）/ cleared（解除）；规则下尚无任何事件时为 null
    last_transition: Optional[Literal["entered", "cleared"]] = None
    entered_at: Optional[datetime] = None
    cleared_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class AlertEpisodeOut(BaseModel):
    # 一个告警周期：按转换流水自增顺序配对的 entered/cleared 的处理时刻（created_at），
    # 以及裁剪到查询区间 [start, end) 后的区间内秒数；尚未解除时 cleared_at 为 null
    entered_at: datetime
    cleared_at: Optional[datetime] = None
    seconds: float


class AlertEpisodesOut(BaseModel):
    device_id: str
    start: datetime
    end: datetime
    episodes: list[AlertEpisodeOut]
    # 区间内总告警秒数 = 各明细 seconds 之和
    total_seconds: float


# ---------- 工具函数 ----------

def to_epoch(dt: datetime) -> float:
    """datetime -> epoch 秒；naive 时间按 UTC 处理。"""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def to_iso(ts: Optional[float]) -> Optional[str]:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def window_floor(t: float, size: int) -> float:
    return math.floor(t / size) * size


def get_device(conn: sqlite3.Connection, device_id: str) -> Optional[sqlite3.Row]:
    return conn.execute("SELECT * FROM devices WHERE device_id = ?", (device_id,)).fetchone()


def get_or_create_device(conn: sqlite3.Connection, device_id: str, now: str) -> sqlite3.Row:
    row = get_device(conn, device_id)
    if row is None:
        conn.execute(
            "INSERT INTO devices (device_id, window_size_seconds, allowed_lateness_seconds, watermark, created_at)"
            " VALUES (?, ?, ?, NULL, ?)",
            (device_id, DEFAULT_WINDOW_SIZE_SECONDS, DEFAULT_ALLOWED_LATENESS_SECONDS, now),
        )
        row = get_device(conn, device_id)
    return row


def require_device(conn: sqlite3.Connection, device_id: str) -> sqlite3.Row:
    row = get_device(conn, device_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"device not found: {device_id}")
    return row


_DEVICE_STATS_SELECT = (
    "SELECT d.*,"
    " (SELECT COUNT(*) FROM events WHERE device_id = d.device_id) AS event_count,"
    " (SELECT COUNT(*) FROM windows WHERE device_id = d.device_id AND status = 'open') AS open_windows,"
    " (SELECT COUNT(*) FROM windows WHERE device_id = d.device_id AND status = 'sealed') AS sealed_windows"
    " FROM devices d"
)


def device_row_to_out(r: sqlite3.Row) -> DeviceOut:
    return DeviceOut(
        device_id=r["device_id"],
        window_size_seconds=r["window_size_seconds"],
        allowed_lateness_seconds=r["allowed_lateness_seconds"],
        watermark=to_iso(r["watermark"]),
        event_count=r["event_count"],
        open_windows=r["open_windows"],
        sealed_windows=r["sealed_windows"],
        created_at=r["created_at"],
    )


def device_out(conn: sqlite3.Connection, device_id: str) -> DeviceOut:
    row = conn.execute(
        _DEVICE_STATS_SELECT + " WHERE d.device_id = ?", (device_id,)
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"device not found: {device_id}")
    return device_row_to_out(row)


def normalize_label_key(raw: str) -> str:
    """键名规范化：去首尾空白并转小写；非法键名抛 400。"""
    key = raw.strip().lower()
    if len(key) > MAX_LABEL_KEY_LENGTH or not LABEL_KEY_RE.fullmatch(key):
        raise HTTPException(
            status_code=400,
            detail=(
                f"invalid label key: {raw!r}; after normalization it must match "
                f"{LABEL_KEY_RE.pattern} and be at most {MAX_LABEL_KEY_LENGTH} characters"
            ),
        )
    return key


def normalize_label_value(raw: str) -> str:
    """值规范化：仅去首尾空白；空值或超长值抛 400。"""
    value = raw.strip()
    if not value:
        raise HTTPException(status_code=400, detail="label value must not be empty")
    if len(value) > MAX_LABEL_VALUE_LENGTH:
        raise HTTPException(
            status_code=400,
            detail=f"label value must be at most {MAX_LABEL_VALUE_LENGTH} characters",
        )
    return value


def normalize_labels(items: list[LabelItem]) -> dict[str, str]:
    """规范化整批标签并拒绝规范化后的重复键；结果按键名排序。"""
    labels: dict[str, str] = {}
    for item in items:
        key = normalize_label_key(item.key)
        value = normalize_label_value(item.value)
        if key in labels:
            raise HTTPException(
                status_code=400,
                detail=f"duplicate label key after normalization: {key}",
            )
        labels[key] = value
    return dict(sorted(labels.items()))


def get_device_labels(conn: sqlite3.Connection, device_id: str) -> dict[str, str]:
    rows = conn.execute(
        "SELECT label_key, label_value FROM device_labels"
        " WHERE device_id = ? ORDER BY label_key",
        (device_id,),
    ).fetchall()
    return {r["label_key"]: r["label_value"] for r in rows}


def window_row_to_out(r: sqlite3.Row) -> WindowOut:
    return WindowOut(
        device_id=r["device_id"],
        window_start=to_iso(r["window_start"]),
        window_end=to_iso(r["window_end"]),
        status=r["status"],
        count=r["count"],
        sum=r["sum"],
        avg=r["sum"] / r["count"] if r["count"] else None,
        min=r["min"],
        max=r["max"],
        correction_count=r["correction_count"],
        late_event_count=r["late_event_count"],
        sealed_at=r["sealed_at"],
    )


# ---------- 跨设备窗口检索：游标编解码 ----------

# 进程级签名密钥：首次需要时惰性生成；服务重启后旧游标签名自然失效（按非法游标返回 400）。
_cursor_secret: Optional[bytes] = None


def _get_cursor_secret() -> bytes:
    global _cursor_secret
    if _cursor_secret is None:
        _cursor_secret = os.urandom(32)
    return _cursor_secret


def _b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _b64url_decode(text: str) -> bytes:
    padding = "=" * (-len(text) % 4)
    return base64.urlsafe_b64decode(text + padding)


def _sign(payload_b64: str) -> str:
    return hmac.new(
        _get_cursor_secret(), payload_b64.encode("ascii"), hashlib.sha256
    ).hexdigest()


def encode_search_cursor(
    window_start: float,
    device_id: str,
    snapshot_max_rowid: int,
    filters: dict[str, Any],
    issued_at: Optional[float] = None,
) -> str:
    """生成不透明游标：载荷为版本化 JSON（base64url），附 HMAC-SHA256 签名。"""
    payload = {
        "v": 1,
        "ws": window_start,
        "did": device_id,
        "snap": snapshot_max_rowid,
        "iss": issued_at if issued_at is not None else time.time(),
        "f": filters,
    }
    payload_b64 = _b64url_encode(
        json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    )
    return payload_b64 + "." + _sign(payload_b64)


def decode_search_cursor(cursor: str, filters: dict[str, Any]) -> dict[str, Any]:
    """校验并解码游标，返回载荷；任何非法情形（含过期、筛选条件不一致）抛 400。

    - 结构损坏/载荷不是合法 JSON/缺字段/签名不匹配 → 400（防篡改、防伪造）；
    - 超过 SEARCH_CURSOR_TTL_SECONDS → 400（过期游标必须重新发起检索）；
    - 游标绑定的筛选条件与当前请求不一致 → 400（游标只在同一组筛选下有效）。
    """
    invalid = HTTPException(status_code=400, detail="invalid cursor")
    try:
        payload_b64, _, sig = cursor.partition(".")
        if not payload_b64 or not sig or not hmac.compare_digest(sig, _sign(payload_b64)):
            raise invalid
        payload = json.loads(_b64url_decode(payload_b64).decode("utf-8"))
        if not isinstance(payload, dict):
            raise invalid
        if payload.get("v") != 1:
            raise invalid
        window_start = float(payload["ws"])
        device_id = payload["did"]
        snapshot_max_rowid = int(payload["snap"])
        issued_at = float(payload["iss"])
    except (ValueError, TypeError, KeyError, UnicodeError):
        # ValueError/UnicodeError: base64 或数值转换失败；KeyError: 缺字段
        raise invalid

    if not isinstance(device_id, str) or not device_id:
        raise invalid
    if time.time() - issued_at > SEARCH_CURSOR_TTL_SECONDS:
        raise HTTPException(status_code=400, detail="cursor expired; please restart the search")
    if payload.get("f") != filters:
        raise HTTPException(
            status_code=400,
            detail="cursor does not match the current filter parameters",
        )
    return {
        "window_start": window_start,
        "device_id": device_id,
        "snapshot_max_rowid": snapshot_max_rowid,
    }


# ---------- 设备组工具 ----------

def placeholders(n: int) -> str:
    return ",".join("?" * n)


def require_group(conn: sqlite3.Connection, group_id: str) -> sqlite3.Row:
    row = conn.execute("SELECT * FROM device_groups WHERE group_id = ?", (group_id,)).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"group not found: {group_id}")
    return row


def group_member_ids(conn: sqlite3.Connection, group_id: str) -> list[str]:
    rows = conn.execute(
        "SELECT device_id FROM device_group_members WHERE group_id = ? ORDER BY position",
        (group_id,),
    ).fetchall()
    return [r["device_id"] for r in rows]


def device_group_rows(conn: sqlite3.Connection, device_id: str) -> list[sqlite3.Row]:
    """返回设备所属的全部组（含各组窗口配置），按 group_id 升序。

    设备可同时属于多个固定成员组；PUT config 时新配置必须与**每一个**所属组一致，
    否则该设备在不同组里会落到不同的窗口网格 / 封存时点。
    """
    return conn.execute(
        "SELECT g.* FROM device_groups g"
        " JOIN device_group_members m ON m.group_id = g.group_id"
        " WHERE m.device_id = ? ORDER BY g.group_id",
        (device_id,),
    ).fetchall()


def incompatible_windows(
    conn: sqlite3.Connection, device_id: str, window_size_seconds: int
) -> list[sqlite3.Row]:
    """返回该设备与目标窗口大小不相容的已有窗口（按 window_start 升序）。

    相容口径（与组对账的网格过滤同一标准）：窗口起点为整秒且落在目标大小的
    对齐网格上，且实际跨度（window_end - window_start）等于目标大小。
    旧库中组成员配置曾可漂移，漂移期间落盘的窗口可能起点错格或跨度不符；
    仅把配置改回组配置而不核对，会让组汇总把这些异网格窗口按新口径对账。
    """
    return conn.execute(
        "SELECT window_start, window_end FROM windows"
        " WHERE device_id = ?"
        " AND ("
        "   (window_start - CAST(window_start AS INTEGER)) != 0"
        "   OR CAST(window_start AS INTEGER) % ? != 0"
        "   OR (window_end - window_start) != ?"
        " )"
        " ORDER BY window_start",
        (device_id, window_size_seconds, window_size_seconds),
    ).fetchall()


def reconcile_group(
    conn: sqlite3.Connection,
    group_id: str,
    now: str,
    only_starts: Optional[set[float]] = None,
) -> None:
    """对一个组的各 window_start 做对账。

    - 全部成员窗口 sealed → ready，计算 sum/avg；已 ready 且成员发生新修正时原位重算、revision+1。
    - 否则维持/落为 pending，value 始终为 NULL，绝不使用部分成员数据。

    两种触发口径：
    - only_starts is None（组创建后的全量初始化）：候选为任一成员出现过的、属于本组对齐
      网格的全部 window_start，遍历历史补算 pending/ready；
    - only_starts 为起点集合（ingest 写入路径的增量对账）：只处理本批实际新增或迟到修正
      触及的窗口，以及水位线推进而刚封存的窗口；同组同窗天然去重（set），其余无变化的
      历史窗口不读取、不参与本次对账。纯重复批次传入空集即完全不触碰组窗口。
    """
    group = conn.execute("SELECT * FROM device_groups WHERE group_id = ?", (group_id,)).fetchone()
    if group is None:
        return
    size = group["window_size_seconds"]
    aggregation = group["aggregation"]

    members = group_member_ids(conn, group_id)
    member_n = len(members)

    if only_starts is None:
        # 组窗口口径：任一成员出现过的、属于本组对齐网格的 window_start 都是候选。
        # 组创建时已保证成员窗口配置一致，这里再按窗口大小过滤以排除理论上的异网格数据。
        starts = [
            r["window_start"]
            for r in conn.execute(
                f"SELECT DISTINCT w.window_start AS window_start FROM windows w"
                f" JOIN device_group_members m ON m.device_id = w.device_id AND m.group_id = ?"
                f" WHERE (w.window_start - CAST(w.window_start AS INTEGER)) = 0"
                f"   AND CAST(w.window_start AS INTEGER) % ? = 0"
                f" ORDER BY w.window_start",
                (group_id, size),
            ).fetchall()
        ]
    else:
        # 增量对账：候选起点全部来自本批变更（设备窗口配置与组一致，整数/网格过滤仅作防御）。
        starts = sorted(
            s for s in only_starts
            if s == int(s) and int(s) % size == 0
        )

    for w_start in starts:
        w_end = w_start + size
        win_rows = {
            r["device_id"]: r
            for r in conn.execute(
                f"SELECT w.* FROM windows w"
                f" JOIN device_group_members m ON m.device_id = w.device_id AND m.group_id = ?"
                f" WHERE w.window_start = ? ORDER BY m.position",
                (group_id, w_start),
            ).fetchall()
        }
        sealed = [win_rows[d] for d in members if d in win_rows and win_rows[d]["status"] == "sealed"]
        sealed_n = len(sealed)
        all_sealed = sealed_n == member_n
        correction_sig = sum(r["correction_count"] for r in sealed)

        existing = conn.execute(
            "SELECT * FROM group_windows WHERE group_id = ? AND window_start = ?",
            (group_id, w_start),
        ).fetchone()

        if all_sealed:
            if aggregation == "sum":
                value = sum(r["sum"] for r in sealed)
            else:
                # avg：各成员窗口平均值的算术平均（每个固定成员权重相同）
                value = sum(r["sum"] / r["count"] for r in sealed if r["count"] > 0) / member_n

            if existing is None:
                conn.execute(
                    "INSERT INTO group_windows (group_id, window_start, window_end, status, value,"
                    " member_count, sealed_member_count, revision, correction_sig, ready_at,"
                    " created_at, updated_at)"
                    " VALUES (?, ?, ?, 'ready', ?, ?, ?, 1, ?, ?, ?, ?)",
                    (group_id, w_start, w_end, value, member_n, member_n, correction_sig, now, now, now),
                )
                # 首次 ready：同一事务追加 revision=1 的不可变快照
                append_revision_snapshot(conn, group_id, w_start, 1, value, members, win_rows, now)
            elif existing["status"] == "ready":
                # 已生成的组汇总原位重算：仅当成员窗口出现新的迟到修正（签名推进）时升版。
                # 同一 event_id 重试在 ingest 侧幂等去重，correction_count 不变，故不重复升版。
                if correction_sig > existing["correction_sig"]:
                    new_revision = existing["revision"] + 1
                    conn.execute(
                        "UPDATE group_windows SET status = 'ready', value = ?, sealed_member_count = ?,"
                        " revision = revision + 1, correction_sig = ?, updated_at = ?"
                        " WHERE group_id = ? AND window_start = ?",
                        (value, member_n, correction_sig, now, group_id, w_start),
                    )
                    # revision 升版：同一事务追加新快照；旧快照不动、绝不覆盖
                    append_revision_snapshot(
                        conn, group_id, w_start, new_revision, value, members, win_rows, now
                    )
            else:
                # pending → ready（迟到修正发生在 ready 之前，已包含在首次结果里，revision 从 1 起）
                conn.execute(
                    "UPDATE group_windows SET status = 'ready', value = ?, sealed_member_count = ?,"
                    " revision = 1, correction_sig = ?, ready_at = ?, updated_at = ?"
                    " WHERE group_id = ? AND window_start = ?",
                    (value, member_n, correction_sig, now, now, group_id, w_start),
                )
                # pending → ready 的首次 ready：同一事务追加 revision=1 快照
                append_revision_snapshot(conn, group_id, w_start, 1, value, members, win_rows, now)
        else:
            if existing is None:
                conn.execute(
                    "INSERT INTO group_windows (group_id, window_start, window_end, status, value,"
                    " member_count, sealed_member_count, revision, correction_sig, ready_at,"
                    " created_at, updated_at)"
                    " VALUES (?, ?, ?, 'pending', NULL, ?, ?, 0, 0, NULL, ?, ?)",
                    (group_id, w_start, w_end, member_n, sealed_n, now, now),
                )
            elif existing["status"] == "pending" and existing["sealed_member_count"] != sealed_n:
                conn.execute(
                    "UPDATE group_windows SET sealed_member_count = ?, updated_at = ?"
                    " WHERE group_id = ? AND window_start = ?",
                    (sealed_n, now, group_id, w_start),
                )


def reconcile_groups_for_device(
    conn: sqlite3.Connection,
    device_id: str,
    now: str,
    affected_starts: set[float],
) -> None:
    """成员设备 ingest 后，对其所属的全部组做**增量**对账。

    只处理本批实际新增/迟到修正触及、以及因水位线推进而刚封存的窗口起点；
    同组同窗由 set 去重只处理一次，其余历史窗口不参与。affected_starts 为空
    （纯重复批次，无任何窗口变化）时不触碰任何组窗口。
    """
    if not affected_starts:
        return
    group_ids = [
        r["group_id"]
        for r in conn.execute(
            "SELECT group_id FROM device_group_members WHERE device_id = ?", (device_id,)
        ).fetchall()
    ]
    for group_id in group_ids:
        reconcile_group(conn, group_id, now, only_starts=affected_starts)


def append_revision_snapshot(
    conn: sqlite3.Connection,
    group_id: str,
    window_start: float,
    revision: int,
    value: float,
    members: list[str],
    win_rows: dict[str, sqlite3.Row],
    now: str,
    kind: Literal["revision", "baseline"] = "revision",
) -> None:
    """在当前事务内为组窗口的一个 revision 追加不可变快照。

    快照头记录当时的 group value；成员明细按固定成员顺序记录每个成员窗口当时的
    count/sum/avg/correction_count。(group_id, window_start, revision) 唯一约束兜底，
    重复上报（签名未推进）不会再次进入这里，即使进入也会被约束挡下而不重复留档。
    """
    cur = conn.execute(
        "INSERT INTO group_window_snapshots"
        " (group_id, window_start, revision, value, kind, created_at)"
        " VALUES (?, ?, ?, ?, ?, ?)",
        (group_id, window_start, revision, value, kind, now),
    )
    snapshot_id = cur.lastrowid
    conn.executemany(
        "INSERT INTO group_window_snapshot_members"
        " (snapshot_id, group_id, device_id, position, count, sum, avg, correction_count)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                snapshot_id,
                group_id,
                device_id,
                position,
                win_rows[device_id]["count"],
                win_rows[device_id]["sum"],
                (
                    win_rows[device_id]["sum"] / win_rows[device_id]["count"]
                    if win_rows[device_id]["count"]
                    else None
                ),
                win_rows[device_id]["correction_count"],
            )
            for position, device_id in enumerate(members)
        ],
    )


def backfill_baseline_snapshots() -> None:
    """旧库升级时为已 ready 但没有任何快照的组窗口补一份**当前 revision** 的 baseline。

    只补当前版本这一份，绝不伪造 ready 之后可能存在过的早期版本；幂等：
    已有任意快照的 ready 窗口跳过，新库（启动即为空）无任何可补数据。
    """
    now = utcnow_iso()
    with db.session() as conn:
        ready_rows = conn.execute(
            "SELECT * FROM group_windows WHERE status = 'ready'"
        ).fetchall()
        for gw in ready_rows:
            exists = conn.execute(
                "SELECT 1 FROM group_window_snapshots"
                " WHERE group_id = ? AND window_start = ? LIMIT 1",
                (gw["group_id"], gw["window_start"]),
            ).fetchone()
            if exists:
                continue

            members = group_member_ids(conn, gw["group_id"])
            win_rows = {
                r["device_id"]: r
                for r in conn.execute(
                    "SELECT w.* FROM windows w"
                    " JOIN device_group_members m ON m.device_id = w.device_id AND m.group_id = ?"
                    " WHERE w.window_start = ?",
                    (gw["group_id"], gw["window_start"]),
                ).fetchall()
            }
            # 数据不自洽（有成员缺窗口）时不补，避免留下不完整的基线
            if len(win_rows) != len(members):
                continue
            append_revision_snapshot(
                conn,
                gw["group_id"],
                gw["window_start"],
                gw["revision"],
                gw["value"],
                members,
                win_rows,
                now,
                kind="baseline",
            )


def group_window_out(r: sqlite3.Row) -> GroupWindowOut:
    return GroupWindowOut(
        group_id=r["group_id"],
        window_start=to_iso(r["window_start"]),
        window_end=to_iso(r["window_end"]),
        status=r["status"],
        value=r["value"],
        member_count=r["member_count"],
        sealed_member_count=r["sealed_member_count"],
        revision=r["revision"],
        ready_at=r["ready_at"],
        created_at=r["created_at"],
        updated_at=r["updated_at"],
    )


# ---------- 设备级高值告警工具 ----------

def validate_alert_thresholds(trigger: float, recovery: float) -> None:
    """阈值合法性校验：必须是有限数（拒绝 NaN/Inf），且恢复线严格低于触发线。

    校验失败直接 400：调用方在开启任何写事务之前执行，非法请求绝不改动既有规则。
    """
    if not math.isfinite(trigger) or not math.isfinite(recovery):
        raise HTTPException(
            status_code=400,
            detail="trigger_threshold and recovery_threshold must be finite numbers",
        )
    if recovery >= trigger:
        raise HTTPException(
            status_code=400,
            detail=(
                f"recovery_threshold ({recovery}) must be strictly lower than"
                f" trigger_threshold ({trigger})"
            ),
        )


def get_alert_rule(conn: sqlite3.Connection, device_id: str) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM device_alert_rules WHERE device_id = ?", (device_id,)
    ).fetchone()


def alert_rule_to_out(r: sqlite3.Row) -> AlertRuleOut:
    return AlertRuleOut(
        device_id=r["device_id"],
        trigger_threshold=r["trigger_threshold"],
        recovery_threshold=r["recovery_threshold"],
        created_at=r["created_at"],
        updated_at=r["updated_at"],
    )


def get_alert_state(conn: sqlite3.Connection, device_id: str) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM device_alert_states WHERE device_id = ?", (device_id,)
    ).fetchone()


def build_alert_out(rule: sqlite3.Row, state: Optional[sqlite3.Row]) -> AlertOut:
    if state is None:
        # 已配置规则但尚无事件驱动过状态机：按初始状态返回
        return AlertOut(
            device_id=rule["device_id"],
            rule=alert_rule_to_out(rule),
            alerting=False,
            alarm_count=0,
            recovery_count=0,
        )
    return AlertOut(
        device_id=rule["device_id"],
        rule=alert_rule_to_out(rule),
        alerting=bool(state["alerting"]),
        alarm_count=state["alarm_count"],
        recovery_count=state["recovery_count"],
        last_value=state["last_value"],
        last_event_id=state["last_event_id"],
        last_event_time=to_iso(state["last_event_time"]),
        last_transition=state["last_transition"],
        entered_at=state["entered_at"],
        cleared_at=state["cleared_at"],
        updated_at=state["updated_at"],
    )


def apply_alert_state_machine(
    conn: sqlite3.Connection,
    rule: sqlite3.Row,
    device_id: str,
    event_id: str,
    event_time: float,
    value: float,
    now: str,
) -> None:
    """在当前 ingest 事务内逐事件驱动双阈值滞回状态机。

    - value >= trigger_threshold 且当前未告警 → entered（首次越线才进入，
      告警中再次出现高值不重复触发）；
    - value <= recovery_threshold 且当前告警中 → cleared（降至恢复线才解除）；
    - recovery < value < trigger 的中间区间保持原状态（滞回防抖）。
    重复事件不会到达这里（ingest 侧先做 (device_id, event_id) 幂等去重）；
    流水表的 (device_id, event_id) 唯一约束是兜底防线。
    """
    state = get_alert_state(conn, device_id)
    currently_alerting = state is not None and bool(state["alerting"])

    if not currently_alerting and value >= rule["trigger_threshold"]:
        kind = "entered"
    elif currently_alerting and value <= rule["recovery_threshold"]:
        kind = "cleared"
    else:
        # 中间区间（或同态继续高值/低值）：仅更新最近事件观测，状态保持不变
        if state is None:
            conn.execute(
                "INSERT INTO device_alert_states"
                " (device_id, alerting, alarm_count, recovery_count, last_value,"
                "  last_event_id, last_event_time, last_transition, entered_at,"
                "  cleared_at, updated_at)"
                " VALUES (?, 0, 0, 0, ?, ?, ?, NULL, NULL, NULL, ?)",
                (device_id, value, event_id, event_time, now),
            )
        else:
            conn.execute(
                "UPDATE device_alert_states SET last_value = ?, last_event_id = ?,"
                " last_event_time = ?, updated_at = ? WHERE device_id = ?",
                (value, event_id, event_time, now, device_id),
            )
        return

    conn.execute(
        "INSERT INTO device_alert_transitions"
        " (device_id, event_id, event_time, value, kind, created_at)"
        " VALUES (?, ?, ?, ?, ?, ?)",
        (device_id, event_id, event_time, value, kind, now),
    )
    if state is None:
        conn.execute(
            "INSERT INTO device_alert_states"
            " (device_id, alerting, alarm_count, recovery_count, last_value,"
            "  last_event_id, last_event_time, last_transition, entered_at,"
            "  cleared_at, updated_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                device_id,
                1 if kind == "entered" else 0,
                1 if kind == "entered" else 0,
                1 if kind == "cleared" else 0,
                value, event_id, event_time, kind,
                now if kind == "entered" else None,
                now if kind == "cleared" else None,
                now,
            ),
        )
    else:
        conn.execute(
            "UPDATE device_alert_states SET alerting = ?,"
            " alarm_count = alarm_count + ?,"
            " recovery_count = recovery_count + ?,"
            " last_value = ?, last_event_id = ?, last_event_time = ?,"
            " last_transition = ?, entered_at = ?, cleared_at = ?, updated_at = ?"
            " WHERE device_id = ?",
            (
                1 if kind == "entered" else 0,
                1 if kind == "entered" else 0,
                1 if kind == "cleared" else 0,
                value, event_id, event_time, kind,
                now if kind == "entered" else state["entered_at"],
                now if kind == "cleared" else state["cleared_at"],
                now,
                device_id,
            ),
        )


# ---------- 接口 ----------

@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


def _run_ingest_batch(
    conn: sqlite3.Connection, req: IngestRequest, now: str
) -> tuple[list[EventResult], Optional[float], set[float]]:
    """在给定事务连接上按请求顺序处理整批事件（/ingest 与 /ingest/preview 共用的唯一写入口径）。

    逐事件推进幂等去重、窗口聚合、迟到修正、告警状态机与设备水位线，最后统一
    封存越线窗口并增量对账设备所在组（只对账本批触及窗口与刚封存窗口，纯重复批次
    不触碰组窗口）。返回 (逐事件结果, 处理后水位线, 本批次触及窗口起点集合)。
    "触及窗口"指至少有一个非重复事件落入的窗口；预演接口据此回读预演后的窗口状态。
    """
    results: list[EventResult] = []
    touched_starts: set[float] = set()

    dev = get_or_create_device(conn, req.device_id, now)
    watermark = dev["watermark"]
    size = dev["window_size_seconds"]
    lateness = dev["allowed_lateness_seconds"]
    # 批次开始后规则若被其他事务替换不会发生（写锁串行化）；
    # 无规则的设备告警状态机不参与本批次处理。
    alert_rule = get_alert_rule(conn, req.device_id)

    for ev in req.events:
        t = to_epoch(ev.event_time)

        # 幂等去重：同一 (device_id, event_id) 只生效一次
        dup = conn.execute(
            "SELECT 1 FROM events WHERE device_id = ? AND event_id = ?",
            (req.device_id, ev.event_id),
        ).fetchone()
        if dup:
            results.append(EventResult(event_id=ev.event_id, status="duplicate"))
            continue

        w_start = window_floor(t, size)
        w_end = w_start + size
        # 所属窗口已封存（水位线已越过 window_end + 允许迟到窗口）=> 迟到数据
        is_late = watermark is not None and watermark >= w_end + lateness
        status = "late" if is_late else "accepted"

        conn.execute(
            "INSERT INTO events (device_id, event_id, event_time, value, status, ingested_at)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (req.device_id, ev.event_id, t, ev.value, status, now),
        )
        conn.execute(
            """
            INSERT INTO windows (device_id, window_start, window_end, count, sum, min, max,
                                 status, correction_count, late_event_count)
            VALUES (?, ?, ?, 1, ?, ?, ?, 'open', ?, ?)
            ON CONFLICT (device_id, window_start) DO UPDATE SET
                count             = count + 1,
                sum               = sum + excluded.sum,
                min               = MIN(windows.min, excluded.min),
                max               = MAX(windows.max, excluded.max),
                correction_count  = correction_count + excluded.correction_count,
                late_event_count  = late_event_count + excluded.late_event_count
            """,
            (
                req.device_id, w_start, w_end, ev.value, ev.value, ev.value,
                1 if is_late else 0,
                1 if is_late else 0,
            ),
        )
        touched_starts.add(w_start)
        if is_late:
            # 迟到数据落入已封存窗口：生成修正记录
            conn.execute(
                "INSERT INTO corrections (device_id, event_id, window_start, event_time, value, created_at)"
                " VALUES (?, ?, ?, ?, ?, ?)",
                (req.device_id, ev.event_id, w_start, t, ev.value, now),
            )

        # 高值告警状态机：与聚合在同一事务内按事件值驱动（迟到事件同样参与，
        # 因为重试的重复事件在上面的去重分支已 continue，故不会重复产生状态变化）
        if alert_rule is not None:
            apply_alert_state_machine(
                conn, alert_rule, req.device_id, ev.event_id, t, ev.value, now
            )

        if watermark is None or t > watermark:
            watermark = t
        results.append(EventResult(event_id=ev.event_id, status=status, window_start=to_iso(w_start)))

    # 持久化水位线，并封存所有越过允许迟到窗口的窗口
    conn.execute(
        "UPDATE devices SET watermark = ? WHERE device_id = ?",
        (watermark, req.device_id),
    )

    # 增量对账的窗口起点 = 本批非重复事件触及（新增/迟到修正）的窗口
    # ∪ 因水位线推进而**刚封存**的窗口。刚封存集合必须在封存 UPDATE 前确定：
    # 只取当前仍 open、封存后即转 sealed 的起点（已封存的旧窗口不会重复对账）。
    # 纯重复批次两集合皆空，组窗口完全不被触碰。
    affected_starts: set[float] = set(touched_starts)
    if watermark is not None:
        affected_starts.update(
            r["window_start"]
            for r in conn.execute(
                "SELECT window_start FROM windows"
                " WHERE device_id = ? AND status = 'open' AND window_end + ? <= ?",
                (req.device_id, lateness, watermark),
            ).fetchall()
        )

    conn.execute(
        "UPDATE windows SET status = 'sealed', sealed_at = ?"
        " WHERE device_id = ? AND status = 'open' AND window_end + ? <= ?",
        (now, req.device_id, lateness, watermark),
    )

    # 窗口封存 / 迟到修正后，仅对本批实际发生变化的窗口增量重算该设备所在组的汇总；
    # 无变化的历史窗口不参与本次写入对账（组创建时的全量初始化仍遍历全部历史窗口）。
    reconcile_groups_for_device(conn, req.device_id, now, affected_starts)

    return results, watermark, touched_starts


def _ingest_response(
    device_id: str, results: list[EventResult], watermark: Optional[float]
) -> IngestResponse:
    return IngestResponse(
        device_id=device_id,
        watermark=to_iso(watermark),
        accepted=sum(1 for r in results if r.status == "accepted"),
        duplicates=sum(1 for r in results if r.status == "duplicate"),
        late=sum(1 for r in results if r.status == "late"),
        results=results,
    )


@app.post("/ingest", response_model=IngestResponse)
def ingest(req: IngestRequest) -> IngestResponse:
    """按设备提交事件批次。批次内事件按提交顺序处理（与流式语义一致）。"""
    now = utcnow_iso()

    # BEGIN IMMEDIATE：ingest 内含告警状态机的读-改-写（以及组汇总对账），
    # 立即加写锁保证并发批次串行化，避免两个写事务基于同一状态快照而丢失状态变化。
    with db.immediate_session() as conn:
        results, watermark, _ = _run_ingest_batch(conn, req, now)

    return _ingest_response(req.device_id, results, watermark)


@app.post("/ingest/preview", response_model=IngestPreviewResponse)
def ingest_preview(req: IngestRequest) -> IngestPreviewResponse:
    """预演一次与 POST /ingest 同口径的批次提交（只读语义，绝不留下写入）。

    接收与 POST /ingest 完全相同的设备与事件批次，在**同一数据库快照**上按请求顺序
    推进去重、水位线、窗口封存、迟到修正、告警状态机与组汇总对账，返回：
    - 逐事件预计状态 accepted/duplicate/late（批内重复 event_id 只计一次；前一事件
      抬高水位线后，后一旧事件可能因此变成 late）；
    - 预演后的预计水位线；
    - 本批次触及窗口（至少落入一个非重复事件）预演后的 count、sum、status、
      correction_count，按 window_start 升序。
    结果口径与随即在无并发写入时提交同一批次完全一致。整个预演在 BEGIN IMMEDIATE
    事务内完成并**一律回滚**：设备（含不存在设备的自动注册）、事件、窗口、修正、
    告警状态/流水及组汇总/审计快照均不留痕。
    """
    now = utcnow_iso()

    # rollback_session 与提交路径同样立即加写锁：预演期间无其他写事务能插入，
    # 逐事件判断基于的快照稳定；结束后全部写入随回滚撤销。
    with db.rollback_session() as conn:
        results, watermark, touched_starts = _run_ingest_batch(conn, req, now)

        windows: list[PreviewWindowOut] = []
        if touched_starts:
            starts_sql = placeholders(len(touched_starts))
            rows = conn.execute(
                f"SELECT * FROM windows WHERE device_id = ?"
                f" AND window_start IN ({starts_sql}) ORDER BY window_start",
                [req.device_id, *sorted(touched_starts)],
            ).fetchall()
            windows = [
                PreviewWindowOut(
                    window_start=to_iso(r["window_start"]),
                    window_end=to_iso(r["window_end"]),
                    status=r["status"],
                    count=r["count"],
                    sum=r["sum"],
                    correction_count=r["correction_count"],
                )
                for r in rows
            ]

    accepted = sum(1 for r in results if r.status == "accepted")
    duplicates = sum(1 for r in results if r.status == "duplicate")
    late = sum(1 for r in results if r.status == "late")
    return IngestPreviewResponse(
        device_id=req.device_id,
        watermark=to_iso(watermark),
        accepted=accepted,
        duplicates=duplicates,
        late=late,
        results=results,
        windows=windows,
    )


@app.put("/devices/{device_id}/config", response_model=DeviceOut)
def put_device_config(device_id: str, cfg: DeviceConfig) -> DeviceOut:
    """创建或更新设备配置。

    - 未入组设备沿用既有规则：已有窗口数据后不允许修改窗口大小（避免历史窗口口径
      不一致），允许迟到秒数可随时调整。
    - 已入组设备的窗口大小与允许迟到秒数必须与**每一个所属组**保持一致：
      任一组的窗口配置不同即返回 409 且不写入——即使该设备尚无任何窗口也不得改窗口
      大小（否则同组 window_start 时间范围错开），允许迟到秒数同样不得随时改
      （否则各成员封存时点不一致，组汇总会提前/延后出数）。提交与所属组完全相同的
      配置属于幂等重复提交，返回 200（也可借此把历史漂移配置纠正回组配置）。
    - 旧库纠偏：提交值与所有所属组一致时，写入前在同一写锁事务内核对该设备
      已有窗口与目标 window_size_seconds 是否相容（起点对齐网格、实际跨度等于
      窗口大小）。任一窗口不相容说明历史漂移已落到窗口数据上，仅改写配置会让
      组汇总按新口径对账异网格窗口而失真——返回 409，设备配置、窗口与组汇总
      均保持原样；无冲突时才允许纠正仅配置字段的历史漂移。

    整个"读设备/读所属组 → 校验 → 写入"在单个 BEGIN IMMEDIATE 写锁事务内完成，
    与同样持写锁的 POST /groups 串行化：组创建的成员校验与设备配置更新不可能交错，
    不会留下"已入组但配置漂移"的成员。
    """
    # BEGIN IMMEDIATE：与组创建互斥，保证成员配置校验与设备配置更新串行
    with db.immediate_session() as conn:
        dev = get_device(conn, device_id)
        if dev is None:
            conn.execute(
                "INSERT INTO devices (device_id, window_size_seconds, allowed_lateness_seconds, watermark, created_at)"
                " VALUES (?, ?, ?, NULL, ?)",
                (device_id, cfg.window_size_seconds, cfg.allowed_lateness_seconds, utcnow_iso()),
            )
        else:
            groups = device_group_rows(conn, device_id)
            if groups:
                # 已入组：新配置必须与每个所属组一致；任一组不一致即拒绝、绝不写入。
                mismatched = [
                    g["group_id"]
                    for g in groups
                    if g["window_size_seconds"] != cfg.window_size_seconds
                    or g["allowed_lateness_seconds"] != cfg.allowed_lateness_seconds
                ]
                if mismatched:
                    raise HTTPException(
                        status_code=409,
                        detail=(
                            "device is a group member; window config must stay identical to"
                            " every group it belongs to (window_size_seconds and"
                            " allowed_lateness_seconds). Conflicting groups: "
                            + ", ".join(mismatched)
                        ),
                    )
                # 旧库纠偏：提交值与所有所属组一致时，写入前核对该设备已有窗口与
                # 目标 window_size_seconds 是否相容（起点对齐 + 实际跨度）。任一窗口
                # 不相容说明历史漂移已落到窗口数据上，仅改写配置会让组汇总按新口径
                # 对账异网格窗口而失真——拒绝并保持设备配置、窗口与组汇总原样
                # （此刻尚未写入，异常退出即整体回滚）。
                incompatible = incompatible_windows(
                    conn, device_id, cfg.window_size_seconds
                )
                if incompatible:
                    raise HTTPException(
                        status_code=409,
                        detail=(
                            "device has existing windows incompatible with target"
                            f" window_size_seconds={cfg.window_size_seconds}"
                            " (misaligned window_start or mismatched span);"
                            " device config, windows and group summaries left unchanged."
                            " Incompatible window_start: "
                            + ", ".join(
                                str(to_iso(r["window_start"])) for r in incompatible[:5]
                            )
                            + (" ..." if len(incompatible) > 5 else "")
                        ),
                    )
                # 与所属组配置完全相同且已有窗口相容：幂等重复提交，按组配置落盘
                # （可纠正仅配置字段的历史漂移）。
                conn.execute(
                    "UPDATE devices SET window_size_seconds = ?, allowed_lateness_seconds = ?"
                    " WHERE device_id = ?",
                    (cfg.window_size_seconds, cfg.allowed_lateness_seconds, device_id),
                )
            else:
                # 未入组：沿用既有规则——已有窗口数据后不允许修改窗口大小。
                if cfg.window_size_seconds != dev["window_size_seconds"]:
                    n = conn.execute(
                        "SELECT COUNT(*) AS c FROM windows WHERE device_id = ?", (device_id,)
                    ).fetchone()["c"]
                    if n > 0:
                        raise HTTPException(
                            status_code=409,
                            detail="window_size_seconds cannot be changed after data has been ingested",
                        )
                conn.execute(
                    "UPDATE devices SET window_size_seconds = ?, allowed_lateness_seconds = ? WHERE device_id = ?",
                    (cfg.window_size_seconds, cfg.allowed_lateness_seconds, device_id),
                )
        return device_out(conn, device_id)


@app.get("/devices", response_model=list[DeviceOut])
def list_devices(
    label_key: Optional[str] = Query(default=None),
    label_value: Optional[str] = Query(default=None),
) -> list[DeviceOut]:
    """列出全部设备；可按当前标签筛选。

    - `label_key` + `label_value`：当前标签精确匹配该键值对的设备；
    - 仅 `label_key`：当前标签中存在该键的设备；
    - 仅 `label_value`：422（值必须依附键筛选）；
    - 都不给：全部设备。
    筛选键名与写入时同样规范化（去空白、小写）后再匹配。
    """
    if label_key is not None and label_key.strip() == "":
        label_key = None
    if label_value is not None and label_key is None:
        raise HTTPException(
            status_code=422, detail="label_value requires label_key to be set"
        )

    sql = _DEVICE_STATS_SELECT
    params: list = []
    if label_key is not None:
        key = label_key.strip().lower()
        if label_value is not None:
            sql += (
                " WHERE EXISTS (SELECT 1 FROM device_labels l"
                " WHERE l.device_id = d.device_id AND l.label_key = ? AND l.label_value = ?)"
            )
            params.extend([key, label_value.strip()])
        else:
            sql += (
                " WHERE EXISTS (SELECT 1 FROM device_labels l"
                " WHERE l.device_id = d.device_id AND l.label_key = ?)"
            )
            params.append(key)
    sql += " ORDER BY d.device_id"

    with db.session() as conn:
        rows = conn.execute(sql, params).fetchall()
        return [device_row_to_out(r) for r in rows]


@app.get("/devices/{device_id}", response_model=DeviceOut)
def get_device_info(device_id: str) -> DeviceOut:
    with db.session() as conn:
        return device_out(conn, device_id)


# ---------- 设备标签接口 ----------

@app.put("/devices/{device_id}/labels", response_model=LabelsOut)
def put_device_labels(device_id: str, req: LabelUpdateRequest) -> LabelsOut:
    """全量替换设备标签（不超过 20 个 key/value），版本单调递增。

    在单个 SQLite 写事务内完成：规范化键名（去空白、小写）、拒绝重复键、
    乐观并发校验（if_version 与当前版本不符返回 409 且不写入任何数据）、
    删除旧标签、写入新标签并把版本加一。
    """
    labels = normalize_labels(req.labels)  # 规范化/校验失败时不进入事务、不改数据
    now = utcnow_iso()

    with db.immediate_session() as conn:
        dev = require_device(conn, device_id)
        current_version = dev["label_version"]

        if req.if_version is not None and req.if_version != current_version:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"label version conflict: if_version={req.if_version} "
                    f"does not match current version {current_version}"
                ),
            )

        new_version = current_version + 1
        conn.execute("DELETE FROM device_labels WHERE device_id = ?", (device_id,))
        conn.executemany(
            "INSERT INTO device_labels (device_id, label_key, label_value, updated_at)"
            " VALUES (?, ?, ?, ?)",
            [(device_id, key, value, now) for key, value in labels.items()],
        )
        conn.execute(
            "UPDATE devices SET label_version = ?, labels_updated_at = ? WHERE device_id = ?",
            (new_version, now, device_id),
        )

    return LabelsOut(
        device_id=device_id, version=new_version, labels=labels, updated_at=now
    )


@app.get("/devices/{device_id}/labels", response_model=LabelsOut)
def get_device_labels_endpoint(device_id: str) -> LabelsOut:
    """返回设备当前标签与版本；从未写入标签时版本为 0、标签为空。"""
    with db.session() as conn:
        dev = require_device(conn, device_id)
        return LabelsOut(
            device_id=device_id,
            version=dev["label_version"],
            labels=get_device_labels(conn, device_id),
            updated_at=dev["labels_updated_at"],
        )


@app.get("/devices/{device_id}/windows", response_model=list[WindowOut])
def list_windows(
    device_id: str,
    status: Optional[Literal["open", "sealed"]] = None,
) -> list[WindowOut]:
    """返回设备各时间窗的聚合值、状态与修正次数。"""
    with db.session() as conn:
        require_device(conn, device_id)
        sql = "SELECT * FROM windows WHERE device_id = ?"
        params: list = [device_id]
        if status:
            sql += " AND status = ?"
            params.append(status)
        sql += " ORDER BY window_start"
        rows = conn.execute(sql, params).fetchall()
        return [window_row_to_out(r) for r in rows]


@app.get("/devices/{device_id}/corrections", response_model=list[CorrectionOut])
def list_corrections(device_id: str) -> list[CorrectionOut]:
    """返回设备全部修正记录（窗口封存后到达的迟到数据）。"""
    with db.session() as conn:
        require_device(conn, device_id)
        rows = conn.execute(
            "SELECT * FROM corrections WHERE device_id = ? ORDER BY id", (device_id,)
        ).fetchall()
        return [
            CorrectionOut(
                id=r["id"],
                device_id=r["device_id"],
                event_id=r["event_id"],
                window_start=to_iso(r["window_start"]),
                event_time=to_iso(r["event_time"]),
                value=r["value"],
                created_at=r["created_at"],
            )
            for r in rows
        ]


# ---------- 设备缺报区间查询接口 ----------

def _merge_overlapping_intervals(
    intervals: list[tuple[float, float]],
) -> list[tuple[float, float]]:
    """把严格重叠（半开区间相交，端点触碰不算重叠）的区间合并成不相交并集。

    输入为半开区间 [start, end)：两个区间仅当一个的起点**严格小于**另一个的终点
    （s < other_end）才算重叠；[a,b) 与 [b,c) 仅端点触碰，不合并、分别保留。
    """
    ordered = sorted(intervals)
    merged: list[tuple[float, float]] = []
    for s, e in ordered:
        if merged and s < merged[-1][1]:
            # 严格重叠：并入当前并集（终点取较大者）；s == merged[-1][1] 是端点触碰，不合并
            merged[-1] = (merged[-1][0], max(merged[-1][1], e))
        else:
            merged.append((s, e))
    return merged


def _subtract_intervals(
    bases: list[tuple[float, float]],
    cuts: list[tuple[float, float]],
) -> list[tuple[float, float]]:
    """从一组不相交基段中扣除另一组**已合并**的不相交区间，返回剩余连续段。

    两组区间都按起点升序且各自不重叠；扣除按半开集合差（cut 终点处重新开口），
    端点触碰（cut 的某端正好落在基段边界上）天然不会多扣：
    - 基段起点恰为某个 cut 终点时，该位置正好重新计入剩余（s == cut_end 不进扣除分支）；
    - 基段被整段覆盖（piece_end <= cut_end）时直接消失，不会产生零长段。
    全程只对段做游标推进（每组段至多线性扫描），不按窗口逐格遍历。
    """
    result: list[tuple[float, float]] = []
    j = 0
    for base_s, base_e in bases:
        s = base_s
        while j < len(cuts) and cuts[j][1] <= s:
            # 该扣除段已完全落在当前剩余位置左侧（含端点触碰），之后也不会再用到
            j += 1
        k = j
        while k < len(cuts) and cuts[k][0] < base_e:
            cs, ce = cuts[k]
            if ce <= s:
                # 扣除段在当前位置之前结束（与剩余部分仅端点触碰或完全靠左）：不扣
                k += 1
                continue
            if cs > s:
                # [s, cs) 落在扣除段之前，是一段剩余
                result.append((s, min(cs, base_e)))
            # 扣除段覆盖 [max(s,cs), min(ce,base_e))，剩余起点推进到 ce
            s = max(s, ce)
            if s >= base_e:
                break
            k += 1
        if s < base_e:
            result.append((s, base_e))
    return result


def _planned_outages_overlapping(
    conn: sqlite3.Connection, device_id: str, start_ts: float, end_ts: float
) -> list[tuple[float, float]]:
    """读取与半开查询范围 [start_ts, end_ts) 严格重叠（端点触碰不算）的计划停报时段。

    条件 start_time < end_ts AND end_time > start_ts：
    [a,b) 与 [b,c) 在 b 处仅端点触碰，不算重叠，不被取出。
    """
    rows = conn.execute(
        "SELECT start_time, end_time FROM planned_outages"
        " WHERE device_id = ? AND start_time < ? AND end_time > ?"
        " ORDER BY start_time, end_time",
        (device_id, end_ts, start_ts),
    ).fetchall()
    return [(r["start_time"], r["end_time"]) for r in rows]


@app.get("/devices/{device_id}/missing-windows", response_model=list[MissingWindowSegmentOut])
def list_missing_windows(
    device_id: str,
    start: datetime = Query(..., description="统计范围下界（含），必须对齐设备窗口大小，ISO 8601"),
    end: datetime = Query(..., description="统计范围上界（不含），必须对齐设备窗口大小，ISO 8601"),
    exclude_planned: bool = Query(
        default=False,
        description="为 true 时合并并扣除查询范围内重叠的计划停报时段（端点触碰不算重叠）",
    ),
) -> list[MissingWindowSegmentOut]:
    """查询设备在 [start, end) 内的连续缺报区间（只读）。

    一个对齐窗口位置被计为"缺失"需同时满足：
    - 不早于设备最早有数据窗口的下一个窗口（设备尚未开始上报之前不追缺）；
    - 水位线已越过该窗口的封存条件（watermark >= window_end + allowed_lateness），
      尚未到达封存条件的窗口一律不报缺；
    - windows 表中没有该位置的窗口记录。

    实现按现存窗口之间的空隙计算（空隙宽度 / 窗口大小 = 缺失窗口数），
    不逐窗遍历整个跨度；迟到事件首次填入空窗后会新增窗口记录，
    后续查询自然按最新空隙缩短或拆分缺报段，不追改任何已有聚合。

    exclude_planned=true 时，先按上述水位线、现存窗口与迟到补报规则求出原始缺报段，
    再取出与查询范围严格重叠（半开相交，端点触碰不算）的计划停报时段，先合并重叠计划
    （互相重叠的多条只扣一次，杜绝重复扣除），再按半开集合差从缺报段中扣除，
    返回剩余连续缺报段及窗口数；区间运算全程按段进行，不逐窗遍历。
    设备不存在返回 404；设备尚无数据返回空列表；
    end <= start 或边界未对齐设备窗口大小返回 422。
    """
    with db.session() as conn:
        dev = require_device(conn, device_id)
        size = dev["window_size_seconds"]
        lateness = dev["allowed_lateness_seconds"]
        watermark = dev["watermark"]

        start_ts = to_epoch(start)
        end_ts = to_epoch(end)
        if end_ts <= start_ts:
            raise HTTPException(status_code=422, detail="end must be strictly greater than start")
        if start_ts % size != 0 or end_ts % size != 0:
            raise HTTPException(
                status_code=422,
                detail=f"start and end must be aligned to the device window size ({size}s)",
            )

        if watermark is None:
            return []
        first_start = conn.execute(
            "SELECT MIN(window_start) AS first_start FROM windows WHERE device_id = ?",
            (device_id,),
        ).fetchone()["first_start"]
        if first_start is None:
            return []

        # 统计下界：最早有数据窗口的下一个窗口，且不早于 start
        lo = max(start_ts, first_start + size)
        # 满足封存条件的最大窗口起点（含）：watermark >= window_start + size + lateness
        max_start = watermark - size - lateness
        # 缺失位置的半开上界：对齐到窗口网格，且不超出 end
        limit = min(end_ts, window_floor(max_start, size) + size)
        if lo >= limit:
            return []

        # 只需 [lo, limit) 内的现存窗口记录，按空隙算术推出缺失段，不逐窗遍历跨度
        starts = [
            r["window_start"]
            for r in conn.execute(
                "SELECT window_start FROM windows"
                " WHERE device_id = ? AND window_start >= ? AND window_start < ?"
                " ORDER BY window_start",
                (device_id, lo, limit),
            ).fetchall()
        ]

        gaps: list[tuple[float, float]] = []
        cursor = lo
        for w in starts:
            if w > cursor:
                gaps.append((cursor, w))
            cursor = w + size
        if cursor < limit:
            gaps.append((cursor, limit))

        if exclude_planned and gaps:
            # 与查询范围严格重叠的计划区间；先合并互相重叠的计划（并集只扣一次），
            # 再从原始缺报段中做半开集合差。触碰查询边界（start_time == end_ts 等）
            # 的计划不会被取出，自然不参与扣除。
            planned = _planned_outages_overlapping(conn, device_id, start_ts, end_ts)
            merged_planned = _merge_overlapping_intervals(planned)
            gaps = _subtract_intervals(gaps, merged_planned)

        return [
            MissingWindowSegmentOut(
                start=to_iso(gap_start),
                end=to_iso(gap_end),
                missing_windows=int(round((gap_end - gap_start) / size)),
            )
            for gap_start, gap_end in gaps
        ]


# ---------- 设备计划停报（已批准停机报备）接口 ----------

def _planned_outage_to_out(r: sqlite3.Row) -> PlannedOutageOut:
    return PlannedOutageOut(
        id=r["id"],
        device_id=r["device_id"],
        start=to_iso(r["start_time"]),
        end=to_iso(r["end_time"]),
        reason=r["reason"],
        created_at=r["created_at"],
    )


@app.post(
    "/devices/{device_id}/planned-outages",
    response_model=PlannedOutageOut,
    status_code=201,
)
def register_planned_outage(device_id: str, req: PlannedOutageIn) -> PlannedOutageOut:
    """按设备登记一个计划停报（已批准停机）半开区间 [start, end)。

    start/end 必须对齐该设备窗口大小，且 end 严格大于 start；未对齐或 end <= start
    返回 422，设备不存在返回 404。可携带可选 reason（去首尾空白；纯空白记为 null）。
    允许与已有计划重叠登记多条——缺报查询扣除前会先按严格重叠合并，重叠部分只扣一次。
    """
    now = utcnow_iso()
    with db.immediate_session() as conn:
        dev = require_device(conn, device_id)
        size = dev["window_size_seconds"]

        start_ts = to_epoch(req.start)
        end_ts = to_epoch(req.end)
        if end_ts <= start_ts:
            raise HTTPException(status_code=422, detail="end must be strictly greater than start")
        if start_ts % size != 0 or end_ts % size != 0:
            raise HTTPException(
                status_code=422,
                detail=(
                    f"start and end must be aligned to the device window size ({size}s)"
                ),
            )

        reason = req.reason.strip() if req.reason is not None else None
        cur = conn.execute(
            "INSERT INTO planned_outages (device_id, start_time, end_time, reason, created_at)"
            " VALUES (?, ?, ?, ?, ?)",
            (device_id, start_ts, end_ts, reason or None, now),
        )
        row = conn.execute(
            "SELECT * FROM planned_outages WHERE id = ?", (cur.lastrowid,)
        ).fetchone()
        return _planned_outage_to_out(row)


@app.get(
    "/devices/{device_id}/planned-outages",
    response_model=list[PlannedOutageOut],
)
def list_planned_outages(device_id: str) -> list[PlannedOutageOut]:
    """列出设备全部计划停报时段（按 start、id 升序）；设备不存在返回 404。"""
    with db.session() as conn:
        require_device(conn, device_id)
        rows = conn.execute(
            "SELECT * FROM planned_outages WHERE device_id = ?"
            " ORDER BY start_time, end_time, id",
            (device_id,),
        ).fetchall()
        return [_planned_outage_to_out(r) for r in rows]


# ---------- 跨设备窗口检索接口 ----------

@app.get("/windows/search", response_model=WindowSearchResponse)
def search_windows(
    start: Optional[datetime] = Query(
        default=None, description="窗口时间范围下界（对 window_start，含），ISO 8601"
    ),
    end: Optional[datetime] = Query(
        default=None, description="窗口时间范围上界（对 window_start，不含），ISO 8601"
    ),
    status: Optional[Literal["open", "sealed"]] = Query(
        default=None, description="窗口状态精确筛选"
    ),
    label_key: Optional[str] = Query(default=None, description="设备当前标签键（规范化后匹配）"),
    label_value: Optional[str] = Query(default=None, description="设备当前标签值（与 label_key 同时给出时按键值精确匹配）"),
    limit: int = Query(default=SEARCH_DEFAULT_LIMIT, ge=1, le=SEARCH_MAX_LIMIT),
    cursor: Optional[str] = Query(default=None, description="上一页返回的 next_cursor；只可用于相同筛选条件"),
) -> WindowSearchResponse:
    """跨设备检索窗口（稳定游标分页，只读）。

    按窗口时间范围（对 window_start 的半开区间 [start, end)）、open/sealed 状态及设备
    **当前**标签精确筛选，按 window_start、device_id 升序返回窗口聚合与设备标识。
    设备标签筛选语义与 GET /devices 一致：label_key+label_value 精确匹配键值对、
    仅 label_key 匹配含该键的设备、只给 label_value 返回 422。

    分页游标与本次筛选条件绑定并签名：翻页期间新增的窗口不会进入本次遍历的行快照，
    既不会在后续页重复出现，也不会把原有行挤出本页或插入到已遍历区间造成遗漏；
    需要看新增数据时不带 cursor 重新发起检索即可。游标非法、篡改、过期或与当前筛选
    条件不一致均返回 400。
    """
    if label_key is not None and label_key.strip() == "":
        label_key = None
    if label_value is not None and label_key is None:
        raise HTTPException(status_code=422, detail="label_value requires label_key to be set")

    start_epoch = to_epoch(start) if start is not None else None
    end_epoch = to_epoch(end) if end is not None else None
    if start_epoch is not None and end_epoch is not None and end_epoch <= start_epoch:
        raise HTTPException(
            status_code=422, detail="end must be strictly greater than start"
        )

    key = label_key.strip().lower() if label_key is not None else None
    value = label_value.strip() if label_value is not None else None
    # 游标绑定的规范化筛选条件（JSON 可序列化、与请求解码后的形态比较）
    filters: dict[str, Any] = {
        "start": start_epoch,
        "end": end_epoch,
        "status": status,
        "label_key": key,
        "label_value": value,
    }

    with db.session() as conn:
        if cursor is not None:
            cur = decode_search_cursor(cursor, filters)
            anchor_ws = cur["window_start"]
            anchor_device = cur["device_id"]
            snapshot_max_rowid = cur["snapshot_max_rowid"]
        else:
            anchor_ws = None
            anchor_device = None
            # 首次检索固定行快照上界：本事务内 rowid 大于该值的行（翻页期间新增）一律忽略，
            # 保证后续页与首页遍历的是同一份稳定集合
            row = conn.execute("SELECT COALESCE(MAX(rowid), 0) AS m FROM windows").fetchone()
            snapshot_max_rowid = row["m"]

        sql = "SELECT w.* FROM windows w"
        # JOIN 占位符在语句文本上先于 WHERE，参数分两组按出现顺序组装
        join_params: list[Any] = []
        clauses = ["w.rowid <= ?"]
        where_params: list[Any] = [snapshot_max_rowid]
        if start_epoch is not None:
            clauses.append("w.window_start >= ?")
            where_params.append(start_epoch)
        if end_epoch is not None:
            clauses.append("w.window_start < ?")
            where_params.append(end_epoch)
        if status is not None:
            clauses.append("w.status = ?")
            where_params.append(status)
        if key is not None:
            # device_labels 主键 (device_id, label_key) 保证每个设备至多匹配一行，JOIN 不会产生重复窗口
            if value is not None:
                sql += (
                    " JOIN device_labels dl ON dl.device_id = w.device_id"
                    " AND dl.label_key = ? AND dl.label_value = ?"
                )
                join_params.extend([key, value])
            else:
                sql += (
                    " JOIN device_labels dl ON dl.device_id = w.device_id"
                    " AND dl.label_key = ?"
                )
                join_params.append(key)
        if anchor_ws is not None:
            # 键集翻页：(window_start, device_id) 严格大于游标锚点
            clauses.append(
                "(w.window_start > ? OR (w.window_start = ? AND w.device_id > ?))"
            )
            where_params.extend([anchor_ws, anchor_ws, anchor_device])

        sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY w.window_start, w.device_id LIMIT ?"

        rows = conn.execute(sql, [*join_params, *where_params, limit + 1]).fetchall()
        has_more = len(rows) > limit
        page = rows[:limit]
        windows = [window_row_to_out(r) for r in page]

        next_cursor: Optional[str] = None
        if has_more:
            last = page[-1]
            next_cursor = encode_search_cursor(
                last["window_start"],
                last["device_id"],
                snapshot_max_rowid,
                filters,
            )

        return WindowSearchResponse(windows=windows, next_cursor=next_cursor)


# ---------- 封存窗口稳健异常检测接口 ----------

def _median(values: list[float]) -> float:
    """有限样本中位数：偶数个时取中间两个的平均值。"""
    ordered = sorted(values)
    n = len(ordered)
    mid = n // 2
    if n % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2


@app.get("/devices/{device_id}/anomalies", response_model=list[AnomalyOut])
def list_device_anomalies(
    device_id: str,
    window_start: datetime = Query(..., description="窗口起始时间（ISO 8601），按设备窗口大小对齐"),
    threshold: float = Query(default=DEFAULT_ANOMALY_THRESHOLD, gt=0),
) -> list[AnomalyOut]:
    """对指定**已封存**窗口内的事件做中位数/MAD 稳健异常检测（只读）。

    score = 0.6745 * |value - median| / MAD（修正 z 分数），仅返回 score > threshold 的事件，
    threshold 默认 3.5 且必须 > 0，否则返回 422。MAD 为 0 时除以 0 无定义，
    按需求把所有不等于中位数的事件一律判为异常（score 返回 null）。
    检测包含封存后到达的迟到事件；纯只读，不改变窗口聚合与修正计数。
    设备或窗口不存在返回 404，窗口尚未封存（open）返回 409。
    """
    w_start = to_epoch(window_start)
    with db.session() as conn:
        require_device(conn, device_id)
        win = conn.execute(
            "SELECT * FROM windows WHERE device_id = ? AND window_start = ?",
            (device_id, w_start),
        ).fetchone()
        if win is None:
            raise HTTPException(
                status_code=404,
                detail=f"window not found for device {device_id} at window_start {window_start.isoformat()}",
            )
        if win["status"] != "sealed":
            raise HTTPException(
                status_code=409,
                detail=(
                    f"window at {window_start.isoformat()} is not sealed; "
                    "anomaly detection is only available on sealed windows"
                ),
            )

        rows = conn.execute(
            "SELECT event_id, event_time, value FROM events"
            " WHERE device_id = ? AND event_time >= ? AND event_time < ?"
            " ORDER BY event_time, event_id",
            (device_id, win["window_start"], win["window_end"]),
        ).fetchall()

        values = [r["value"] for r in rows]
        if not values:
            return []

        median = _median(values)
        mad = _median([abs(v - median) for v in values])

        anomalies: list[AnomalyOut] = []
        if mad == 0.0:
            # MAD 为 0：除以 0 无定义，所有非中位值按异常处理；全等于中位数则无异常
            for r in rows:
                if r["value"] != median:
                    anomalies.append(AnomalyOut(
                        event_id=r["event_id"],
                        event_time=to_iso(r["event_time"]),
                        value=r["value"],
                        score=None,
                    ))
            return anomalies

        for r in rows:
            score = MAD_NORMALIZER * abs(r["value"] - median) / mad
            if score > threshold:
                anomalies.append(AnomalyOut(
                    event_id=r["event_id"],
                    event_time=to_iso(r["event_time"]),
                    value=r["value"],
                    score=score,
                ))
        return anomalies


# ---------- 设备每日同窗基线偏移查询接口 ----------

@app.get("/devices/{device_id}/daily-baseline", response_model=DailyBaselineOut)
def get_device_daily_baseline(
    device_id: str,
    window_start: datetime = Query(..., description="目标窗口起始时间（ISO 8601），按设备窗口大小对齐"),
    days: int = Query(..., ge=MIN_BASELINE_DAYS, le=MAX_BASELINE_DAYS, description="按 UTC 逐日回看天数（3..30）"),
    threshold: float = Query(..., gt=0, description="正数越界阈值，score 严格大于该值判越界"),
) -> DailyBaselineOut:
    """查询目标窗口相对其按 UTC 每天相同窗位的历史基线的偏移（只读）。

    前置条件：设备窗口秒数必须可整除一天（86400 % window_size_seconds == 0），
    否则"每天同一窗位"无法逐日对齐，返回 422；目标窗口不存在返回 404、
    尚未封存（open）返回 409；设备不存在返回 404。

    样本采集：取 window_start - k*86400（k=1..days）各天同一窗位的历史窗口，
    仅当窗口存在且**已封存**（含迟到修正后的最新聚合）时作为有效样本，
    缺失或仍 open 的位置一律跳过，绝不用临近窗口代替。

    打分：基线中位数/MAD 取有效窗口均值（无 0.6745 归一化），
    score = |目标均值 - 中位数| / MAD，score > threshold 判越界。
    有效样本不足 3 个返回 status=insufficient_samples（score 与 breached 均为 null）。
    MAD=0 时，目标均值与中位数同值记 score=0、不越界；异值偏移分数无定义（null）并判越界。
    迟到修正原位更新窗口聚合，再次查询立即反映最新值。
    """
    w_start = to_epoch(window_start)
    with db.session() as conn:
        dev = require_device(conn, device_id)
        size = dev["window_size_seconds"]

        if SECONDS_PER_DAY % size != 0:
            raise HTTPException(
                status_code=422,
                detail=(
                    f"window size {size}s must divide one day ({SECONDS_PER_DAY}s) exactly"
                    " so that the same window position lines up across UTC days"
                ),
            )

        target = conn.execute(
            "SELECT * FROM windows WHERE device_id = ? AND window_start = ?",
            (device_id, w_start),
        ).fetchone()
        if target is None:
            raise HTTPException(
                status_code=404,
                detail=f"window not found for device {device_id} at window_start {window_start.isoformat()}",
            )
        if target["status"] != "sealed":
            raise HTTPException(
                status_code=409,
                detail=(
                    f"window at {window_start.isoformat()} is not sealed; "
                    "daily-baseline comparison is only available on sealed windows"
                ),
            )

        target_avg = target["sum"] / target["count"] if target["count"] else None

        # 逐日采集同格已封存窗口；缺失/未封存位置跳过（绝不以临近窗口代替）。
        # 天数上限 30，逐日点查代价有界。
        sample_rows: list[sqlite3.Row] = []
        for k in range(1, days + 1):
            hist_start = w_start - k * SECONDS_PER_DAY
            row = conn.execute(
                "SELECT * FROM windows WHERE device_id = ? AND window_start = ?",
                (device_id, hist_start),
            ).fetchone()
            if row is not None and row["status"] == "sealed" and row["count"] > 0:
                sample_rows.append(row)
        sample_rows.sort(key=lambda r: r["window_start"])

        samples = [
            DailyBaselineSampleOut(
                window_start=to_iso(r["window_start"]),
                window_end=to_iso(r["window_end"]),
                offset_days=int(round((w_start - r["window_start"]) / SECONDS_PER_DAY)),
                avg=r["sum"] / r["count"],
            )
            for r in sample_rows
        ]

        result = DailyBaselineOut(
            device_id=device_id,
            window_start=to_iso(w_start),
            window_end=to_iso(w_start + size),
            days=days,
            status="insufficient_samples" if len(samples) < MIN_BASELINE_SAMPLES else "ok",
            sample_count=len(samples),
            samples=samples,
            target_avg=target_avg,
            threshold=threshold,
        )
        if len(samples) < MIN_BASELINE_SAMPLES:
            # 有效样本不足：不计算基线、不判越界
            return result

        avgs = [s.avg for s in samples]
        median = _median(avgs)
        mad = _median([abs(v - median) for v in avgs])
        deviation = abs(target_avg - median) if target_avg is not None else None
        result.baseline_median = median
        result.baseline_mad = mad
        result.deviation = deviation

        if mad == 0.0:
            # MAD 为 0：同值偏移记零、不越界；异值偏移除以 0 无定义，score=null 并判越界。
            # 均值经 sum/count 得到，数学同值可能残留极小浮点误差，用容差判定"同值"。
            same_value = math.isclose(deviation, 0.0, rel_tol=1e-9, abs_tol=1e-12) if deviation is not None else False
            result.score = 0.0 if same_value else None
            result.breached = not same_value
        else:
            result.score = deviation / mad
            result.breached = result.score > threshold
        return result


# ---------- 设备持久化聚合完整性核查接口 ----------

def _integrity_float_match(a: Optional[float], b: Optional[float]) -> bool:
    """浮点聚合对账：相对或绝对误差 1e-9 内视为一致；None 仅与 None 一致。"""
    if a is None or b is None:
        return a is None and b is None
    return math.isclose(
        a, b, rel_tol=INTEGRITY_FLOAT_TOLERANCE, abs_tol=INTEGRITY_FLOAT_TOLERANCE
    )


def _integrity_mismatched_fields(
    expected: IntegrityAggregateOut, actual: Optional[IntegrityAggregateOut]
) -> list[str]:
    """逐项对账五项聚合，返回不一致字段名；actual 为 None（缺少窗口行）时全部字段不一致。"""
    if actual is None:
        return ["count", "sum", "min", "max", "late_event_count"]
    mismatched: list[str] = []
    if expected.count != actual.count:
        mismatched.append("count")
    if not _integrity_float_match(expected.sum, actual.sum):
        mismatched.append("sum")
    if not _integrity_float_match(expected.min, actual.min):
        mismatched.append("min")
    if not _integrity_float_match(expected.max, actual.max):
        mismatched.append("max")
    if expected.late_event_count != actual.late_event_count:
        mismatched.append("late_event_count")
    return mismatched


@app.get("/devices/{device_id}/integrity", response_model=IntegrityReportOut)
def check_device_integrity(
    device_id: str,
    start: datetime = Query(..., description="核查范围下界（含），必须对齐设备窗口大小，ISO 8601"),
    end: datetime = Query(..., description="核查范围上界（不含），必须对齐设备窗口大小，ISO 8601"),
) -> IntegrityReportOut:
    """核查指定窗口范围内持久化聚合与原始事件的一致性（只读）。

    在单次 SQLite 只读事务内按事件时间把原始事件归窗（分桶公式与 ingest 的
    window_floor 一致），重算 count/sum/min/max/late_event_count，并与 windows 表
    逐窗对账：
    - 有事件但缺少窗口行（missing_window_row）、或存在无事件的窗口行
      （unexpected_window_row）都列为差异；
    - 两侧都有记录的窗口逐项比较五项聚合，浮点聚合按相对或绝对误差 1e-9 判定一致。
    差异项按 window_start 升序返回，每项给出期望值、实际值（缺行为 null）与不一致字段。
    start/end 必须对齐设备窗口大小且 end > start，单次最多覆盖 1000 个窗口；
    设备不存在返回 404，范围非法返回 422。核查纯只读，不改动水位线、修正记录与组汇总。
    """
    with db.session() as conn:
        # 显式只读事务：以下所有查询落在同一快照上（WAL 下并发写入不影响本快照），
        # 会话结束提交的是纯读事务，不改动任何数据
        conn.execute("BEGIN")
        dev = require_device(conn, device_id)
        size = dev["window_size_seconds"]

        start_ts = to_epoch(start)
        end_ts = to_epoch(end)
        if end_ts <= start_ts:
            raise HTTPException(status_code=422, detail="end must be strictly greater than start")
        if start_ts % size != 0 or end_ts % size != 0:
            raise HTTPException(
                status_code=422,
                detail=f"start and end must be aligned to the device window size ({size}s)",
            )
        window_count = int(round((end_ts - start_ts) / size))
        if window_count > INTEGRITY_MAX_WINDOWS:
            raise HTTPException(
                status_code=422,
                detail=(
                    f"range covers {window_count} windows, exceeding the maximum of"
                    f" {INTEGRITY_MAX_WINDOWS}"
                ),
            )

        # 从原始事件重算：FLOOR(event_time / size) 与 ingest 的 window_floor 同为
        # IEEE 754 双精度除法后向下取整，归窗口径完全一致
        event_rows = conn.execute(
            "SELECT CAST(FLOOR(event_time / ?) AS INTEGER) AS bucket,"
            " COUNT(*) AS count, SUM(value) AS sum, MIN(value) AS min, MAX(value) AS max,"
            " SUM(CASE WHEN status = 'late' THEN 1 ELSE 0 END) AS late_event_count"
            " FROM events"
            " WHERE device_id = ? AND event_time >= ? AND event_time < ?"
            " GROUP BY bucket",
            (size, device_id, start_ts, end_ts),
        ).fetchall()
        window_rows = conn.execute(
            "SELECT window_start, count, sum, min, max, late_event_count"
            " FROM windows WHERE device_id = ? AND window_start >= ? AND window_start < ?",
            (device_id, start_ts, end_ts),
        ).fetchall()

    expected_by_start: dict[float, IntegrityAggregateOut] = {
        float(r["bucket"] * size): IntegrityAggregateOut(
            count=r["count"],
            sum=r["sum"],
            min=r["min"],
            max=r["max"],
            late_event_count=r["late_event_count"],
        )
        for r in event_rows
    }
    actual_by_start: dict[float, IntegrityAggregateOut] = {
        r["window_start"]: IntegrityAggregateOut(
            count=r["count"],
            sum=r["sum"],
            min=r["min"],
            max=r["max"],
            late_event_count=r["late_event_count"],
        )
        for r in window_rows
    }

    discrepancies: list[IntegrityDiscrepancyOut] = []
    for w_start in sorted(set(expected_by_start) | set(actual_by_start)):
        expected = expected_by_start.get(w_start)
        actual = actual_by_start.get(w_start)
        if expected is None:
            # 存在无事件的窗口行：期望为全零聚合（无事件时 min/max 为 null）
            expected = IntegrityAggregateOut(
                count=0, sum=0.0, min=None, max=None, late_event_count=0
            )
            kind = "unexpected_window_row"
        elif actual is None:
            kind = "missing_window_row"
        else:
            kind = "aggregate_mismatch"
        mismatched = _integrity_mismatched_fields(expected, actual)
        if kind == "aggregate_mismatch" and not mismatched:
            continue
        discrepancies.append(
            IntegrityDiscrepancyOut(
                window_start=to_iso(w_start),
                window_end=to_iso(w_start + size),
                kind=kind,
                expected=expected,
                actual=actual,
                mismatched_fields=mismatched,
            )
        )

    return IntegrityReportOut(
        device_id=device_id,
        start=start,
        end=end,
        window_size_seconds=size,
        windows_checked=window_count,
        discrepancy_count=len(discrepancies),
        discrepancies=discrepancies,
    )


# ---------- 设备级高值告警接口 ----------

@app.put("/devices/{device_id}/alert-rule", response_model=AlertRuleOut)
def put_device_alert_rule(device_id: str, req: AlertRuleIn) -> AlertRuleOut:
    """设置（替换）设备高值告警的双阈值规则。

    触发阈值必须严格高于恢复阈值，且二者都必须是有限数（NaN/Infinity 拒绝）；
    任一不合法返回 400 且不开启写事务，**旧规则保持原样不被覆盖**。
    设备不存在返回 404。规则替换本身不触发任何状态转换，既有告警状态保持不变，
    后续事件按新阈值驱动状态机。
    """
    validate_alert_thresholds(req.trigger_threshold, req.recovery_threshold)
    now = utcnow_iso()
    with db.session() as conn:
        require_device(conn, device_id)
        existing = get_alert_rule(conn, device_id)
        if existing is None:
            conn.execute(
                "INSERT INTO device_alert_rules"
                " (device_id, trigger_threshold, recovery_threshold, created_at, updated_at)"
                " VALUES (?, ?, ?, ?, ?)",
                (device_id, req.trigger_threshold, req.recovery_threshold, now, now),
            )
        else:
            conn.execute(
                "UPDATE device_alert_rules SET trigger_threshold = ?,"
                " recovery_threshold = ?, updated_at = ? WHERE device_id = ?",
                (req.trigger_threshold, req.recovery_threshold, now, device_id),
            )
        return alert_rule_to_out(get_alert_rule(conn, device_id))


@app.get("/devices/{device_id}/alert", response_model=AlertOut)
def get_device_alert(device_id: str) -> AlertOut:
    """查询设备当前告警状态与正在生效的规则。

    返回是否告警中（alerting）、累计进入/解除次数、最近驱动事件及最近一次转换时间。
    设备不存在返回 404；设备存在但从未配置告警规则返回 404（无规则即无状态机）。
    """
    with db.session() as conn:
        require_device(conn, device_id)
        rule = get_alert_rule(conn, device_id)
        if rule is None:
            raise HTTPException(
                status_code=404,
                detail=f"no alert rule configured for device: {device_id}",
            )
        return build_alert_out(rule, get_alert_state(conn, device_id))


@app.get("/devices/{device_id}/alert-episodes", response_model=AlertEpisodesOut)
def list_alert_episodes(
    device_id: str,
    start: datetime = Query(..., description="统计范围下界（含），ISO 8601，不带时区按 UTC 处理"),
    end: datetime = Query(..., description="统计范围上界（不含），ISO 8601，不带时区按 UTC 处理"),
) -> AlertEpisodesOut:
    """查询设备在半开区间 [start, end) 内的告警周期与总告警秒数（只读）。

    按告警转换流水的自增顺序把 entered / cleared 配对成周期，周期起止时刻取流水
    记录的处理时刻 created_at（迟到事件不按 event_time 重排）。只返回与 [start, end)
    相交的周期，逐条给出进入/解除时刻与裁剪后的区间内秒数；尚未解除的周期只计至
    end 与查询时刻的较早者。total_seconds 为各明细秒数之和。
    start/end 必填且 end > start，否则 422；设备或告警规则不存在返回 404；
    无相交周期返回空明细与 0。
    """
    start_ts = to_epoch(start)
    end_ts = to_epoch(end)
    if end_ts <= start_ts:
        raise HTTPException(status_code=422, detail="end must be strictly greater than start")

    now_ts = time.time()
    with db.session() as conn:
        require_device(conn, device_id)
        if get_alert_rule(conn, device_id) is None:
            raise HTTPException(
                status_code=404,
                detail=f"no alert rule configured for device: {device_id}",
            )
        rows = conn.execute(
            "SELECT kind, created_at FROM device_alert_transitions"
            " WHERE device_id = ? ORDER BY id",
            (device_id,),
        ).fetchall()

    # 按流水自增顺序配对：entered 开启一个周期，下一条 cleared 将其闭合。
    # 状态机保证 entered/cleared 严格交替，顺序配对即正确；迟到事件只按处理
    # 顺序追加流水，不按 event_time 重排。
    pairs: list[tuple[str, Optional[str]]] = []
    pending_entered: Optional[str] = None
    for r in rows:
        if r["kind"] == "entered":
            pending_entered = r["created_at"]
        elif pending_entered is not None:
            pairs.append((pending_entered, r["created_at"]))
            pending_entered = None
    if pending_entered is not None:
        pairs.append((pending_entered, None))

    episodes: list[AlertEpisodeOut] = []
    total_seconds = 0.0
    for entered_at, cleared_at in pairs:
        entered_ts = datetime.fromisoformat(entered_at).timestamp()
        if cleared_at is not None:
            episode_end_ts = datetime.fromisoformat(cleared_at).timestamp()
        else:
            # 尚未解除的周期只计至 end 与查询时刻的较早者
            episode_end_ts = min(now_ts, end_ts)
        clip_start = max(entered_ts, start_ts)
        clip_end = min(episode_end_ts, end_ts)
        seconds = clip_end - clip_start
        if seconds <= 0:
            # 与 [start, end) 无交集（半开区间，端点相接或零长周期都不算相交）
            continue
        episodes.append(
            AlertEpisodeOut(entered_at=entered_at, cleared_at=cleared_at, seconds=seconds)
        )
        total_seconds += seconds

    return AlertEpisodesOut(
        device_id=device_id,
        start=start,
        end=end,
        episodes=episodes,
        total_seconds=total_seconds,
    )


# ---------- 设备组接口 ----------

@app.post("/groups", response_model=GroupOut, status_code=201)
def create_group(req: GroupCreate) -> GroupOut:
    """创建固定成员设备组。

    成员设备必须均已存在，且窗口配置（window_size_seconds、allowed_lateness_seconds）
    与请求一致，保证同一 window_start 的成员窗口对齐、封存口径一致。

    成员存在性/配置校验、组与成员落盘、历史窗口全量补算全部在单个 BEGIN IMMEDIATE
    写锁事务内完成：校验通过到成员关系落盘之间不会有其他写事务插入，与同样持写锁的
    PUT /devices/{id}/config 串行，杜绝"校验时一致、入组时已漂移"的并发交错；
    校验失败（404/409）时整事务回滚，不留下半个组。
    """
    now = utcnow_iso()
    member_ids = req.member_ids
    if len(set(member_ids)) != len(member_ids):
        raise HTTPException(status_code=400, detail="member_ids must not contain duplicates")

    # BEGIN IMMEDIATE：读成员配置 → 写组/成员关系 → 全量补算全程持写锁
    with db.immediate_session() as conn:
        if conn.execute(
            "SELECT 1 FROM device_groups WHERE group_id = ?", (req.group_id,)
        ).fetchone():
            raise HTTPException(status_code=409, detail=f"group already exists: {req.group_id}")

        missing: list[str] = []
        mismatched: list[str] = []
        for device_id in member_ids:
            dev = get_device(conn, device_id)
            if dev is None:
                missing.append(device_id)
            elif (
                dev["window_size_seconds"] != req.window_size_seconds
                or dev["allowed_lateness_seconds"] != req.allowed_lateness_seconds
            ):
                mismatched.append(device_id)
        if missing:
            raise HTTPException(
                status_code=404, detail=f"devices not found: {', '.join(missing)}"
            )
        if mismatched:
            raise HTTPException(
                status_code=409,
                detail=(
                    "devices with window config different from group config: "
                    + ", ".join(mismatched)
                ),
            )

        conn.execute(
            "INSERT INTO device_groups (group_id, window_size_seconds, allowed_lateness_seconds,"
            " aggregation, created_at) VALUES (?, ?, ?, ?, ?)",
            (
                req.group_id,
                req.window_size_seconds,
                req.allowed_lateness_seconds,
                req.aggregation,
                now,
            ),
        )
        conn.executemany(
            "INSERT INTO device_group_members (group_id, device_id, position) VALUES (?, ?, ?)",
            [(req.group_id, device_id, i) for i, device_id in enumerate(member_ids)],
        )

        # 若成员已有封存窗口，组创建后立即对账补算
        reconcile_group(conn, req.group_id, now)

        return GroupOut(
            group_id=req.group_id,
            member_ids=member_ids,
            aggregation=req.aggregation,
            window_size_seconds=req.window_size_seconds,
            allowed_lateness_seconds=req.allowed_lateness_seconds,
            created_at=now,
        )


@app.get("/groups/{group_id}", response_model=GroupOut)
def get_group(group_id: str) -> GroupOut:
    with db.session() as conn:
        group = require_group(conn, group_id)
        return GroupOut(
            group_id=group["group_id"],
            member_ids=group_member_ids(conn, group_id),
            aggregation=group["aggregation"],
            window_size_seconds=group["window_size_seconds"],
            allowed_lateness_seconds=group["allowed_lateness_seconds"],
            created_at=group["created_at"],
        )


@app.get("/groups/{group_id}/windows", response_model=list[GroupWindowOut])
def list_group_windows(
    group_id: str,
    status: Optional[Literal["pending", "ready"]] = None,
    window_start: Optional[datetime] = None,
) -> list[GroupWindowOut]:
    """按窗口查询组汇总，可按 status / window_start 过滤。pending 窗口 value 为 null。"""
    with db.session() as conn:
        require_group(conn, group_id)
        sql = "SELECT * FROM group_windows WHERE group_id = ?"
        params: list = [group_id]
        if status:
            sql += " AND status = ?"
            params.append(status)
        if window_start is not None:
            sql += " AND window_start = ?"
            params.append(to_epoch(window_start))
        sql += " ORDER BY window_start"
        rows = conn.execute(sql, params).fetchall()
        return [group_window_out(r) for r in rows]


@app.get("/groups/{group_id}/windows/{window_start}", response_model=GroupWindowOut)
def get_group_window(group_id: str, window_start: datetime) -> GroupWindowOut:
    """查询组在单个 window_start 上的汇总（pending 或 ready）。"""
    with db.session() as conn:
        require_group(conn, group_id)
        row = conn.execute(
            "SELECT * FROM group_windows WHERE group_id = ? AND window_start = ?",
            (group_id, to_epoch(window_start)),
        ).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="group window not found (no member has data yet)")
        return group_window_out(row)


@app.get(
    "/groups/{group_id}/windows/{window_start}/pending",
    response_model=list[PendingMemberOut],
)
def list_pending_members(group_id: str, window_start: datetime) -> list[PendingMemberOut]:
    """返回该组窗口尚未封存的成员明细。

    - 组窗口已 ready：返回空列表（无缺失成员）。
    - 组窗口尚不存在（成员均无数据）：404。
    - 成员在该 window_start 没有窗口：reason=missing；窗口仍 open：reason=open。
    """
    w_start = to_epoch(window_start)
    with db.session() as conn:
        group = require_group(conn, group_id)
        gw = conn.execute(
            "SELECT * FROM group_windows WHERE group_id = ? AND window_start = ?",
            (group_id, w_start),
        ).fetchone()
        if gw is None:
            raise HTTPException(status_code=404, detail="group window not found (no member has data yet)")
        if gw["status"] == "ready":
            return []

        w_end = w_start + group["window_size_seconds"]
        members = group_member_ids(conn, group_id)
        placeholders_sql = placeholders(len(members))
        win_rows = {
            r["device_id"]: r
            for r in conn.execute(
                f"SELECT * FROM windows WHERE window_start = ? AND device_id IN ({placeholders_sql})",
                [w_start, *members],
            ).fetchall()
        }

        result: list[PendingMemberOut] = []
        for device_id in members:
            w = win_rows.get(device_id)
            if w is None:
                result.append(PendingMemberOut(
                    device_id=device_id,
                    window_start=None,
                    window_end=to_iso(w_end),
                    window_status=None,
                    reason="missing",
                ))
            elif w["status"] != "sealed":
                result.append(PendingMemberOut(
                    device_id=device_id,
                    window_start=to_iso(w["window_start"]),
                    window_end=to_iso(w["window_end"]),
                    window_status=w["status"],
                    reason="open",
                ))
        return result


# ---------- 组窗口修订审计接口 ----------

def _require_group_window(conn: sqlite3.Connection, group_id: str, w_start: float) -> sqlite3.Row:
    require_group(conn, group_id)
    gw = conn.execute(
        "SELECT * FROM group_windows WHERE group_id = ? AND window_start = ?",
        (group_id, w_start),
    ).fetchone()
    if gw is None:
        raise HTTPException(status_code=404, detail="group window not found (no member has data yet)")
    return gw


def _require_snapshot(
    conn: sqlite3.Connection, group_id: str, w_start: float, revision: int
) -> sqlite3.Row:
    snap = conn.execute(
        "SELECT * FROM group_window_snapshots WHERE group_id = ? AND window_start = ? AND revision = ?",
        (group_id, w_start, revision),
    ).fetchone()
    if snap is None:
        raise HTTPException(
            status_code=404, detail=f"revision not found: {revision}"
        )
    return snap


def _snapshot_member_states(
    conn: sqlite3.Connection, aggregation: str, snapshot_id: int
) -> dict[str, dict]:
    """读取某份快照的成员明细，并算出各成员当时对 group value 的贡献。"""
    rows = conn.execute(
        "SELECT * FROM group_window_snapshot_members WHERE snapshot_id = ? ORDER BY position",
        (snapshot_id,),
    ).fetchall()
    states: dict[str, dict] = {}
    for r in rows:
        contribution = r["sum"] if aggregation == "sum" else (r["avg"] if r["avg"] is not None else 0.0)
        states[r["device_id"]] = {
            "device_id": r["device_id"],
            "position": r["position"],
            "count": r["count"],
            "sum": r["sum"],
            "avg": r["avg"],
            "correction_count": r["correction_count"],
            "contribution": contribution,
        }
    return states


@app.get(
    "/groups/{group_id}/windows/{window_start}/revisions",
    response_model=list[GroupWindowRevisionOut],
)
def list_group_window_revisions(group_id: str, window_start: datetime) -> list[GroupWindowRevisionOut]:
    """返回组窗口的修订审计列表（不可变快照），按 revision 升序。

    组窗口存在但尚未 ready（无任何修订）时返回空列表；组或窗口不存在返回 404。
    """
    w_start = to_epoch(window_start)
    with db.session() as conn:
        _require_group_window(conn, group_id, w_start)
        rows = conn.execute(
            "SELECT s.*, ("
            "  SELECT COUNT(*) FROM group_window_snapshot_members m WHERE m.snapshot_id = s.id"
            " ) AS member_count"
            " FROM group_window_snapshots s"
            " WHERE s.group_id = ? AND s.window_start = ?"
            " ORDER BY s.revision",
            (group_id, w_start),
        ).fetchall()
        return [
            GroupWindowRevisionOut(
                revision=r["revision"],
                value=r["value"],
                kind=r["kind"],
                created_at=r["created_at"],
                member_count=r["member_count"],
            )
            for r in rows
        ]


@app.get(
    "/groups/{group_id}/windows/{window_start}/revisions/diff",
    response_model=RevisionDiffOut,
)
def diff_group_window_revisions(
    group_id: str,
    window_start: datetime,
    from_revision: int = Query(..., ge=1),
    to_revision: int = Query(..., ge=1),
) -> RevisionDiffOut:
    """对比组窗口两个 revision 的不可变快照。

    按成员顺序返回每个发生变化成员的前后贡献与各数值差（count/sum/avg/correction_count），
    仅列变化成员。from/to 必须为 >=1 且 from < to，否则 422；
    组、窗口或任一 revision 不存在返回 404。
    """
    if from_revision >= to_revision:
        raise HTTPException(
            status_code=422, detail="from_revision must be a positive revision smaller than to_revision"
        )

    w_start = to_epoch(window_start)
    with db.session() as conn:
        _require_group_window(conn, group_id, w_start)
        group = conn.execute(
            "SELECT aggregation FROM device_groups WHERE group_id = ?", (group_id,)
        ).fetchone()
        snap_from = _require_snapshot(conn, group_id, w_start, from_revision)
        snap_to = _require_snapshot(conn, group_id, w_start, to_revision)

        before_states = _snapshot_member_states(conn, group["aggregation"], snap_from["id"])
        after_states = _snapshot_member_states(conn, group["aggregation"], snap_to["id"])

        member_deltas: list[RevisionMemberDeltaOut] = []
        for device_id in sorted(set(before_states) | set(after_states),
                                key=lambda d: after_states.get(d, before_states[d])["position"]):
            b = before_states.get(device_id)
            a = after_states.get(device_id)
            if b is None or a is None or (
                b["count"], b["sum"], b["avg"], b["correction_count"]
            ) != (
                a["count"], a["sum"], a["avg"], a["correction_count"]
            ):
                # 固定成员组两侧成员集合必然相同；任一缺失视为变化（防御性处理）
                b = b or a
                a = a or b
                member_deltas.append(RevisionMemberDeltaOut(
                    device_id=device_id,
                    position=a["position"],
                    before=RevisionMemberStateOut(**b),
                    after=RevisionMemberStateOut(**a),
                    count_delta=a["count"] - b["count"],
                    sum_delta=a["sum"] - b["sum"],
                    avg_delta=(
                        a["avg"] - b["avg"]
                        if a["avg"] is not None and b["avg"] is not None
                        else None
                    ),
                    correction_count_delta=a["correction_count"] - b["correction_count"],
                    contribution_delta=a["contribution"] - b["contribution"],
                ))

        return RevisionDiffOut(
            group_id=group_id,
            window_start=to_iso(w_start),
            aggregation=group["aggregation"],
            from_revision=from_revision,
            to_revision=to_revision,
            from_value=snap_from["value"],
            to_value=snap_to["value"],
            value_delta=snap_to["value"] - snap_from["value"],
            members=member_deltas,
        )
