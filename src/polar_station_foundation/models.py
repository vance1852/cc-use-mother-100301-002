"""定义基础服务在模块边界使用的数据对象。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Actor:
    """表示具有明确角色的后台操作者。"""

    actor_id: str
    display_name: str
    role: str
    organization_id: str
    active: bool


@dataclass(frozen=True)
class Site:
    """表示科考运行机构下的业务场所。"""

    site_id: str
    organization_id: str
    name: str
    timezone_name: str
    version: int


@dataclass(frozen=True)
class DomainRecord:
    """表示已经持久化的领域资料记录。"""

    record_id: str
    site_id: str
    category: str
    external_key: str
    payload: dict[str, Any]
    created_by: str
    created_at: str


@dataclass(frozen=True)
class WriteReceipt:
    """描述一次幂等写入的稳定结果。"""

    request_id: str
    resource_type: str
    resource_id: str
    replayed: bool


@dataclass(frozen=True)
class Container:
    """样品容器谱系节点的当前投影。"""

    container_id: str
    plan_id: str | None
    site_id: str
    kind: str
    status: str
    holder_actor_id: str | None
    holder_location: str | None
    seal_id: str | None
    sealed_by: str | None
    mass_grams: float | None
    version: int
    temp_excursion: bool
    seal_anomaly: bool
    last_device_id: str | None
    source_event_id: str
    parents: tuple[str, ...]
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class CustodyEvent:
    """不可变的监管链事件（载荷为当时版本快照）。"""

    event_id: str
    device_id: str | None
    local_sequence: int | None
    event_type: str
    actor_id: str
    payload: dict[str, Any]
    expected_version: str | None
    occurred_at: str
    recorded_at: str
    plan_id: str | None
    analysis_id: str | None
    containers: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class Analysis:
    """派生分析结果及其溯源入口。"""

    analysis_id: str
    container_id: str
    method: str
    result: dict[str, Any]
    instrument: dict[str, Any] | None
    event_id: str
    created_by: str
    created_at: str


@dataclass(frozen=True)
class QuarantineCase:
    """等待管理员裁决的异常事件。"""

    case_id: str
    device_id: str | None
    local_sequence: int | None
    request_id: str | None
    event_type: str
    envelope: dict[str, Any]
    actor_id: str | None
    occurred_at: str | None
    reason_code: str
    reason_detail: str
    status: str
    created_event_id: str | None
    decided_by: str | None
    decided_at: str | None
    decision_note: str | None
    applied_event_id: str | None
    created_at: str


@dataclass(frozen=True)
class SubmitResult:
    """现场事件提交的处理结果：已应用或已隔离。"""

    accepted: bool
    replayed: bool
    event_id: str | None
    case_id: str | None
    reason_code: str | None
    reason_detail: str | None
