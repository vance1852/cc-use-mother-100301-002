"""定义样品监管链服务在模块边界使用的数据对象。"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class EventReceipt:
    """描述一次监管事件提交的稳定处理结果。

    status 为 confirmed 或 quarantined；quarantined 时 case_id 指向隔离案件。
    重复上传同一事件时返回同一 event_id 与当前状态，replayed 为 True。
    """

    event_id: str
    status: str
    case_id: str | None
    replayed: bool
