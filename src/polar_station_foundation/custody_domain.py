"""定义样品监管链允许的事件类型与状态规则。"""

from __future__ import annotations

# 采样计划派生原始岩芯；其余事件都沿父子关系推进。
PLANNED_EVENTS = frozenset({
    "core_generated",
    "aliquot",
    "merge",
    "weigh",
    "seal",
    "open",
    "transfer",
    "loan",
    "return",
    "consume",
    "environment",
    "analysis",
})

# 终态后不得再追加业务事件（隔离裁决除外）。
TERMINAL_STATUSES = frozenset({"consumed", "destroyed"})

# 会打开容器的事件：与封签状态和角色权限联合校验是否越权开封。
OPENING_EVENTS = frozenset({"open", "aliquot", "merge", "consume", "analysis"})

# 隔离裁决原因代码。
QUARANTINE_REASONS = frozenset({
    "clock_regression",
    "sequence_gap",
    "unauthorized_open",
    "seal_mismatch",
    "unknown_container",
    "unknown_parent",
    "parent_not_generated",
    "terminal_container",
    "state_conflict",
    "mass_inconsistency",
    "validation_error",
    "duplicate_mismatch",
})
