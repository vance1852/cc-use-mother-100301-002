"""样品监管链领域服务：容器谱系、监管事件、离线同步、隔离裁决与数量核对。

设计要点：
- 采样计划登记时生成根容器，分装/合并/派生沿父子关系推进；
- 每个事件必须声明所触容器的基准版本（base_versions），版本不符即"无法接续"；
- 称量器具、封签、人员、校准依据、环境记录以版本化依据文档保存，
  事件引用时把当时内容快照写入事件载荷，事后更新依据不影响历史；
- 野外设备按 (device_id, local_sequence) 幂等提交历史事件，重复上传返回原处理结果；
- 时钟倒退、越权开封、前后矛盾、无法接续的事件进入隔离裁决，
  裁决只能追加仍适用的事件或驳回，绝不能覆盖已确认历史。
"""

from __future__ import annotations

import json
import math
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from polar_station_foundation.audit import append_event, canonical_json, digest
from polar_station_foundation.clock import Clock, SystemClock
from polar_station_foundation.errors import (
    ConflictError,
    NotFoundError,
    PermissionDenied,
    ValidationError,
)
from polar_station_foundation.models import Actor, WriteReceipt
from polar_station_foundation.service import IDENTIFIER
from polar_station_foundation.storage import Database

from .models import EventReceipt


SUBMIT_ROLES = frozenset({"admin", "operator"})
ADJUDICATE_ROLES = frozenset({"admin", "reviewer"})
REFERENCE_CATEGORIES = frozenset({"weighing_scale", "seal", "personnel", "calibration", "environment"})
SUBMITTABLE_TYPES = frozenset({
    "split", "merge", "lend", "return", "consume", "derive",
    "seal", "unseal", "transfer", "weigh", "environment",
})
EPSILON = 1e-9
WEIGH_TOLERANCE_RELATIVE = 0.005
FUTURE_SKEW_SECONDS = 300

REASON_CLOCK_ROLLBACK = "clock_rollback"
REASON_CLOCK_ANOMALY = "clock_anomaly"
REASON_UNAUTHORIZED_UNSEAL = "unauthorized_unseal"
REASON_UNAUTHORIZED = "unauthorized"
REASON_CONTRADICTION = "contradiction"
REASON_DISCONTINUITY = "discontinuity"
REASON_CONFLICTING_REUPLOAD = "conflicting_reupload"


class QuarantineRequired(Exception):
    """表示事件不能直接并入已确认历史，必须进入隔离裁决。"""

    def __init__(self, reason: str, message: str, detail: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.reason = reason
        self.message = message
        self.detail = detail or {}


def _intended_subject(event_type: str, detail: dict[str, Any]) -> str | None:
    """在不校验内容的前提下推断事件针对的容器，用于隔离分诊。"""

    if event_type in {"lend", "return", "consume", "seal", "unseal", "transfer", "weigh", "environment"}:
        value = detail.get("container_id")
    elif event_type in {"split", "derive"}:
        value = detail.get("parent_id")
    elif event_type == "merge":
        sources = detail.get("source_ids")
        value = sources[0] if isinstance(sources, list) and sources else None
    else:
        value = None
    return value if isinstance(value, str) and value else None


class CustodyService:
    """协调样品监管链的权限、幂等、事务、隔离和审计规则。"""

    def __init__(self, database: Database, clock: Clock | None = None) -> None:
        self.database = database
        self.clock = clock or SystemClock()

    # ------------------------------------------------------------------
    # 基础工具
    # ------------------------------------------------------------------

    def _now(self) -> str:
        return self.clock.now().astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")

    def _parse_time(self, value: Any, field: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValidationError(f"{field} 必须是 ISO 8601 时间字符串")
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValidationError(f"{field} 不是合法的 ISO 8601 时间") from exc
        if parsed.tzinfo is None:
            raise ValidationError(f"{field} 必须包含时区")
        return parsed.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")

    def _identifier(self, value: Any, field: str) -> str:
        if not isinstance(value, str):
            raise ValidationError(f"{field} 格式无效")
        value = value.strip()
        if not IDENTIFIER.fullmatch(value):
            raise ValidationError(f"{field} 格式无效")
        return value

    def _text(self, value: Any, field: str, limit: int = 200) -> str:
        if not isinstance(value, str):
            raise ValidationError(f"{field} 必须是字符串")
        value = value.strip()
        if not value or len(value) > limit:
            raise ValidationError(f"{field} 不能为空且不能超过 {limit} 个字符")
        return value

    def _optional_text(self, value: Any, field: str, limit: int = 200) -> str | None:
        if value is None:
            return None
        return self._text(value, field, limit)

    def _quantity(self, value: Any, field: str, *, allow_zero: bool = False) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
            raise ValidationError(f"{field} 必须是有限数值")
        quantity = float(value)
        if quantity < 0 or (quantity <= 0 and not allow_zero):
            raise ValidationError(f"{field} 必须大于零" if not allow_zero else f"{field} 不能为负")
        return quantity

    def _temperature(self, value: Any, field: str) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
            raise ValidationError(f"{field} 必须是有限数值")
        return float(value)

    def _actor(self, connection, actor_id: str) -> Actor:
        row = connection.execute("SELECT * FROM actors WHERE actor_id=?", (actor_id,)).fetchone()
        if row is None:
            raise NotFoundError("操作者不存在")
        actor = Actor(row["actor_id"], row["display_name"], row["role"], row["organization_id"], bool(row["active"]))
        if not actor.active:
            raise PermissionDenied("操作者已停用")
        return actor

    def _require_role(self, actor: Actor, roles: frozenset[str]) -> None:
        if actor.role not in roles:
            raise PermissionDenied("当前角色不能执行该动作")

    def _site(self, connection, site_id: Any):
        if not isinstance(site_id, str) or not site_id:
            raise ValidationError("site_id 不能为空")
        row = connection.execute("SELECT * FROM sites WHERE site_id=?", (site_id,)).fetchone()
        if row is None:
            raise NotFoundError("场所不存在")
        return row

    def _check_org(self, actor: Actor, site) -> None:
        if actor.role != "admin" and actor.organization_id != site["organization_id"]:
            raise PermissionDenied("不能在其他组织的场所操作")

    def _party(self, connection, actor_id: str) -> dict[str, Any] | None:
        row = connection.execute("SELECT * FROM actors WHERE actor_id=?", (actor_id,)).fetchone()
        if row is None:
            return None
        return {"actor_id": row["actor_id"], "display_name": row["display_name"], "role": row["role"]}

    def _active_party(self, connection, actor_id: Any, field: str) -> dict[str, Any]:
        """返回在职人员的当时快照；人员缺失或停用属于数据矛盾。"""

        if not isinstance(actor_id, str) or not actor_id:
            raise ValidationError(f"{field}编号不能为空")
        snapshot = self._party(connection, actor_id)
        row = connection.execute("SELECT active FROM actors WHERE actor_id=?", (actor_id,)).fetchone()
        if snapshot is None or not row["active"]:
            raise QuarantineRequired(REASON_CONTRADICTION, f"{field} {actor_id} 不存在或已停用",
                                     {"actor_id": actor_id})
        return snapshot

    @staticmethod
    def _party_of(actor: Actor) -> dict[str, Any]:
        return {"actor_id": actor.actor_id, "display_name": actor.display_name, "role": actor.role}

    def _idempotent(self, connection, *, request_id: str, action: str,
                    payload: dict[str, Any], create) -> tuple[WriteReceipt, dict[str, Any]]:
        request_id = self._identifier(request_id, "request_id")
        payload_hash = digest(payload)
        row = connection.execute("SELECT * FROM request_receipts WHERE request_id=?", (request_id,)).fetchone()
        if row:
            if row["action"] != action or row["payload_hash"] != payload_hash:
                raise ConflictError("request_id 已被不同内容使用")
            return WriteReceipt(request_id, row["resource_type"], row["resource_id"], True), json.loads(row["response_json"])
        resource_type, resource_id, response = create()
        connection.execute(
            "INSERT INTO request_receipts(request_id,action,payload_hash,resource_type,resource_id,response_json,created_at) "
            "VALUES(?,?,?,?,?,?,?)",
            (request_id, action, payload_hash, resource_type, resource_id, canonical_json(response), self._now()),
        )
        return WriteReceipt(request_id, resource_type, resource_id, False), response

    # ------------------------------------------------------------------
    # 在线登记：设备、依据文档、采样计划
    # ------------------------------------------------------------------

    def register_device(self, *, request_id: str, actor_id: str, device_id: str,
                        site_id: str, label: str) -> WriteReceipt:
        payload = {"actor_id": actor_id, "device_id": device_id, "site_id": site_id, "label": label}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require_role(actor, SUBMIT_ROLES)
            site = self._site(connection, site_id)
            self._check_org(actor, site)
            device_id = self._identifier(device_id, "device_id")
            label = self._text(label, "label")

            def create() -> tuple[str, str, dict[str, Any]]:
                try:
                    connection.execute(
                        "INSERT INTO field_devices(device_id,site_id,label,registered_by,registered_at) VALUES(?,?,?,?,?)",
                        (device_id, site_id, label, actor_id, self._now()),
                    )
                except Exception as exc:
                    raise ConflictError("设备编号已经存在") from exc
                append_event(connection, actor_id=actor_id, action="custody.device.registered",
                             resource_type="field_device", resource_id=device_id,
                             detail={"site_id": site_id, "label": label}, occurred_at=self._now())
                return "field_device", device_id, {"device_id": device_id}

            receipt, _ = self._idempotent(connection, request_id=request_id,
                                          action="custody.register_device", payload=payload, create=create)
            return receipt

    def register_reference(self, *, request_id: str, actor_id: str, doc_id: str, category: str,
                           site_id: str, content: dict[str, Any]) -> WriteReceipt:
        """登记依据文档的新版本；旧版本永久保留，事件引用哪个版本就快照哪个版本。"""

        if not isinstance(content, dict) or not content:
            raise ValidationError("content 必须是非空对象")
        payload = {"actor_id": actor_id, "doc_id": doc_id, "category": category,
                   "site_id": site_id, "content": content}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require_role(actor, SUBMIT_ROLES)
            site = self._site(connection, site_id)
            self._check_org(actor, site)
            doc_id = self._identifier(doc_id, "doc_id")
            if category not in REFERENCE_CATEGORIES:
                raise ValidationError("依据类别不在允许范围内")

            def create() -> tuple[str, str, dict[str, Any]]:
                row = connection.execute(
                    "SELECT COALESCE(MAX(version), 0) + 1 AS next_version FROM reference_docs WHERE doc_id=?",
                    (doc_id,),
                ).fetchone()
                version = row["next_version"]
                connection.execute(
                    "INSERT INTO reference_docs(doc_id,version,category,site_id,content_json,registered_by,registered_at) "
                    "VALUES(?,?,?,?,?,?,?)",
                    (doc_id, version, category, site_id, canonical_json(content), actor_id, self._now()),
                )
                append_event(connection, actor_id=actor_id, action="custody.reference.registered",
                             resource_type="reference_doc", resource_id=f"{doc_id}@{version}",
                             detail={"doc_id": doc_id, "version": version, "category": category,
                                     "content_hash": digest(content)}, occurred_at=self._now())
                return "reference_doc", f"{doc_id}@{version}", {"doc_id": doc_id, "version": version}

            receipt, _ = self._idempotent(connection, request_id=request_id,
                                          action="custody.register_reference", payload=payload, create=create)
            return receipt

    def register_plan(self, *, request_id: str, actor_id: str, plan_id: str, site_id: str,
                      title: str, unit: str, containers: list[dict[str, Any]],
                      max_temperature_c: float | None = None, description: str = "") -> WriteReceipt:
        """登记采样计划并生成根容器谱系；每个根容器产生一条 register 监管事件。"""

        if not isinstance(containers, list) or not containers:
            raise ValidationError("containers 必须是非空数组")
        payload = {"actor_id": actor_id, "plan_id": plan_id, "site_id": site_id, "title": title,
                   "unit": unit, "containers": containers, "max_temperature_c": max_temperature_c,
                   "description": description}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require_role(actor, SUBMIT_ROLES)
            site = self._site(connection, site_id)
            self._check_org(actor, site)
            plan_id = self._identifier(plan_id, "plan_id")
            title = self._text(title, "title")
            unit = self._text(unit, "unit", 20)
            description = self._text(description, "description", 500) if description else ""
            if max_temperature_c is not None:
                max_temperature_c = self._temperature(max_temperature_c, "max_temperature_c")
            specs = []
            seen: set[str] = set()
            for item in containers:
                if not isinstance(item, dict):
                    raise ValidationError("containers 元素必须是对象")
                container_id = self._identifier(item.get("container_id", ""), "containers.container_id")
                if container_id in seen:
                    raise ValidationError("容器编号重复")
                seen.add(container_id)
                custodian_id = item.get("custodian_id")
                if not isinstance(custodian_id, str) or not custodian_id:
                    raise ValidationError("containers.custodian_id 不能为空")
                custodian = self._party(connection, custodian_id)
                active = connection.execute("SELECT active FROM actors WHERE actor_id=?", (custodian_id,)).fetchone()
                if custodian is None or not active["active"]:
                    raise ValidationError(f"保管人 {custodian_id} 不存在或已停用")
                specs.append({
                    "container_id": container_id,
                    "label": self._text(item.get("label", container_id), "containers.label"),
                    "quantity": self._quantity(item.get("quantity"), "containers.quantity"),
                    "custodian_id": custodian_id,
                    "custodian": custodian,
                    "location": self._text(item.get("location", ""), "containers.location"),
                    "seal_id": self._optional_text(item.get("seal_id"), "containers.seal_id", 120),
                })

            def create() -> tuple[str, str, dict[str, Any]]:
                try:
                    connection.execute(
                        "INSERT INTO sampling_plans(plan_id,site_id,title,description,unit,max_temperature_c,created_by,created_at) "
                        "VALUES(?,?,?,?,?,?,?,?)",
                        (plan_id, site_id, title, description, unit, max_temperature_c, actor_id, self._now()),
                    )
                except Exception as exc:
                    raise ConflictError("采样计划编号已经存在") from exc
                now = self._now()
                for spec in specs:
                    event_id = uuid.uuid4().hex
                    envelope = {
                        "event_type": "register", "site_id": site_id, "device_id": None,
                        "local_sequence": None, "occurred_at": now, "submitted_by": actor_id,
                    }
                    event_payload = {
                        **envelope,
                        "detail": {"plan_id": plan_id, "container_id": spec["container_id"],
                                   "label": spec["label"], "quantity": spec["quantity"],
                                   "location": spec["location"], "seal_id": spec["seal_id"]},
                        "base_versions": {}, "references": [],
                        "parties": {"submitter": self._party_of(actor), "custodian": spec["custodian"]},
                        "extra": {},
                    }
                    self._insert_event(connection, event_id=event_id, envelope=envelope,
                                       payload_hash=digest(event_payload["detail"]),
                                       payload=event_payload, status="confirmed",
                                       subject=spec["container_id"], recorded_at=now, temperature=None)
                    self._insert_container(connection, container_id=spec["container_id"], plan_id=plan_id,
                                           kind="root", label=spec["label"], quantity=spec["quantity"],
                                           unit=unit, custodian_id=spec["custodian_id"],
                                           location=spec["location"], seal_id=spec["seal_id"],
                                           result_id=None, head_event_id=event_id, created_at=now)
                    self._insert_effect(connection, event_id, spec["container_id"], "subject",
                                        None, spec["quantity"], 1)
                    append_event(connection, actor_id=actor_id, action="custody.event.register",
                                 resource_type="custody_event", resource_id=event_id,
                                 detail={"event_type": "register", "subject": spec["container_id"],
                                         "site_id": site_id}, occurred_at=now)
                append_event(connection, actor_id=actor_id, action="custody.plan.registered",
                             resource_type="sampling_plan", resource_id=plan_id,
                             detail={"site_id": site_id, "title": title, "unit": unit,
                                     "containers": [spec["container_id"] for spec in specs]},
                             occurred_at=now)
                return "sampling_plan", plan_id, {"plan_id": plan_id,
                                                  "containers": [spec["container_id"] for spec in specs]}

            receipt, _ = self._idempotent(connection, request_id=request_id,
                                          action="custody.register_plan", payload=payload, create=create)
            return receipt

    # ------------------------------------------------------------------
    # 事件入口：在线提交与设备离线补录共用同一条验证管线
    # ------------------------------------------------------------------

    def submit_event(self, *, actor_id: str, site_id: str, event_type: str,
                     detail: dict[str, Any], device_id: str | None = None,
                     local_sequence: int | None = None, request_id: str | None = None,
                     occurred_at: str | None = None,
                     base_versions: dict[str, int] | None = None,
                     references: list[dict[str, Any]] | None = None) -> EventReceipt:
        base_versions = base_versions if base_versions is not None else {}
        references = references if references is not None else []
        if event_type not in SUBMITTABLE_TYPES:
            raise ValidationError("event_type 不受支持")
        if not isinstance(detail, dict):
            raise ValidationError("detail 必须是对象")
        if not isinstance(base_versions, dict) or not all(
                isinstance(key, str) and isinstance(value, int) and not isinstance(value, bool) and value >= 1
                for key, value in base_versions.items()):
            raise ValidationError("base_versions 必须是 {容器编号: 正整数版本}")
        normalized_references = []
        for reference in references:
            if not isinstance(reference, dict):
                raise ValidationError("references 元素必须是对象")
            doc_id = reference.get("doc_id")
            version = reference.get("version")
            if not isinstance(doc_id, str) or not doc_id:
                raise ValidationError("references.doc_id 不能为空")
            if not isinstance(version, int) or isinstance(version, bool) or version < 1:
                raise ValidationError("references.version 必须是正整数")
            normalized_references.append({"doc_id": doc_id, "version": version})
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require_role(actor, SUBMIT_ROLES)
            site = self._site(connection, site_id)
            self._check_org(actor, site)
            envelope: dict[str, Any] = {
                "submitted_by": actor_id, "site_id": site_id, "event_type": event_type,
                "detail": detail, "base_versions": base_versions,
                "references": normalized_references,
            }
            if device_id is not None:
                if request_id is not None:
                    raise ValidationError("设备事件使用本地序列，不应携带 request_id")
                device_id = self._identifier(device_id, "device_id")
                device = connection.execute(
                    "SELECT * FROM field_devices WHERE device_id=?", (device_id,)).fetchone()
                if device is None:
                    raise NotFoundError("设备未登记")
                if device["site_id"] != site_id:
                    raise ValidationError("设备不属于该站点")
                if not isinstance(local_sequence, int) or isinstance(local_sequence, bool) or local_sequence < 1:
                    raise ValidationError("local_sequence 必须是正整数")
                if occurred_at is None:
                    raise ValidationError("设备事件必须携带 occurred_at")
                envelope["device_id"] = device_id
                envelope["local_sequence"] = local_sequence
                envelope["occurred_at"] = self._parse_time(occurred_at, "occurred_at")
                payload_hash = digest(envelope)
                existing = connection.execute(
                    "SELECT * FROM custody_events WHERE device_id=? AND local_sequence=?",
                    (device_id, local_sequence)).fetchone()
                if existing is not None:
                    if existing["payload_hash"] == payload_hash:
                        return self._receipt_for(connection, existing, replayed=True)
                    case_id = self._open_case(
                        connection, event_id=existing["event_id"],
                        reason=REASON_CONFLICTING_REUPLOAD,
                        detail={"message": "同一设备序列号上传了不同内容",
                                "confirmed_hash": existing["payload_hash"],
                                "received_hash": payload_hash, "received": envelope})
                    append_event(connection, actor_id=actor_id, action="custody.event.quarantined",
                                 resource_type="custody_event", resource_id=existing["event_id"],
                                 detail={"event_type": event_type, "reason": REASON_CONFLICTING_REUPLOAD},
                                 occurred_at=self._now())
                    return EventReceipt(existing["event_id"], "quarantined", case_id, False)
                return self._intake(connection, actor=actor, envelope=envelope,
                                    payload_hash=payload_hash, device=device)
            if local_sequence is not None:
                raise ValidationError("在线事件不应携带 local_sequence")
            if request_id is None:
                raise ValidationError("在线事件必须携带 request_id")
            envelope["device_id"] = None
            envelope["local_sequence"] = None
            envelope["occurred_at"] = (self._parse_time(occurred_at, "occurred_at")
                                       if occurred_at is not None else self._now())
            payload_hash = digest(envelope)

            def create() -> tuple[str, str, dict[str, Any]]:
                receipt = self._intake(connection, actor=actor, envelope=envelope,
                                       payload_hash=payload_hash, device=None)
                return ("custody_event", receipt.event_id,
                        {"event_id": receipt.event_id, "status": receipt.status, "case_id": receipt.case_id})

            write_receipt, _ = self._idempotent(connection, request_id=request_id,
                                                action="custody.submit_event", payload=envelope, create=create)
            event_row = connection.execute(
                "SELECT * FROM custody_events WHERE event_id=?", (write_receipt.resource_id,)).fetchone()
            return self._receipt_for(connection, event_row, replayed=write_receipt.replayed)

    def _receipt_for(self, connection, event_row, *, replayed: bool) -> EventReceipt:
        case_id = None
        if event_row["status"] == "quarantined":
            case = connection.execute(
                "SELECT case_id FROM quarantine_cases WHERE event_id=? ORDER BY rowid DESC LIMIT 1",
                (event_row["event_id"],)).fetchone()
            case_id = case["case_id"] if case else None
        return EventReceipt(event_row["event_id"], event_row["status"], case_id, replayed)

    def _intake(self, connection, *, actor: Actor, envelope: dict[str, Any],
                payload_hash: str, device) -> EventReceipt:
        """验证并接收一条事件；域内冲突不报错，而是落为隔离案件。"""

        event_id = uuid.uuid4().hex
        recorded_at = self._now()
        event_type = envelope["event_type"]
        try:
            if device is not None:
                self._check_device_clock(connection, device_id=device["device_id"], envelope=envelope)
            outcome = self._apply(connection, actor=actor, event_id=event_id, envelope=envelope)
        except QuarantineRequired as hold:
            subject = _intended_subject(event_type, envelope["detail"])
            self._insert_event(connection, event_id=event_id, envelope=envelope, payload_hash=payload_hash,
                               payload={"envelope": envelope}, status="quarantined", subject=subject,
                               recorded_at=recorded_at, temperature=self._maybe_temperature(envelope))
            case_id = self._open_case(connection, event_id=event_id, reason=hold.reason,
                                      detail={"message": hold.message, **hold.detail})
            append_event(connection, actor_id=actor.actor_id, action="custody.event.quarantined",
                         resource_type="custody_event", resource_id=event_id,
                         detail={"event_type": event_type, "reason": hold.reason, "subject": subject},
                         occurred_at=recorded_at)
            return EventReceipt(event_id, "quarantined", case_id, False)
        payload = {
            **envelope,
            "references": outcome["references"],
            "parties": outcome["parties"],
            "extra": outcome["extra"],
        }
        self._insert_event(connection, event_id=event_id, envelope=envelope, payload_hash=payload_hash,
                           payload=payload, status="confirmed", subject=outcome["subject"],
                           recorded_at=recorded_at, temperature=outcome["temperature"])
        for effect in outcome["effects"]:
            self._insert_effect(connection, event_id, effect["container_id"], effect["role"],
                                effect["quantity_before"], effect["quantity_after"], effect["container_version"])
        append_event(connection, actor_id=actor.actor_id, action=f"custody.event.{event_type}",
                     resource_type="custody_event", resource_id=event_id,
                     detail={"event_type": event_type, "subject": outcome["subject"],
                             "site_id": envelope["site_id"]}, occurred_at=recorded_at)
        return EventReceipt(event_id, "confirmed", None, False)

    def _insert_event(self, connection, *, event_id: str, envelope: dict[str, Any], payload_hash: str,
                      payload: dict[str, Any], status: str, subject: str | None,
                      recorded_at: str, temperature: float | None) -> None:
        connection.execute(
            "INSERT INTO custody_events(event_id,device_id,local_sequence,event_type,site_id,subject_container_id,"
            "payload_json,payload_hash,occurred_at,recorded_at,submitted_by,status,temperature_c) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (event_id, envelope["device_id"], envelope["local_sequence"], envelope["event_type"],
             envelope["site_id"], subject, canonical_json(payload), payload_hash,
             envelope["occurred_at"], recorded_at, envelope["submitted_by"], status, temperature),
        )

    def _insert_effect(self, connection, event_id: str, container_id: str, role: str,
                       before: float | None, after: float | None, version: int) -> None:
        connection.execute(
            "INSERT INTO event_effects(event_id,container_id,role,quantity_before,quantity_after,container_version) "
            "VALUES(?,?,?,?,?,?)",
            (event_id, container_id, role, before, after, version),
        )

    def _insert_container(self, connection, *, container_id: str, plan_id: str, kind: str, label: str,
                          quantity: float, unit: str, custodian_id: str, location: str,
                          seal_id: str | None, result_id: str | None,
                          head_event_id: str, created_at: str) -> None:
        connection.execute(
            "INSERT INTO containers(container_id,plan_id,kind,label,quantity,unit,status,custodian_id,location,"
            "seal_id,result_id,head_event_id,state_version,created_at) "
            "VALUES(?,?,?,?,?,?,'active',?,?,?,?,?,1,?)",
            (container_id, plan_id, kind, label, quantity, unit, custodian_id, location,
             seal_id, result_id, head_event_id, created_at),
        )

    def _open_case(self, connection, *, event_id: str, reason: str, detail: dict[str, Any]) -> str:
        case_id = uuid.uuid4().hex
        connection.execute(
            "INSERT INTO quarantine_cases(case_id,event_id,reason,detail_json,status) VALUES(?,?,?,?,'pending')",
            (case_id, event_id, reason, canonical_json(detail)),
        )
        return case_id

    def _maybe_temperature(self, envelope: dict[str, Any]) -> float | None:
        if envelope["event_type"] != "environment":
            return None
        value = envelope["detail"].get("temperature_c")
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
            return None
        return float(value)

    def _check_device_clock(self, connection, *, device_id: str, envelope: dict[str, Any]) -> None:
        """设备时钟体检：序列更靠后但时间更早是时钟倒退；与更高序列冲突是矛盾。"""

        occurred_at = envelope["occurred_at"]
        local_sequence = envelope["local_sequence"]
        row = connection.execute(
            "SELECT MAX(occurred_at) AS latest FROM custody_events "
            "WHERE device_id=? AND status='confirmed' AND local_sequence<?",
            (device_id, local_sequence)).fetchone()
        if row and row["latest"] and occurred_at < row["latest"]:
            raise QuarantineRequired(REASON_CLOCK_ROLLBACK, "设备时钟出现倒退",
                                     {"occurred_at": occurred_at, "latest_confirmed": row["latest"]})
        row = connection.execute(
            "SELECT MIN(occurred_at) AS earliest FROM custody_events "
            "WHERE device_id=? AND status='confirmed' AND local_sequence>?",
            (device_id, local_sequence)).fetchone()
        if row and row["earliest"] and occurred_at > row["earliest"]:
            raise QuarantineRequired(REASON_CONTRADICTION, "事件时间与更高序列的已确认事件矛盾",
                                     {"occurred_at": occurred_at, "earliest_confirmed": row["earliest"]})
        future_limit = (self.clock.now().astimezone(timezone.utc)
                        + timedelta(seconds=FUTURE_SKEW_SECONDS)).isoformat(timespec="microseconds").replace("+00:00", "Z")
        if occurred_at > future_limit:
            raise QuarantineRequired(REASON_CLOCK_ANOMALY, "事件时间超过服务器当前时间",
                                     {"occurred_at": occurred_at, "server_now": self._now()})

    # ------------------------------------------------------------------
    # 事件应用：先完整校验再落库，校验失败抛 QuarantineRequired
    # ------------------------------------------------------------------

    def _apply(self, connection, *, actor: Actor, event_id: str, envelope: dict[str, Any]) -> dict[str, Any]:
        event_type = envelope["event_type"]
        detail = envelope["detail"]
        base = envelope["base_versions"]
        references = self._resolve_references(connection, envelope["references"])
        handlers = {
            "split": self._apply_split,
            "merge": self._apply_merge,
            "lend": self._apply_lend,
            "return": self._apply_return,
            "consume": self._apply_consume,
            "derive": self._apply_derive,
            "seal": self._apply_seal,
            "unseal": self._apply_unseal,
            "transfer": self._apply_transfer,
            "weigh": self._apply_weigh,
            "environment": self._apply_environment,
        }
        outcome = handlers[event_type](connection, actor, event_id, detail, base)
        outcome["references"] = references
        outcome["parties"] = {"submitter": self._party_of(actor), **outcome["parties"]}
        return outcome

    def _resolve_references(self, connection, references: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """把事件引用的依据文档解析为当时版本的内容快照。"""

        snapshots = []
        for reference in references:
            row = connection.execute(
                "SELECT * FROM reference_docs WHERE doc_id=? AND version=?",
                (reference["doc_id"], reference["version"])).fetchone()
            if row is None:
                raise QuarantineRequired(
                    REASON_CONTRADICTION,
                    f"依据 {reference['doc_id']}@{reference['version']} 不存在",
                    {"doc_id": reference["doc_id"], "version": reference["version"]})
            snapshots.append({"doc_id": row["doc_id"], "version": row["version"],
                              "category": row["category"], "content": json.loads(row["content_json"])})
        return snapshots

    def _container_row(self, connection, container_id: Any):
        if not isinstance(container_id, str) or not container_id:
            raise ValidationError("container_id 不能为空")
        return connection.execute("SELECT * FROM containers WHERE container_id=?", (container_id,)).fetchone()

    def _require_container(self, connection, container_id: Any):
        row = self._container_row(connection, container_id)
        if row is None:
            raise QuarantineRequired(REASON_DISCONTINUITY,
                                     f"容器 {container_id} 尚未登记，事件无法接续",
                                     {"container_id": container_id})
        return row

    def _check_base(self, base_versions: dict[str, int], row) -> None:
        container_id = row["container_id"]
        if container_id not in base_versions:
            raise ValidationError(f"base_versions 缺少容器 {container_id} 的基准版本")
        provided = base_versions[container_id]
        if provided != row["state_version"]:
            raise QuarantineRequired(REASON_DISCONTINUITY,
                                     f"容器 {container_id} 的基准版本 {provided} 与当前版本 "
                                     f"{row['state_version']} 不符，事件无法接续",
                                     {"container_id": container_id, "expected": row["state_version"],
                                      "provided": provided})

    def _check_status(self, row, allowed: set[str], event_type: str) -> None:
        if row["status"] not in allowed:
            raise QuarantineRequired(REASON_CONTRADICTION,
                                     f"容器 {row['container_id']} 状态为 {row['status']}，不能执行 {event_type}",
                                     {"container_id": row["container_id"], "status": row["status"]})

    def _check_custody(self, actor: Actor, row, event_type: str) -> None:
        if actor.actor_id != row["custodian_id"] and actor.role != "admin":
            reason = REASON_UNAUTHORIZED_UNSEAL if event_type == "unseal" else REASON_UNAUTHORIZED
            raise QuarantineRequired(reason,
                                     f"操作者 {actor.actor_id} 不是容器 {row['container_id']} 的保管人",
                                     {"container_id": row["container_id"],
                                      "custodian_id": row["custodian_id"], "actor_id": actor.actor_id})

    def _update_container(self, connection, container_id: str, *, version: int,
                          head_event_id: str, **fields: Any) -> None:
        assignments = ", ".join(f"{name}=?" for name in fields)
        if assignments:
            assignments += ", "
        connection.execute(
            f"UPDATE containers SET {assignments}state_version=?, head_event_id=? WHERE container_id=?",
            (*fields.values(), version, head_event_id, container_id),
        )

    @staticmethod
    def _effect(container_id: str, role: str, before: float | None,
                after: float | None, version: int) -> dict[str, Any]:
        return {"container_id": container_id, "role": role, "quantity_before": before,
                "quantity_after": after, "container_version": version}

    def _apply_split(self, connection, actor: Actor, event_id: str,
                     detail: dict[str, Any], base: dict[str, int]) -> dict[str, Any]:
        parent = self._require_container(connection, detail.get("parent_id"))
        self._check_base(base, parent)
        self._check_status(parent, {"active"}, "split")
        self._check_custody(actor, parent, "split")
        children = detail.get("children")
        if not isinstance(children, list) or not children:
            raise ValidationError("children 必须是非空数组")
        specs = []
        seen: set[str] = set()
        total = 0.0
        for child in children:
            if not isinstance(child, dict):
                raise ValidationError("children 元素必须是对象")
            child_id = self._identifier(child.get("container_id", ""), "children.container_id")
            if child_id in seen:
                raise ValidationError("children 容器编号重复")
            seen.add(child_id)
            if self._container_row(connection, child_id) is not None:
                raise QuarantineRequired(REASON_CONTRADICTION, f"子容器 {child_id} 已经存在",
                                         {"container_id": child_id})
            quantity = self._quantity(child.get("quantity"), "children.quantity")
            total += quantity
            specs.append({
                "container_id": child_id,
                "label": self._text(child.get("label", child_id), "children.label"),
                "quantity": quantity,
                "seal_id": self._optional_text(child.get("seal_id"), "children.seal_id", 120),
            })
        if total - parent["quantity"] > EPSILON:
            raise QuarantineRequired(REASON_CONTRADICTION, "分装总量超过母样余量",
                                     {"parent_id": parent["container_id"],
                                      "remaining": parent["quantity"], "requested": total})
        remaining = parent["quantity"] - total
        if remaining < EPSILON:
            remaining = 0.0
        parent_version = parent["state_version"] + 1
        self._update_container(connection, parent["container_id"], version=parent_version,
                               head_event_id=event_id, quantity=remaining,
                               status="exhausted" if remaining == 0 else "active")
        effects = [self._effect(parent["container_id"], "parent",
                                parent["quantity"], remaining, parent_version)]
        now = self._now()
        for spec in specs:
            self._insert_container(connection, container_id=spec["container_id"], plan_id=parent["plan_id"],
                                   kind="aliquot", label=spec["label"], quantity=spec["quantity"],
                                   unit=parent["unit"], custodian_id=parent["custodian_id"],
                                   location=parent["location"], seal_id=spec["seal_id"],
                                   result_id=None, head_event_id=event_id, created_at=now)
            connection.execute(
                "INSERT INTO container_parents(container_id,parent_id,event_id) VALUES(?,?,?)",
                (spec["container_id"], parent["container_id"], event_id))
            effects.append(self._effect(spec["container_id"], "child", None, spec["quantity"], 1))
        return {"subject": parent["container_id"], "effects": effects,
                "parties": {"custodian": self._party(connection, parent["custodian_id"])},
                "extra": {"children": [spec["container_id"] for spec in specs],
                          "remaining": remaining},
                "temperature": None}

    def _apply_merge(self, connection, actor: Actor, event_id: str,
                     detail: dict[str, Any], base: dict[str, int]) -> dict[str, Any]:
        source_ids = detail.get("source_ids")
        if not isinstance(source_ids, list) or len(source_ids) < 2 or not all(
                isinstance(item, str) and item for item in source_ids):
            raise ValidationError("source_ids 必须包含至少两个容器编号")
        if len(set(source_ids)) != len(source_ids):
            raise ValidationError("source_ids 存在重复")
        target = detail.get("target")
        if not isinstance(target, dict):
            raise ValidationError("target 必须是对象")
        target_id = self._identifier(target.get("container_id", ""), "target.container_id")
        if target_id in source_ids:
            raise ValidationError("target 不能与来源容器相同")
        rows = [self._require_container(connection, source_id) for source_id in source_ids]
        for row in rows:
            self._check_base(base, row)
            self._check_status(row, {"active"}, "merge")
        first = rows[0]
        self._check_custody(actor, first, "merge")
        for row in rows[1:]:
            if row["custodian_id"] != first["custodian_id"] or row["location"] != first["location"]:
                raise QuarantineRequired(REASON_CONTRADICTION, "合并来源不在同一保管人与位置",
                                         {"container_id": row["container_id"]})
            if row["unit"] != first["unit"] or row["plan_id"] != first["plan_id"]:
                raise QuarantineRequired(REASON_CONTRADICTION, "合并来源的单位或计划不一致",
                                         {"container_id": row["container_id"]})
        if self._container_row(connection, target_id) is not None:
            raise QuarantineRequired(REASON_CONTRADICTION, f"目标容器 {target_id} 已经存在",
                                     {"container_id": target_id})
        total = sum(row["quantity"] for row in rows)
        effects = []
        for row in rows:
            version = row["state_version"] + 1
            self._update_container(connection, row["container_id"], version=version,
                                   head_event_id=event_id, quantity=0.0, status="merged")
            effects.append(self._effect(row["container_id"], "source", row["quantity"], 0.0, version))
        self._insert_container(connection, container_id=target_id, plan_id=first["plan_id"], kind="pool",
                               label=self._text(target.get("label", target_id), "target.label"),
                               quantity=total, unit=first["unit"], custodian_id=first["custodian_id"],
                               location=first["location"],
                               seal_id=self._optional_text(target.get("seal_id"), "target.seal_id", 120),
                               result_id=None, head_event_id=event_id, created_at=self._now())
        for row in rows:
            connection.execute(
                "INSERT INTO container_parents(container_id,parent_id,event_id) VALUES(?,?,?)",
                (target_id, row["container_id"], event_id))
        effects.append(self._effect(target_id, "target", None, total, 1))
        return {"subject": target_id, "effects": effects,
                "parties": {"custodian": self._party(connection, first["custodian_id"])},
                "extra": {"source_ids": list(source_ids), "merged_quantity": total},
                "temperature": None}

    def _apply_lend(self, connection, actor: Actor, event_id: str,
                    detail: dict[str, Any], base: dict[str, int]) -> dict[str, Any]:
        subject = self._require_container(connection, detail.get("container_id"))
        self._check_base(base, subject)
        self._check_status(subject, {"active"}, "lend")
        self._check_custody(actor, subject, "lend")
        borrower = self._active_party(connection, detail.get("borrower_id"), "借用人")
        due_back_at = self._parse_time(detail.get("due_back_at"), "due_back_at")
        version = subject["state_version"] + 1
        self._update_container(connection, subject["container_id"], version=version,
                               head_event_id=event_id, status="lent",
                               custodian_id=borrower["actor_id"], due_back_at=due_back_at)
        return {"subject": subject["container_id"],
                "effects": [self._effect(subject["container_id"], "subject",
                                         subject["quantity"], subject["quantity"], version)],
                "parties": {"from_custodian": self._party(connection, subject["custodian_id"]),
                            "borrower": borrower},
                "extra": {"due_back_at": due_back_at},
                "temperature": None}

    def _apply_return(self, connection, actor: Actor, event_id: str,
                      detail: dict[str, Any], base: dict[str, int]) -> dict[str, Any]:
        subject = self._require_container(connection, detail.get("container_id"))
        self._check_base(base, subject)
        self._check_status(subject, {"lent"}, "return")
        self._check_custody(actor, subject, "return")
        receiver = self._active_party(connection, detail.get("receiver_id"), "接收人")
        location = self._optional_text(detail.get("location"), "location")
        version = subject["state_version"] + 1
        fields: dict[str, Any] = {"status": "active", "custodian_id": receiver["actor_id"], "due_back_at": None}
        if location is not None:
            fields["location"] = location
        self._update_container(connection, subject["container_id"], version=version,
                               head_event_id=event_id, **fields)
        return {"subject": subject["container_id"],
                "effects": [self._effect(subject["container_id"], "subject",
                                         subject["quantity"], subject["quantity"], version)],
                "parties": {"from_custodian": self._party(connection, subject["custodian_id"]),
                            "receiver": receiver},
                "extra": {},
                "temperature": None}

    def _apply_consume(self, connection, actor: Actor, event_id: str,
                       detail: dict[str, Any], base: dict[str, int]) -> dict[str, Any]:
        subject = self._require_container(connection, detail.get("container_id"))
        self._check_base(base, subject)
        self._check_status(subject, {"active"}, "consume")
        self._check_custody(actor, subject, "consume")
        amount = self._quantity(detail.get("amount"), "amount")
        if amount - subject["quantity"] > EPSILON:
            raise QuarantineRequired(REASON_CONTRADICTION, "消耗数量超出容器余量",
                                     {"container_id": subject["container_id"],
                                      "remaining": subject["quantity"], "requested": amount})
        purpose = self._optional_text(detail.get("purpose"), "purpose", 300)
        remaining = subject["quantity"] - amount
        if remaining < EPSILON:
            remaining = 0.0
        version = subject["state_version"] + 1
        self._update_container(connection, subject["container_id"], version=version,
                               head_event_id=event_id, quantity=remaining,
                               status="consumed" if remaining == 0 else "active")
        return {"subject": subject["container_id"],
                "effects": [self._effect(subject["container_id"], "subject",
                                         subject["quantity"], remaining, version)],
                "parties": {"custodian": self._party(connection, subject["custodian_id"])},
                "extra": {"amount": amount, "purpose": purpose},
                "temperature": None}

    def _apply_derive(self, connection, actor: Actor, event_id: str,
                      detail: dict[str, Any], base: dict[str, int]) -> dict[str, Any]:
        parent = self._require_container(connection, detail.get("parent_id"))
        self._check_base(base, parent)
        self._check_status(parent, {"active"}, "derive")
        self._check_custody(actor, parent, "derive")
        analysis = detail.get("analysis")
        if not isinstance(analysis, dict):
            raise ValidationError("analysis 必须是对象")
        child_id = self._identifier(analysis.get("container_id", ""), "analysis.container_id")
        result_id = self._identifier(analysis.get("result_id", ""), "analysis.result_id")
        quantity = self._quantity(analysis.get("quantity"), "analysis.quantity")
        method = self._optional_text(analysis.get("method"), "analysis.method", 200)
        if self._container_row(connection, child_id) is not None:
            raise QuarantineRequired(REASON_CONTRADICTION, f"分析容器 {child_id} 已经存在",
                                     {"container_id": child_id})
        if connection.execute("SELECT 1 FROM containers WHERE result_id=?", (result_id,)).fetchone():
            raise QuarantineRequired(REASON_CONTRADICTION, f"分析结果 {result_id} 已经登记",
                                     {"result_id": result_id})
        if quantity - parent["quantity"] > EPSILON:
            raise QuarantineRequired(REASON_CONTRADICTION, "派生用量超过母样余量",
                                     {"parent_id": parent["container_id"],
                                      "remaining": parent["quantity"], "requested": quantity})
        remaining = parent["quantity"] - quantity
        if remaining < EPSILON:
            remaining = 0.0
        parent_version = parent["state_version"] + 1
        self._update_container(connection, parent["container_id"], version=parent_version,
                               head_event_id=event_id, quantity=remaining,
                               status="exhausted" if remaining == 0 else "active")
        self._insert_container(connection, container_id=child_id, plan_id=parent["plan_id"], kind="analysis",
                               label=self._text(analysis.get("label", result_id), "analysis.label"),
                               quantity=quantity, unit=parent["unit"], custodian_id=parent["custodian_id"],
                               location=parent["location"], seal_id=None, result_id=result_id,
                               head_event_id=event_id, created_at=self._now())
        connection.execute(
            "INSERT INTO container_parents(container_id,parent_id,event_id) VALUES(?,?,?)",
            (child_id, parent["container_id"], event_id))
        effects = [self._effect(parent["container_id"], "parent",
                                parent["quantity"], remaining, parent_version),
                   self._effect(child_id, "child", None, quantity, 1)]
        return {"subject": parent["container_id"], "effects": effects,
                "parties": {"custodian": self._party(connection, parent["custodian_id"])},
                "extra": {"analysis_container_id": child_id, "result_id": result_id, "method": method},
                "temperature": None}

    def _apply_seal(self, connection, actor: Actor, event_id: str,
                    detail: dict[str, Any], base: dict[str, int]) -> dict[str, Any]:
        subject = self._require_container(connection, detail.get("container_id"))
        self._check_base(base, subject)
        self._check_status(subject, {"active", "lent"}, "seal")
        self._check_custody(actor, subject, "seal")
        seal_id = self._text(detail.get("seal_id"), "seal_id", 120)
        version = subject["state_version"] + 1
        previous_seal = subject["seal_id"]
        self._update_container(connection, subject["container_id"], version=version,
                               head_event_id=event_id, seal_id=seal_id)
        return {"subject": subject["container_id"],
                "effects": [self._effect(subject["container_id"], "subject",
                                         subject["quantity"], subject["quantity"], version)],
                "parties": {"custodian": self._party(connection, subject["custodian_id"])},
                "extra": {"previous_seal": previous_seal, "seal_id": seal_id},
                "temperature": None}

    def _apply_unseal(self, connection, actor: Actor, event_id: str,
                      detail: dict[str, Any], base: dict[str, int]) -> dict[str, Any]:
        subject = self._require_container(connection, detail.get("container_id"))
        self._check_base(base, subject)
        self._check_status(subject, {"active", "lent"}, "unseal")
        self._check_custody(actor, subject, "unseal")
        seal_id = self._text(detail.get("seal_id"), "seal_id", 120)
        if subject["seal_id"] is None:
            raise QuarantineRequired(REASON_CONTRADICTION, "容器当前未加封",
                                     {"container_id": subject["container_id"]})
        if subject["seal_id"] != seal_id:
            raise QuarantineRequired(REASON_CONTRADICTION, "封签编号与台账记录不符",
                                     {"container_id": subject["container_id"],
                                      "expected": subject["seal_id"], "provided": seal_id})
        version = subject["state_version"] + 1
        self._update_container(connection, subject["container_id"], version=version,
                               head_event_id=event_id, seal_id=None)
        return {"subject": subject["container_id"],
                "effects": [self._effect(subject["container_id"], "subject",
                                         subject["quantity"], subject["quantity"], version)],
                "parties": {"custodian": self._party(connection, subject["custodian_id"])},
                "extra": {"opened_seal": seal_id},
                "temperature": None}

    def _apply_transfer(self, connection, actor: Actor, event_id: str,
                        detail: dict[str, Any], base: dict[str, int]) -> dict[str, Any]:
        subject = self._require_container(connection, detail.get("container_id"))
        self._check_base(base, subject)
        self._check_status(subject, {"active", "lent"}, "transfer")
        self._check_custody(actor, subject, "transfer")
        receiver = self._active_party(connection, detail.get("to_custodian_id"), "接收保管人")
        location = self._optional_text(detail.get("location"), "location")
        version = subject["state_version"] + 1
        fields: dict[str, Any] = {"custodian_id": receiver["actor_id"]}
        if location is not None:
            fields["location"] = location
        self._update_container(connection, subject["container_id"], version=version,
                               head_event_id=event_id, **fields)
        return {"subject": subject["container_id"],
                "effects": [self._effect(subject["container_id"], "subject",
                                         subject["quantity"], subject["quantity"], version)],
                "parties": {"from_custodian": self._party(connection, subject["custodian_id"]),
                            "to_custodian": receiver},
                "extra": {},
                "temperature": None}

    def _apply_weigh(self, connection, actor: Actor, event_id: str,
                     detail: dict[str, Any], base: dict[str, int]) -> dict[str, Any]:
        subject = self._require_container(connection, detail.get("container_id"))
        self._check_base(base, subject)
        self._check_status(subject, {"active", "lent"}, "weigh")
        observed = self._quantity(detail.get("observed_quantity"), "observed_quantity", allow_zero=True)
        expected = subject["quantity"]
        tolerance = max(1e-6, abs(expected) * WEIGH_TOLERANCE_RELATIVE)
        if abs(observed - expected) > tolerance:
            raise QuarantineRequired(REASON_CONTRADICTION, "称量结果与台账数量矛盾",
                                     {"container_id": subject["container_id"],
                                      "expected": expected, "observed": observed,
                                      "tolerance": tolerance})
        version = subject["state_version"] + 1
        self._update_container(connection, subject["container_id"], version=version, head_event_id=event_id)
        return {"subject": subject["container_id"],
                "effects": [self._effect(subject["container_id"], "subject", expected, expected, version)],
                "parties": {"custodian": self._party(connection, subject["custodian_id"])},
                "extra": {"observed_quantity": observed},
                "temperature": None}

    def _apply_environment(self, connection, actor: Actor, event_id: str,
                           detail: dict[str, Any], base: dict[str, int]) -> dict[str, Any]:
        subject = self._require_container(connection, detail.get("container_id"))
        self._check_base(base, subject)
        self._check_status(subject, {"active", "lent"}, "environment")
        temperature = self._temperature(detail.get("temperature_c"), "temperature_c")
        humidity = detail.get("humidity")
        if humidity is not None:
            humidity = self._temperature(humidity, "humidity")
        version = subject["state_version"] + 1
        self._update_container(connection, subject["container_id"], version=version, head_event_id=event_id)
        return {"subject": subject["container_id"],
                "effects": [self._effect(subject["container_id"], "subject",
                                         subject["quantity"], subject["quantity"], version)],
                "parties": {"custodian": self._party(connection, subject["custodian_id"])},
                "extra": {"temperature_c": temperature, "humidity": humidity},
                "temperature": temperature}

    # ------------------------------------------------------------------
    # 隔离裁决：只能驳回，或在事件仍适用时追加并入，绝不改写已确认历史
    # ------------------------------------------------------------------

    def decide_quarantine(self, *, request_id: str, actor_id: str, case_id: str,
                          decision: str, note: str = "") -> WriteReceipt:
        if decision not in {"accept", "reject"}:
            raise ValidationError("decision 必须是 accept 或 reject")
        payload = {"actor_id": actor_id, "case_id": case_id, "decision": decision, "note": note}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require_role(actor, ADJUDICATE_ROLES)
            case = connection.execute(
                "SELECT * FROM quarantine_cases WHERE case_id=?", (case_id,)).fetchone()
            if case is None:
                raise NotFoundError("隔离案件不存在")
            note = self._text(note, "note", 500) if note else ""

            def create() -> tuple[str, str, dict[str, Any]]:
                if case["status"] != "pending":
                    raise ConflictError("案件已裁决")
                now = self._now()
                if decision == "reject":
                    connection.execute(
                        "UPDATE quarantine_cases SET status='rejected', decided_by=?, decided_at=?, decision_note=? "
                        "WHERE case_id=?",
                        (actor_id, now, note, case_id))
                    append_event(connection, actor_id=actor_id, action="custody.quarantine.decided",
                                 resource_type="quarantine_case", resource_id=case_id,
                                 detail={"decision": "reject", "reason": case["reason"]}, occurred_at=now)
                    return "quarantine_case", case_id, {"case_id": case_id, "status": "rejected"}
                event = connection.execute(
                    "SELECT * FROM custody_events WHERE event_id=?", (case["event_id"],)).fetchone()
                envelope = json.loads(event["payload_json"])["envelope"]
                submitter_row = connection.execute(
                    "SELECT * FROM actors WHERE actor_id=?", (envelope["submitted_by"],)).fetchone()
                if submitter_row is None or not submitter_row["active"]:
                    raise ConflictError("原提交人已停用，事件无法并入")
                submitter = Actor(submitter_row["actor_id"], submitter_row["display_name"],
                                  submitter_row["role"], submitter_row["organization_id"],
                                  bool(submitter_row["active"]))
                device = None
                if envelope.get("device_id"):
                    device = connection.execute(
                        "SELECT * FROM field_devices WHERE device_id=?",
                        (envelope["device_id"],)).fetchone()
                try:
                    if device is not None:
                        self._check_device_clock(connection, device_id=device["device_id"], envelope=envelope)
                    outcome = self._apply(connection, actor=submitter,
                                          event_id=event["event_id"], envelope=envelope)
                except QuarantineRequired as hold:
                    raise ConflictError(f"事件仍无法并入：{hold.message}") from hold
                confirmed_payload = {
                    **envelope,
                    "references": outcome["references"],
                    "parties": outcome["parties"],
                    "extra": outcome["extra"],
                }
                connection.execute(
                    "UPDATE custody_events SET status='confirmed', subject_container_id=?, payload_json=?, "
                    "temperature_c=? WHERE event_id=?",
                    (outcome["subject"], canonical_json(confirmed_payload),
                     outcome["temperature"], event["event_id"]))
                for effect in outcome["effects"]:
                    self._insert_effect(connection, event["event_id"], effect["container_id"], effect["role"],
                                        effect["quantity_before"], effect["quantity_after"],
                                        effect["container_version"])
                connection.execute(
                    "UPDATE quarantine_cases SET status='accepted', decided_by=?, decided_at=?, decision_note=? "
                    "WHERE case_id=?",
                    (actor_id, now, note, case_id))
                append_event(connection, actor_id=actor_id, action=f"custody.event.{envelope['event_type']}",
                             resource_type="custody_event", resource_id=event["event_id"],
                             detail={"event_type": envelope["event_type"], "subject": outcome["subject"],
                                     "site_id": envelope["site_id"], "adjudicated": True}, occurred_at=now)
                append_event(connection, actor_id=actor_id, action="custody.quarantine.decided",
                             resource_type="quarantine_case", resource_id=case_id,
                             detail={"decision": "accept", "reason": case["reason"]}, occurred_at=now)
                return "quarantine_case", case_id, {"case_id": case_id, "status": "accepted"}

            receipt, _ = self._idempotent(connection, request_id=request_id,
                                          action="custody.decide_quarantine", payload=payload, create=create)
            return receipt

    # ------------------------------------------------------------------
    # 查询：容器台账、分析结果溯源、数量核对、管理员总览
    # ------------------------------------------------------------------

    def _container_dict(self, row, parents: list[str]) -> dict[str, Any]:
        return {"container_id": row["container_id"], "plan_id": row["plan_id"], "kind": row["kind"],
                "label": row["label"], "quantity": row["quantity"], "unit": row["unit"],
                "status": row["status"], "custodian_id": row["custodian_id"], "location": row["location"],
                "seal_id": row["seal_id"], "due_back_at": row["due_back_at"], "result_id": row["result_id"],
                "state_version": row["state_version"], "parents": parents}

    def _event_dict(self, connection, row) -> dict[str, Any]:
        payload = json.loads(row["payload_json"])
        effects = connection.execute(
            "SELECT * FROM event_effects WHERE event_id=? ORDER BY container_id, role",
            (row["event_id"],)).fetchall()
        return {"event_id": row["event_id"], "event_type": row["event_type"],
                "occurred_at": row["occurred_at"], "recorded_at": row["recorded_at"],
                "submitted_by": row["submitted_by"], "device_id": row["device_id"],
                "local_sequence": row["local_sequence"],
                "detail": payload.get("detail"), "parties": payload.get("parties"),
                "references": payload.get("references"), "extra": payload.get("extra"),
                "effects": [{"container_id": effect["container_id"], "role": effect["role"],
                             "quantity_before": effect["quantity_before"],
                             "quantity_after": effect["quantity_after"],
                             "container_version": effect["container_version"]} for effect in effects]}

    def get_container(self, container_id: str) -> dict[str, Any]:
        connection = self.database.connection
        row = self._container_row(connection, container_id)
        if row is None:
            raise NotFoundError("容器不存在")
        parents = [item["parent_id"] for item in connection.execute(
            "SELECT parent_id FROM container_parents WHERE container_id=? ORDER BY parent_id",
            (container_id,)).fetchall()]
        children = [item["container_id"] for item in connection.execute(
            "SELECT container_id FROM container_parents WHERE parent_id=? ORDER BY container_id",
            (container_id,)).fetchall()]
        history_rows = connection.execute(
            "SELECT e.* FROM custody_events e JOIN event_effects f ON f.event_id=e.event_id "
            "WHERE f.container_id=? AND e.status='confirmed' ORDER BY f.container_version",
            (container_id,)).fetchall()
        return {"container": self._container_dict(row, parents), "children": children,
                "history": [self._event_dict(connection, event) for event in history_rows]}

    def get_provenance(self, result_id: str) -> dict[str, Any]:
        """还原一个分析结果对应的容器谱系与每一次保管转移。"""

        connection = self.database.connection
        target = connection.execute(
            "SELECT * FROM containers WHERE result_id=?", (result_id,)).fetchone()
        if target is None:
            raise NotFoundError("分析结果不存在")
        ancestors = connection.execute(
            "WITH RECURSIVE lineage(container_id) AS ("
            "SELECT parent_id FROM container_parents WHERE container_id=? "
            "UNION "
            "SELECT cp.parent_id FROM container_parents cp "
            "JOIN lineage l ON cp.container_id=l.container_id"
            ") SELECT DISTINCT container_id FROM lineage",
            (target["container_id"],)).fetchall()
        container_ids = [target["container_id"]] + [row["container_id"] for row in ancestors]
        marks = ",".join("?" for _ in container_ids)
        containers = connection.execute(
            f"SELECT * FROM containers WHERE container_id IN ({marks}) "
            "ORDER BY created_at, container_id", container_ids).fetchall()
        parent_rows = connection.execute(
            f"SELECT * FROM container_parents WHERE container_id IN ({marks}) "
            "ORDER BY parent_id", container_ids).fetchall()
        parents_map: dict[str, list[str]] = {}
        for row in parent_rows:
            parents_map.setdefault(row["container_id"], []).append(row["parent_id"])

        def depth(container_id: str, seen: frozenset[str] = frozenset()) -> int:
            parents = [parent for parent in parents_map.get(container_id, []) if parent not in seen]
            if not parents:
                return 0
            return 1 + max(depth(parent, seen | {container_id}) for parent in parents)

        lineage = [self._container_dict(row, parents_map.get(row["container_id"], []))
                   for row in sorted(containers, key=lambda row: (depth(row["container_id"]),
                                                                  row["container_id"]))]
        event_rows = connection.execute(
            f"SELECT DISTINCT e.* FROM custody_events e JOIN event_effects f ON f.event_id=e.event_id "
            f"WHERE e.status='confirmed' AND f.container_id IN ({marks}) "
            "ORDER BY e.occurred_at, e.seq", container_ids).fetchall()
        return {"result_id": result_id,
                "analysis_container": self._container_dict(
                    target, parents_map.get(target["container_id"], [])),
                "lineage": lineage,
                "custody_events": [self._event_dict(connection, row) for row in event_rows]}

    def reconcile_plan(self, plan_id: str) -> dict[str, Any]:
        """核对计划内父子样品数量：任何一克样品都不能凭空增加或消失。"""

        connection = self.database.connection
        plan = connection.execute("SELECT * FROM sampling_plans WHERE plan_id=?", (plan_id,)).fetchone()
        if plan is None:
            raise NotFoundError("采样计划不存在")
        violations: list[dict[str, Any]] = []
        containers = connection.execute(
            "SELECT * FROM containers WHERE plan_id=? ORDER BY created_at, container_id", (plan_id,)).fetchall()
        for container in containers:
            effects = connection.execute(
                "SELECT f.* FROM event_effects f JOIN custody_events e ON e.event_id=f.event_id "
                "WHERE f.container_id=? AND e.status='confirmed' ORDER BY f.container_version",
                (container["container_id"],)).fetchall()
            for index, effect in enumerate(effects, start=1):
                if effect["container_version"] != index:
                    violations.append({"container_id": container["container_id"], "issue": "版本链断裂",
                                       "expected_version": index,
                                       "found_version": effect["container_version"]})
                    break
            for previous, current in zip(effects, effects[1:]):
                if previous["quantity_after"] is None or current["quantity_before"] is None:
                    continue
                if abs(current["quantity_before"] - previous["quantity_after"]) > EPSILON:
                    violations.append({"container_id": container["container_id"], "issue": "数量链条断裂",
                                       "version": current["container_version"]})
            if effects:
                final = effects[-1]["quantity_after"]
                if final is not None and abs(final - container["quantity"]) > EPSILON:
                    violations.append({"container_id": container["container_id"], "issue": "台账数量与事件链不符",
                                       "ledger": container["quantity"], "chain_final": final})
        lineage_events = connection.execute(
            "SELECT DISTINCT e.* FROM custody_events e JOIN event_effects f ON f.event_id=e.event_id "
            "JOIN containers c ON c.container_id=f.container_id "
            "WHERE e.status='confirmed' AND c.plan_id=? AND e.event_type IN ('split','derive','merge','consume')",
            (plan_id,)).fetchall()
        for event in lineage_events:
            effects = connection.execute(
                "SELECT * FROM event_effects WHERE event_id=?", (event["event_id"],)).fetchall()
            by_role: dict[str, list[Any]] = {}
            for effect in effects:
                by_role.setdefault(effect["role"], []).append(effect)
            if event["event_type"] in ("split", "derive"):
                parent = by_role["parent"][0]
                produced = sum(effect["quantity_after"] for effect in by_role.get("child", []))
                delta = parent["quantity_before"] - parent["quantity_after"]
                if abs(delta - produced) > EPSILON:
                    violations.append({"event_id": event["event_id"], "issue": "分装派生数量不守恒",
                                       "parent_delta": delta, "children_total": produced})
            elif event["event_type"] == "merge":
                target = by_role["target"][0]
                merged = sum(effect["quantity_before"] - effect["quantity_after"]
                             for effect in by_role.get("source", []))
                if abs(target["quantity_after"] - merged) > EPSILON:
                    violations.append({"event_id": event["event_id"], "issue": "合并数量不守恒",
                                       "target_total": target["quantity_after"], "sources_total": merged})
            elif event["event_type"] == "consume":
                subject = by_role["subject"][0]
                amount = json.loads(event["payload_json"])["detail"]["amount"]
                delta = subject["quantity_before"] - subject["quantity_after"]
                if abs(delta - amount) > EPSILON:
                    violations.append({"event_id": event["event_id"], "issue": "消耗数量不守恒",
                                       "recorded_amount": amount, "actual_delta": delta})
        registered = connection.execute(
            "SELECT COALESCE(SUM(f.quantity_after),0) AS total FROM event_effects f "
            "JOIN custody_events e ON e.event_id=f.event_id "
            "JOIN containers c ON c.container_id=f.container_id "
            "WHERE c.plan_id=? AND e.event_type='register' AND e.status='confirmed'",
            (plan_id,)).fetchone()["total"]
        current = connection.execute(
            "SELECT COALESCE(SUM(quantity),0) AS total FROM containers "
            "WHERE plan_id=? AND status IN ('active','lent')", (plan_id,)).fetchone()["total"]
        consumed = connection.execute(
            "SELECT COALESCE(SUM(f.quantity_before - f.quantity_after),0) AS total FROM event_effects f "
            "JOIN custody_events e ON e.event_id=f.event_id "
            "JOIN containers c ON c.container_id=f.container_id "
            "WHERE c.plan_id=? AND e.event_type='consume' AND e.status='confirmed' AND f.role='subject'",
            (plan_id,)).fetchone()["total"]
        balanced = not violations and abs(registered - current - consumed) <= EPSILON
        return {"plan_id": plan_id, "unit": plan["unit"], "registered_total": registered,
                "current_total": current, "consumed_total": consumed,
                "balanced": balanced, "violations": violations}

    def admin_overview(self, site_id: str) -> dict[str, Any]:
        """管理员视角：失联、超温、封签异常、待裁决与数量失衡的样品。"""

        connection = self.database.connection
        self._site(connection, site_id)
        now = self._now()
        lost = connection.execute(
            "SELECT c.container_id, c.label, c.custodian_id, c.due_back_at FROM containers c "
            "JOIN sampling_plans p ON p.plan_id=c.plan_id "
            "WHERE p.site_id=? AND c.status='lent' AND c.due_back_at < ? ORDER BY c.due_back_at",
            (site_id, now)).fetchall()
        over_temperature = connection.execute(
            "SELECT c.container_id, c.label, MAX(e.temperature_c) AS peak_temperature_c, "
            "p.max_temperature_c AS threshold_c FROM containers c "
            "JOIN sampling_plans p ON p.plan_id=c.plan_id "
            "JOIN custody_events e ON e.subject_container_id=c.container_id "
            "WHERE p.site_id=? AND p.max_temperature_c IS NOT NULL "
            "AND e.event_type='environment' AND e.status='confirmed' "
            "GROUP BY c.container_id HAVING peak_temperature_c > p.max_temperature_c "
            "ORDER BY c.container_id",
            (site_id,)).fetchall()
        seal_anomalies = connection.execute(
            "SELECT DISTINCT c.container_id, c.label, q.case_id, q.reason FROM quarantine_cases q "
            "JOIN custody_events e ON e.event_id=q.event_id "
            "JOIN containers c ON c.container_id=e.subject_container_id "
            "JOIN sampling_plans p ON p.plan_id=c.plan_id "
            "WHERE p.site_id=? AND q.status='pending' "
            "AND (q.reason=? OR e.event_type IN ('seal','unseal')) ORDER BY c.container_id",
            (site_id, REASON_UNAUTHORIZED_UNSEAL)).fetchall()
        pending = connection.execute(
            "SELECT q.case_id, q.reason, q.event_id, e.event_type, e.subject_container_id, "
            "e.occurred_at, e.submitted_by FROM quarantine_cases q "
            "JOIN custody_events e ON e.event_id=q.event_id "
            "WHERE e.site_id=? AND q.status='pending' ORDER BY e.occurred_at, e.seq",
            (site_id,)).fetchall()
        imbalances = []
        plans = connection.execute(
            "SELECT plan_id FROM sampling_plans WHERE site_id=? ORDER BY plan_id", (site_id,)).fetchall()
        for plan in plans:
            summary = self.reconcile_plan(plan["plan_id"])
            if not summary["balanced"]:
                imbalances.append(summary)
        return {"site_id": site_id,
                "lost_contact": [dict(row) for row in lost],
                "over_temperature": [dict(row) for row in over_temperature],
                "seal_anomalies": [dict(row) for row in seal_anomalies],
                "pending_adjudication": [dict(row) for row in pending],
                "quantity_imbalances": imbalances}

    def list_quarantine(self, site_id: str | None = None, status: str | None = None) -> list[dict[str, Any]]:
        query = ("SELECT q.case_id, q.event_id, q.reason, q.status, q.decided_by, q.decided_at, "
                 "q.decision_note, e.event_type, e.site_id, e.subject_container_id, e.occurred_at, "
                 "e.submitted_by, e.device_id, e.local_sequence FROM quarantine_cases q "
                 "JOIN custody_events e ON e.event_id=q.event_id")
        conditions = []
        parameters: list[Any] = []
        if site_id:
            conditions.append("e.site_id=?")
            parameters.append(site_id)
        if status:
            conditions.append("q.status=?")
            parameters.append(status)
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += " ORDER BY q.rowid"
        rows = self.database.connection.execute(query, parameters).fetchall()
        return [dict(row) for row in rows]

    def list_reference_versions(self, doc_id: str) -> list[dict[str, Any]]:
        if not isinstance(doc_id, str) or not doc_id:
            raise ValidationError("doc_id 不能为空")
        rows = self.database.connection.execute(
            "SELECT * FROM reference_docs WHERE doc_id=? ORDER BY version", (doc_id,)).fetchall()
        return [{"doc_id": row["doc_id"], "version": row["version"], "category": row["category"],
                 "content": json.loads(row["content_json"]), "registered_by": row["registered_by"],
                 "registered_at": row["registered_at"]} for row in rows]
