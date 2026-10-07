"""实现样品监管链：容器谱系、事件回放、离线提交、隔离裁决与溯源查询。"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from .audit import append_event, canonical_json, digest
from .clock import Clock, SystemClock
from .custody_domain import (
    PLANNED_EVENTS,
    QUARANTINE_REASONS,
    TERMINAL_STATUSES,
)
from .errors import ConflictError, NotFoundError, PermissionDenied, ValidationError
from .models import Analysis, Container, CustodyEvent, QuarantineCase, SubmitResult, WriteReceipt
from .storage import Database

MASS_TOLERANCE_GRAMS = 0.01

# 可以开封容器的角色：研究与操作人员可以，审计角色只读。
OPENING_ROLES = frozenset({"admin", "operator", "reviewer"})
WRITE_ROLES = frozenset({"admin", "operator"})


class _Hold(Exception):
    """内部信号：事件必须进入隔离裁决而不是抛给调用方。"""

    def __init__(self, reason_code: str, detail: str) -> None:
        if reason_code not in QUARANTINE_REASONS:
            raise ValueError(f"未知隔离原因 {reason_code}")
        super().__init__(detail)
        self.reason_code = reason_code
        self.detail = detail


class CustodyService:
    """协调样品谱系、监管事件、离线回放、隔离裁决和审计哈希链。"""

    def __init__(self, database: Database, clock: Clock | None = None) -> None:
        self.database = database
        self.clock = clock or SystemClock()

    # ------------------------------------------------------------------ 工具

    def _now(self) -> str:
        return self.clock.now().astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

    def _actor(self, connection, actor_id: str) -> dict[str, Any]:
        if not actor_id:
            raise ValidationError("缺少操作者")
        row = connection.execute("SELECT * FROM actors WHERE actor_id=?", (actor_id,)).fetchone()
        if row is None:
            raise NotFoundError("操作者不存在")
        if not row["active"]:
            raise PermissionDenied("操作者已停用")
        return dict(row)

    def _require_roles(self, actor: dict[str, Any], roles: frozenset[str]) -> None:
        if actor["role"] not in roles:
            raise PermissionDenied("当前角色不能执行该动作")

    def _idempotent(self, connection, *, request_id: str, action: str, payload: dict[str, Any],
                    create) -> WriteReceipt:
        payload_hash = digest(payload)
        row = connection.execute(
            "SELECT * FROM request_receipts WHERE request_id=?", (request_id,)
        ).fetchone()
        if row:
            if row["action"] != action or row["payload_hash"] != payload_hash:
                raise ConflictError("request_id 已被不同内容使用")
            return WriteReceipt(request_id, row["resource_type"], row["resource_id"], True)
        resource_type, resource_id, response = create()
        connection.execute(
            "INSERT INTO request_receipts(request_id,action,payload_hash,resource_type,resource_id,"
            "response_json,created_at) VALUES(?,?,?,?,?,?,?)",
            (request_id, action, payload_hash, resource_type, resource_id,
             canonical_json(response), self._now()),
        )
        return WriteReceipt(request_id, resource_type, resource_id, False)

    def _mass(self, value: Any, field: str, *, allow_none: bool = False) -> float | None:
        if value is None and allow_none:
            return None
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise _Hold("validation_error", f"{field} 必须是数值（克）")
        value = float(value)
        if value < 0 or value != value:
            raise _Hold("validation_error", f"{field} 不能为负或 NaN")
        return round(value, 6)

    def _parse_occurred(self, value: Any) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str):
            return None
        text = value.strip().replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

    # ------------------------------------------------------------ 采样计划

    def create_sampling_plan(self, *, request_id: str, actor_id: str, site_id: str,
                             plan_id: str, cores: list[dict[str, Any]],
                             description: str | None = None) -> WriteReceipt:
        """登记采样计划，声明本批原始岩芯容器与期望封签。"""

        payload = {"actor_id": actor_id, "site_id": site_id, "plan_id": plan_id,
                   "cores": cores, "description": description}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require_roles(actor, WRITE_ROLES)
            site = connection.execute("SELECT * FROM sites WHERE site_id=?", (site_id,)).fetchone()
            if site is None:
                raise NotFoundError("场所不存在")
            if actor["organization_id"] != site["organization_id"] and actor["role"] != "admin":
                raise PermissionDenied("不能为其他组织的场所建立采样计划")
            if not isinstance(plan_id, str) or not plan_id.strip():
                raise ValidationError("plan_id 不能为空")
            self._validate_plan_cores(cores)

            normalized = [
                {k: core[k] for k in
                 ("container_id", "kind", "interval", "mass_grams", "expected_seal_id", "location")
                 if core.get(k) is not None}
                for core in cores
            ]

            def create() -> tuple[str, str, dict[str, Any]]:
                plan_payload = {"cores": normalized, "description": description}
                try:
                    connection.execute(
                        "INSERT INTO sampling_plans(plan_id,site_id,payload_json,payload_hash,"
                        "created_by,created_at) VALUES(?,?,?,?,?,?)",
                        (plan_id, site_id, canonical_json(plan_payload), digest(plan_payload),
                         actor_id, self._now()),
                    )
                except Exception as exc:
                    raise ConflictError("采样计划编号已经存在") from exc
                append_event(connection, actor_id=actor_id, action="sampling_plan.created",
                             resource_type="sampling_plan", resource_id=plan_id,
                             detail={"site_id": site_id, "core_count": len(normalized),
                                     "payload_hash": digest(plan_payload)},
                             occurred_at=self._now())
                return "sampling_plan", plan_id, {"plan_id": plan_id, "cores": len(normalized)}

            return self._idempotent(connection, request_id=request_id,
                                    action="create_sampling_plan", payload=payload, create=create)

    def _validate_plan_cores(self, cores: Any) -> list[dict[str, Any]]:
        if not isinstance(cores, list) or not cores:
            raise ValidationError("cores 必须是非空数组")
        seen: set[str] = set()
        for core in cores:
            if not isinstance(core, dict):
                raise ValidationError("每个岩芯声明必须是对象")
            cid = core.get("container_id")
            if not isinstance(cid, str) or not cid.strip():
                raise ValidationError("container_id 不能为空")
            if cid in seen:
                raise ValidationError(f"计划内容器编号重复：{cid}")
            seen.add(cid)
            kind = core.get("kind")
            if not isinstance(kind, str) or not kind.strip():
                raise ValidationError("kind 不能为空")
            mass = core.get("mass_grams")
            if mass is not None and (isinstance(mass, bool) or not isinstance(mass, (int, float)) or mass < 0):
                raise ValidationError("mass_grams 必须是非负数")
        return cores

    def get_plan(self, plan_id: str) -> dict[str, Any]:
        row = self.database.connection.execute(
            "SELECT * FROM sampling_plans WHERE plan_id=?", (plan_id,)
        ).fetchone()
        if row is None:
            raise NotFoundError("采样计划不存在")
        import json
        return {"plan_id": plan_id, "site_id": row["site_id"],
                "payload": json.loads(row["payload_json"]), "created_by": row["created_by"],
                "created_at": row["created_at"]}

    # ------------------------------------------------------ 事件提交/回放

    def submit_event(self, envelope: dict[str, Any], *, request_id: str | None = None) -> SubmitResult:
        """提交一个现场或在线监管事件；异常进入隔离，重复提交回放原结果。"""

        normalized = self._normalize_envelope(envelope)
        # 幂等指纹只覆盖客户端控制的字段；event_id 由服务端补生成，不参与重试判重。
        identity = {k: v for k, v in normalized.items() if k != "event_id"}
        envelope_hash = digest(identity)
        with self.database.transaction(immediate=True) as connection:
            device_id = normalized["device_id"]
            local_sequence = normalized["local_sequence"]

            # 设备本地序列是第一幂等键：重复上传只能回放原处理结果。
            if device_id is not None:
                receipt = connection.execute(
                    "SELECT * FROM device_event_receipts WHERE device_id=? AND local_sequence=?",
                    (device_id, local_sequence),
                ).fetchone()
                if receipt is not None:
                    if receipt["payload_hash"] != envelope_hash:
                        return self._quarantine(
                            connection, normalized, envelope_hash, request_id,
                            "duplicate_mismatch", "同一设备序列被用于不同内容，禁止覆盖原记录",
                            duplicate_of=receipt["case_id"] or receipt["event_id"],
                            store_device_receipt=False,
                        )
                    return SubmitResult(
                        accepted=receipt["outcome"] == "accepted", replayed=True,
                        event_id=receipt["event_id"], case_id=receipt["case_id"],
                        reason_code=None, reason_detail=None,
                    )

            # 在线提交的 request_id 是第二幂等键。
            if request_id:
                existing = connection.execute(
                    "SELECT * FROM request_receipts WHERE request_id=?", (request_id,)
                ).fetchone()
                if existing is not None:
                    if existing["payload_hash"] != envelope_hash:
                        raise ConflictError("request_id 已被不同内容使用")
                    response = _json_loads(existing["response_json"])
                    return SubmitResult(accepted=response["accepted"], replayed=True,
                                        event_id=response.get("event_id"),
                                        case_id=response.get("case_id"),
                                        reason_code=response.get("reason_code"),
                                        reason_detail=response.get("reason_detail"))

            self._touch_device(connection, device_id, self._now())

            # 客户端声明的事件编号已存在：同内容视为重试回放，不同内容必须隔离，
            # 绝不能让重复编号覆盖已确认历史。
            claimed = connection.execute(
                "SELECT event_type, actor_id, occurred_at, payload_hash FROM custody_events "
                "WHERE event_id=?", (normalized["event_id"],)).fetchone()
            if claimed is not None:
                same = (claimed["event_type"] == normalized["event_type"]
                        and claimed["actor_id"] == normalized["actor_id"]
                        and claimed["occurred_at"] == normalized["occurred_at"]
                        and claimed["payload_hash"] == digest(normalized["payload"]))
                if same:
                    result = {"accepted": True, "replayed": True,
                              "event_id": normalized["event_id"], "case_id": None,
                              "reason_code": None, "reason_detail": None}
                    self._store_outcome(connection, normalized, envelope_hash, request_id,
                                        outcome="accepted", event_id=normalized["event_id"],
                                        response=result)
                    return SubmitResult(True, True, normalized["event_id"], None, None, None)
                return self._quarantine(
                    connection, normalized, envelope_hash, request_id,
                    "duplicate_mismatch",
                    f"事件编号 {normalized['event_id']} 已被其他内容使用，不能覆盖已确认历史")

            # 时钟倒退与序列跳跃只对严格提交生效；它们绝不覆盖已确认历史。
            if device_id is not None:
                device = connection.execute(
                    "SELECT * FROM devices WHERE device_id=?", (device_id,)
                ).fetchone()
                # 离线补传可能乱序到达：只要 1..N-1 每个序列都已收到（无论已确认还是
                # 待裁决），N 就可以正常处理；缺槽时进入隔离，等待缺口补齐后裁决。
                prior = connection.execute(
                    "SELECT COUNT(*) AS count FROM device_event_receipts "
                    "WHERE device_id=? AND local_sequence<?",
                    (device_id, local_sequence)).fetchone()["count"]
                if prior != local_sequence - 1:
                    return self._quarantine(
                        connection, normalized, envelope_hash, request_id,
                        "sequence_gap",
                        f"本地序列 {local_sequence} 之前存在未收到的缺口",
                    )
                # last_occurred_at 只随“已确认”事件推进，待裁决与已驳回事件不参与。
                last_occurred = device["last_occurred_at"]
                if last_occurred is not None and normalized["occurred_at"] < last_occurred:
                    return self._quarantine(
                        connection, normalized, envelope_hash, request_id,
                        "clock_regression",
                        f"事件时间 {normalized['occurred_at']} 早于该设备最后已确认事件 {last_occurred}",
                    )

            try:
                event_id = self._apply_event(connection, normalized, strict=True)
            except _Hold as hold:
                return self._quarantine(connection, normalized, envelope_hash, request_id,
                                        hold.reason_code, hold.detail)

            response = {"accepted": True, "replayed": False, "event_id": event_id,
                        "case_id": None, "reason_code": None, "reason_detail": None}
            self._store_outcome(connection, normalized, envelope_hash, request_id,
                                outcome="accepted", event_id=event_id, response=response)
            self._refresh_device(connection, device_id)
            return SubmitResult(True, False, event_id, None, None, None)

    def _normalize_envelope(self, envelope: Any) -> dict[str, Any]:
        if not isinstance(envelope, dict):
            raise ValidationError("事件包络必须是 JSON 对象")
        event_type = envelope.get("event_type")
        if not isinstance(event_type, str) or event_type not in PLANNED_EVENTS:
            raise ValidationError("event_type 不在监管链事件范围内")
        device_id = envelope.get("device_id")
        local_sequence = envelope.get("local_sequence")
        if device_id is None and local_sequence is None:
            device_id, local_sequence = None, None
        elif device_id is None or local_sequence is None:
            raise ValidationError("device_id 与 local_sequence 必须同时提供")
        else:
            if isinstance(local_sequence, bool) or not isinstance(local_sequence, int) \
                    or local_sequence < 1:
                raise ValidationError("local_sequence 必须是不小于 1 的整数")
            device_id = str(device_id).strip()
            if not device_id:
                raise ValidationError("device_id 不能为空")
        payload = envelope.get("payload")
        if not isinstance(payload, dict):
            raise ValidationError("payload 必须是对象")
        occurred_at = self._parse_occurred(envelope.get("occurred_at"))
        if occurred_at is None:
            raise ValidationError("occurred_at 必须是带时区的 ISO8601 时间")
        actor_id = envelope.get("actor_id")
        if not isinstance(actor_id, str) or not actor_id.strip():
            raise ValidationError("actor_id 不能为空")
        expected = envelope.get("expected_version") or {}
        if not isinstance(expected, dict):
            raise ValidationError("expected_version 必须是 container_id 到版本号的映射")
        for key, value in expected.items():
            if not isinstance(key, str) or isinstance(value, bool) \
                    or not isinstance(value, int) or value < 0:
                raise ValidationError("expected_version 的值必须是非负整数")
        event_id = envelope.get("event_id")
        if event_id is not None and not isinstance(event_id, str):
            raise ValidationError("event_id 必须是字符串")
        return {
            "event_id": event_id or uuid.uuid4().hex,
            "event_type": event_type,
            "device_id": device_id,
            "local_sequence": local_sequence,
            "actor_id": actor_id.strip(),
            "occurred_at": occurred_at,
            "payload": payload,
            "expected_version": expected or None,
        }

    def _touch_device(self, connection, device_id: str | None, seen_at: str) -> None:
        if device_id is None:
            return
        connection.execute(
            "INSERT INTO devices(device_id,last_seen_at,last_sequence,last_occurred_at,created_at) "
            "VALUES(?,?,NULL,NULL,?) ON CONFLICT(device_id) DO UPDATE SET last_seen_at=excluded.last_seen_at",
            (device_id, seen_at, seen_at),
        )

    def _refresh_device(self, connection, device_id: str | None) -> None:
        """以回执与已确认事件为准重建设备游标。

        last_sequence 取已见过的最高本地序列（含待裁决事件，占位防跳号）；
        last_occurred_at 只取真正进入监管链的事件时间，待裁决/驳回不参与。
        """

        if device_id is None:
            return
        connection.execute(
            "UPDATE devices SET "
            "last_sequence=(SELECT MAX(local_sequence) FROM device_event_receipts "
            "WHERE device_id=?), "
            "last_occurred_at=(SELECT MAX(occurred_at) FROM custody_events WHERE device_id=?) "
            "WHERE device_id=?", (device_id, device_id, device_id))

    def _store_outcome(self, connection, env: dict[str, Any], envelope_hash: str,
                       request_id: str | None, *, outcome: str, response: dict[str, Any],
                       event_id: str | None = None, case_id: str | None = None,
                       store_device_receipt: bool = True) -> None:
        if env["device_id"] is not None and store_device_receipt:
            connection.execute(
                "INSERT INTO device_event_receipts(device_id,local_sequence,request_id,payload_hash,"
                "outcome,event_id,case_id,response_json,received_at) VALUES(?,?,?,?,?,?,?,?,?)",
                (env["device_id"], env["local_sequence"], request_id, envelope_hash, outcome,
                 event_id, case_id, canonical_json(response), self._now()),
            )
        if request_id:
            connection.execute(
                "INSERT INTO request_receipts(request_id,action,payload_hash,resource_type,"
                "resource_id,response_json,created_at) VALUES(?,?,?,?,?,?,?)",
                (request_id, "submit_custody_event", envelope_hash,
                 "custody_event" if outcome == "accepted" else "quarantine_case",
                 event_id or case_id or "", canonical_json(response), self._now()),
            )

    def _quarantine(self, connection, env: dict[str, Any], envelope_hash: str,
                    request_id: str | None, reason_code: str, detail: str, *,
                    duplicate_of: str | None = None,
                    store_device_receipt: bool = True) -> SubmitResult:
        case_id = uuid.uuid4().hex
        envelope_snapshot = {k: env[k] for k in
                             ("event_id", "event_type", "device_id", "local_sequence",
                              "actor_id", "occurred_at", "payload", "expected_version")}
        audit = append_event(
            connection, actor_id=env["actor_id"], action="custody.quarantined",
            resource_type="quarantine_case", resource_id=case_id,
            detail={"event_type": env["event_type"], "reason_code": reason_code,
                    "reason_detail": detail, "device_id": env["device_id"],
                    "local_sequence": env["local_sequence"],
                    "claimed_event_id": env["event_id"], "duplicate_of": duplicate_of,
                    "envelope_hash": envelope_hash},
            occurred_at=self._now(),
        )
        connection.execute(
            "INSERT INTO quarantine_cases(case_id,device_id,local_sequence,request_id,event_type,"
            "envelope_json,actor_id,occurred_at,reason_code,reason_detail,status,"
            "created_event_id,created_at) VALUES(?,?,?,?,?,?,?,?,?,?, 'pending',?,?)",
            (case_id, env["device_id"], env["local_sequence"], request_id, env["event_type"],
             canonical_json(envelope_snapshot), env["actor_id"], env["occurred_at"],
             reason_code, detail, audit["event_id"], self._now()),
        )
        result = {"accepted": False, "replayed": False, "event_id": None, "case_id": case_id,
                  "reason_code": reason_code, "reason_detail": detail}
        self._store_outcome(connection, env, envelope_hash, request_id, outcome="quarantined",
                            case_id=case_id, response=result,
                            store_device_receipt=store_device_receipt)
        self._refresh_device(connection, env["device_id"])
        return SubmitResult(False, False, None, case_id, reason_code, detail)

    # ------------------------------------------------------------- 事件应用

    def _container(self, connection, container_id: str) -> dict[str, Any] | None:
        row = connection.execute(
            "SELECT * FROM containers WHERE container_id=?", (container_id,)
        ).fetchone()
        return dict(row) if row else None

    def _expect_version(self, connection, env: dict[str, Any], versions: dict[str, int]) -> None:
        # 乐观版本是在线并发保护；管理员裁决接受的历史事件允许越过过期版本声明。
        if env.get("_strict") is False:
            return
        if not env["expected_version"]:
            return
        for cid, expected in env["expected_version"].items():
            current = versions.get(cid)
            if current is None:
                row = connection.execute(
                    "SELECT version FROM containers WHERE container_id=?", (cid,)
                ).fetchone()
                current = row["version"] if row else 0
            if current != expected:
                raise _Hold("state_conflict",
                            f"容器 {cid} 期望版本 {expected}，实际版本 {current}")

    def _require_container(self, connection, cid: Any, *, reason_unknown: str = "unknown_container"
                           ) -> dict[str, Any]:
        if not isinstance(cid, str) or not cid.strip():
            raise _Hold("validation_error", "container_id 缺失或无效")
        container = self._container(connection, cid)
        if container is None:
            raise _Hold(reason_unknown, f"容器 {cid} 不存在，事件无法接续")
        return container

    def _check_opening(self, connection, env: dict[str, Any], container: dict[str, Any],
                       opened_seal_id: Any, *, strict: bool) -> None:
        actor = self._actor_strict_or_quarantine(connection, env["actor_id"], strict)
        if strict and actor["role"] not in OPENING_ROLES:
            raise _Hold("unauthorized_open",
                        f"角色 {actor['role']} 无权开启容器 {container['container_id']}")
        if container["status"] == "sealed":
            if not isinstance(opened_seal_id, str) or not opened_seal_id.strip():
                if strict:
                    raise _Hold("seal_mismatch",
                                f"开启密封容器 {container['container_id']} 必须申报被破坏的封签编号")
            elif opened_seal_id != container["seal_id"]:
                if strict:
                    raise _Hold("seal_mismatch",
                                f"封签编号 {opened_seal_id} 与容器 {container['container_id']} "
                                f"当前封签 {container['seal_id']} 不符")
        elif opened_seal_id is not None and container["seal_id"] is not None \
                and opened_seal_id != container["seal_id"] and strict:
            raise _Hold("seal_mismatch",
                        f"申报封签 {opened_seal_id} 与登记封签 {container['seal_id']} 不符")

    def _actor_strict_or_quarantine(self, connection, actor_id: str, strict: bool) -> dict[str, Any]:
        row = connection.execute("SELECT * FROM actors WHERE actor_id=?", (actor_id,)).fetchone()
        if row is None or not row["active"]:
            if strict:
                raise _Hold("state_conflict", f"操作者 {actor_id} 不存在或已停用")
            return {"actor_id": actor_id, "role": "operator", "active": 1,
                    "organization_id": None, "display_name": actor_id}
        return dict(row)

    def _evaluate_environment(self, connection, env: dict[str, Any], container: dict[str, Any],
                              environment: Any, *, strict: bool) -> bool:
        """校验环境快照，返回容器是否发生越限。"""

        if environment is None:
            return bool(container["temp_excursion"])
        if not isinstance(environment, dict):
            if strict:
                raise _Hold("validation_error", "environment 必须是对象")
            return bool(container["temp_excursion"])
        temp = environment.get("temp_c")
        excursion = bool(container["temp_excursion"])
        if isinstance(temp, (int, float)) and not isinstance(temp, bool):
            tmin = environment.get("temp_min")
            tmax = environment.get("temp_max")
            if isinstance(tmin, (int, float)) and temp < tmin:
                excursion = True
            if isinstance(tmax, (int, float)) and temp > tmax:
                excursion = True
        return excursion

    def _record_event(self, connection, env: dict[str, Any], *, plan_id: str | None = None,
                      analysis_id: str | None = None, links: list[tuple[str, str, int]] | None = None,
                      adjudication: dict[str, Any] | None = None) -> str:
        existing = connection.execute(
            "SELECT payload_hash FROM custody_events WHERE event_id=?", (env["event_id"],)
        ).fetchone()
        if existing is not None:
            if existing["payload_hash"] == digest(env["payload"]):
                return env["event_id"]
            raise _Hold("duplicate_mismatch",
                        f"事件编号 {env['event_id']} 已被其他内容使用，不能覆盖已确认历史")
        payload_json = canonical_json(env["payload"])
        connection.execute(
            "INSERT INTO custody_events(event_id,device_id,local_sequence,event_type,actor_id,"
            "payload_json,payload_hash,expected_version,occurred_at,recorded_at,plan_id,"
            "analysis_id,adjudication_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (env["event_id"], env["device_id"], env["local_sequence"], env["event_type"],
             env["actor_id"], payload_json, digest(env["payload"]),
             canonical_json(env["expected_version"]) if env["expected_version"] else None,
             env["occurred_at"], self._now(), plan_id, analysis_id,
             canonical_json(adjudication) if adjudication else None),
        )
        for cid, role, ordinal in links or []:
            connection.execute(
                "INSERT INTO custody_event_containers(event_id,container_id,role,ordinal) "
                "VALUES(?,?,?,?)", (env["event_id"], cid, role, ordinal),
            )
        append_event(connection, actor_id=env["actor_id"],
                     action=f"custody.{env['event_type']}", resource_type="custody_event",
                     resource_id=env["event_id"],
                     detail={"event_type": env["event_type"], "device_id": env["device_id"],
                             "local_sequence": env["local_sequence"], "plan_id": plan_id,
                             "analysis_id": analysis_id,
                             "containers": [{"container_id": c, "role": r} for c, r, _ in (links or [])],
                             "payload_hash": digest(env["payload"]),
                             "adjudicated": bool(adjudication)},
                     occurred_at=env["occurred_at"])
        return env["event_id"]

    def _write_container(self, connection, container: dict[str, Any], env: dict[str, Any]) -> None:
        container["version"] = int(container["version"]) + 1
        container["last_device_id"] = env["device_id"]
        container["source_event_id"] = env["event_id"]
        container["updated_at"] = env["occurred_at"]
        connection.execute(
            "UPDATE containers SET status=?, holder_actor_id=?, holder_location=?, seal_id=?, "
            "sealed_by=?, mass_grams=?, version=?, temp_excursion=?, seal_anomaly=?, "
            "last_device_id=?, source_event_id=?, updated_at=? WHERE container_id=?",
            (container["status"], container["holder_actor_id"], container["holder_location"],
             container["seal_id"], container["sealed_by"], container["mass_grams"],
             container["version"], 1 if container["temp_excursion"] else 0,
             1 if container["seal_anomaly"] else 0, container["last_device_id"],
             container["source_event_id"], container["updated_at"], container["container_id"]),
        )

    def _create_container(self, connection, env: dict[str, Any], *, cid: str,
                          plan: dict[str, Any] | None, site_id: str, kind: str,
                          mass: float | None, holder_actor_id: str, location: str | None,
                          seal: dict[str, Any] | None, parents: list[str],
                          temp_excursion: bool = False) -> dict[str, Any]:
        sealed = isinstance(seal, dict) and bool(seal.get("seal_id"))
        container = {
            "container_id": cid, "plan_id": plan["plan_id"] if plan else None,
            "site_id": site_id, "kind": kind,
            "status": "sealed" if sealed else "in_custody",
            "holder_actor_id": holder_actor_id, "holder_location": location,
            "seal_id": seal.get("seal_id") if sealed else None,
            "sealed_by": seal.get("sealed_by", holder_actor_id) if sealed else None,
            "mass_grams": mass, "version": 1, "temp_excursion": temp_excursion,
            "seal_anomaly": False, "last_device_id": env["device_id"],
            "source_event_id": env["event_id"], "created_at": env["occurred_at"],
            "updated_at": env["occurred_at"],
        }
        connection.execute(
            "INSERT INTO containers(container_id,plan_id,site_id,kind,status,holder_actor_id,"
            "holder_location,seal_id,sealed_by,mass_grams,version,temp_excursion,seal_anomaly,"
            "last_device_id,source_event_id,created_at,updated_at) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (cid, container["plan_id"], site_id, kind, container["status"], holder_actor_id,
             location, container["seal_id"], container["sealed_by"], mass, 1,
             1 if temp_excursion else 0, 0, env["device_id"], env["event_id"],
             env["occurred_at"], env["occurred_at"]),
        )
        for ordinal, parent in enumerate(parents):
            connection.execute(
                "INSERT INTO container_parents(container_id,parent_id,event_id,ordinal) "
                "VALUES(?,?,?,?)", (cid, parent, env["event_id"], ordinal),
            )
        return container

    def _apply_event(self, connection, env: dict[str, Any], *, strict: bool,
                     adjudication: dict[str, Any] | None = None) -> str:
        etype = env["event_type"]
        payload = env["payload"]
        handler = getattr(self, f"_apply_{etype}")
        return handler(connection, env, payload, strict=strict, adjudication=adjudication)

    def _seal_dict(self, seal: Any, *, strict: bool) -> dict[str, Any] | None:
        if seal is None:
            return None
        if not isinstance(seal, dict) or not isinstance(seal.get("seal_id"), str) \
                or not seal["seal_id"].strip():
            if strict:
                raise _Hold("validation_error", "seal 必须包含非空 seal_id")
            return None
        return {"seal_id": seal["seal_id"].strip(),
                "sealed_by": seal.get("sealed_by") or None}

    def _apply_core_generated(self, connection, env, payload, *, strict, adjudication) -> str:
        actor = self._actor_strict_or_quarantine(connection, env["actor_id"], strict)
        if strict:
            self._require_roles(actor, WRITE_ROLES)
        plan_id = payload.get("plan_id")
        plan_row = connection.execute("SELECT * FROM sampling_plans WHERE plan_id=?",
                                      (plan_id,)).fetchone()
        if plan_row is None:
            raise _Hold("unknown_parent", f"采样计划 {plan_id} 不存在，岩芯无谱系来源")
        import json
        plan_payload = json.loads(plan_row["payload_json"])
        cid = payload.get("container_id")
        if not isinstance(cid, str) or not cid.strip():
            raise _Hold("validation_error", "container_id 缺失")
        spec = next((c for c in plan_payload["cores"] if c["container_id"] == cid), None)
        if spec is None:
            raise _Hold("parent_not_generated",
                        f"容器 {cid} 不在采样计划 {plan_id} 声明的岩芯清单内")
        existing = self._container(connection, cid)
        if existing is not None:
            raise _Hold("state_conflict", f"容器 {cid} 已经生成，不能重复生成")
        self._expect_version(connection, env, {cid: 0})
        kind = payload.get("kind") or spec["kind"]
        if kind != spec["kind"]:
            raise _Hold("state_conflict",
                        f"容器 {cid} 的 kind 与采样计划声明 {spec['kind']} 不一致")
        mass = self._mass(payload.get("mass_grams", spec.get("mass_grams")), "mass_grams",
                          allow_none=True)
        seal = self._seal_dict(payload.get("seal"), strict=strict)
        expected_seal = spec.get("expected_seal_id")
        if strict and expected_seal and seal and seal["seal_id"] != expected_seal:
            raise _Hold("seal_mismatch",
                        f"容器 {cid} 封签 {seal['seal_id']} 与计划预登封签 {expected_seal} 不符，"
                        "疑似离线补录")
        location = payload.get("location")
        excursion = self._evaluate_environment(connection, env,
                                            {"temp_excursion": 0},
                                            payload.get("environment"), strict=strict)
        container = self._create_container(
            connection, env, cid=cid,
            plan={"plan_id": plan_id}, site_id=plan_row["site_id"], kind=kind, mass=mass,
            holder_actor_id=env["actor_id"], location=location, seal=seal, parents=[],
            temp_excursion=excursion,
        )
        return self._record_event(connection, env, plan_id=plan_id,
                                 links=[(cid, "child", 0)], adjudication=adjudication)

    def _apply_aliquot(self, connection, env, payload, *, strict, adjudication) -> str:
        actor = self._actor_strict_or_quarantine(connection, env["actor_id"], strict)
        if strict:
            self._require_roles(actor, WRITE_ROLES)
        parent = self._require_container(connection, payload.get("parent_id"))
        cid = payload.get("container_id")
        if not isinstance(cid, str) or not cid.strip():
            raise _Hold("validation_error", "container_id 缺失")
        if self._container(connection, cid) is not None:
            raise _Hold("state_conflict", f"子容器 {cid} 已存在")
        if parent["status"] in TERMINAL_STATUSES or parent["status"] == "merged":
            raise _Hold("terminal_container",
                        f"父容器 {parent['container_id']} 状态为 {parent['status']}，不能分装")
        self._expect_version(connection, env,
                             {parent["container_id"]: parent["version"], cid: 0})
        self._check_opening(connection, env, parent, payload.get("opened_seal_id"), strict=strict)
        child_mass = self._mass(payload.get("mass_grams"), "mass_grams")
        if parent["mass_grams"] is not None:
            existing_children = connection.execute(
                "SELECT COALESCE(SUM(c.mass_grams),0) AS total FROM container_parents p "
                "JOIN containers c ON c.container_id=p.container_id "
                "WHERE p.parent_id=? AND p.event_id IN ("
                "SELECT event_id FROM custody_events WHERE event_type='aliquot')",
                (parent["container_id"],),
            ).fetchone()["total"]
            if existing_children + child_mass > parent["mass_grams"] + MASS_TOLERANCE_GRAMS:
                raise _Hold("mass_inconsistency",
                            f"从 {parent['container_id']} 累计分装 "
                            f"{existing_children + child_mass:.3f}g 超过父质量 "
                            f"{parent['mass_grams']:.3f}g")
        remaining = self._mass(payload.get("parent_mass_after"), "parent_mass_after",
                               allow_none=True)
        loss = self._mass(payload.get("loss_grams"), "loss_grams", allow_none=True) or 0.0
        if strict and parent["mass_grams"] is not None and remaining is not None:
            expected_remaining = parent["mass_grams"] - child_mass - loss
            if abs(remaining - expected_remaining) > MASS_TOLERANCE_GRAMS:
                raise _Hold("mass_inconsistency",
                            f"分装后质量 {remaining:.3f}g 与守恒值 "
                            f"{expected_remaining:.3f}g 不符")
        seal = self._seal_dict(payload.get("seal"), strict=strict)
        reseal = self._seal_dict(payload.get("reseal"), strict=strict)
        excursion = self._evaluate_environment(connection, env, parent,
                                            payload.get("environment"), strict=strict)
        child = self._create_container(
            connection, env, cid=cid, plan=None, site_id=parent["site_id"],
            kind=str(payload.get("kind") or parent["kind"]), mass=child_mass,
            holder_actor_id=env["actor_id"],
            location=payload.get("location") or parent["holder_location"],
            seal=seal, parents=[parent["container_id"]], temp_excursion=excursion,
        )
        if parent["status"] == "sealed":
            parent["status"] = "sealed" if reseal else "in_custody"
        if reseal:
            parent["seal_id"] = reseal["seal_id"]
            parent["sealed_by"] = reseal.get("sealed_by") or env["actor_id"]
            parent["status"] = "sealed"
        if remaining is not None:
            parent["mass_grams"] = remaining
        elif loss:
            parent["mass_grams"] = round((parent["mass_grams"] or 0) - child_mass - loss, 6)
        parent["temp_excursion"] = excursion
        self._write_container(connection, parent, env)
        return self._record_event(
            connection, env, links=[(parent["container_id"], "parent", 0), (cid, "child", 1)],
            adjudication=adjudication)

    def _apply_merge(self, connection, env, payload, *, strict, adjudication) -> str:
        actor = self._actor_strict_or_quarantine(connection, env["actor_id"], strict)
        if strict:
            self._require_roles(actor, WRITE_ROLES)
        parent_ids = payload.get("parent_ids")
        if not isinstance(parent_ids, list) or len(parent_ids) < 2:
            raise _Hold("validation_error", "merge 需要至少两个 parent_ids")
        if len(set(parent_ids)) != len(parent_ids):
            raise _Hold("validation_error", "merge 的父容器不能重复")
        parents = [self._require_container(connection, pid) for pid in parent_ids]
        versions = {p["container_id"]: p["version"] for p in parents}
        cid = payload.get("container_id")
        if not isinstance(cid, str) or not cid.strip():
            raise _Hold("validation_error", "container_id 缺失")
        if self._container(connection, cid) is not None:
            raise _Hold("state_conflict", f"合并产物容器 {cid} 已存在")
        versions[cid] = 0
        self._expect_version(connection, env, versions)
        for parent in parents:
            if parent["status"] in TERMINAL_STATUSES or parent["status"] == "merged":
                raise _Hold("terminal_container",
                            f"容器 {parent['container_id']} 状态 {parent['status']}，不能参与合并")
            if parent["status"] == "on_loan":
                raise _Hold("state_conflict",
                            f"容器 {parent['container_id']} 处于借出状态，不能合并")
        opened_seals = payload.get("opened_seal_ids") or []
        if strict and not isinstance(opened_seals, list):
            raise _Hold("validation_error", "opened_seal_ids 必须是数组")
        for parent in parents:
            seal_id = None
            if isinstance(opened_seals, list):
                seal_id = next((s for s in opened_seals
                                if isinstance(s, str) and s == parent["seal_id"]), None)
                if parent["status"] == "sealed" and seal_id is None:
                    seal_id = next((s for s in opened_seals if isinstance(s, str)), None)
            self._check_opening(connection, env, parent, seal_id, strict=strict)
        output_mass = self._mass(payload.get("mass_grams"), "mass_grams")
        loss = self._mass(payload.get("loss_grams"), "loss_grams", allow_none=True) or 0.0
        known = [p["mass_grams"] for p in parents if p["mass_grams"] is not None]
        if strict and len(known) == len(parents):
            total = sum(known)
            if output_mass > total + MASS_TOLERANCE_GRAMS:
                raise _Hold("mass_inconsistency",
                            f"合并产物 {output_mass:.3f}g 超过父质量之和 {total:.3f}g")
            if abs((total - loss) - output_mass) > MASS_TOLERANCE_GRAMS:
                raise _Hold("mass_inconsistency",
                            f"合并质量不守恒：父合计 {total:.3f}g - 损耗 {loss:.3f}g "
                            f"≠ 产物 {output_mass:.3f}g")
        seal = self._seal_dict(payload.get("seal"), strict=strict)
        excursion = False
        for parent in parents:
            excursion = self._evaluate_environment(connection, env, parent,
                                                payload.get("environment"), strict=strict) or excursion
        site_id = parents[0]["site_id"]
        if any(p["site_id"] != site_id for p in parents):
            raise _Hold("state_conflict", "不能跨站点合并容器")
        child = self._create_container(
            connection, env, cid=cid, plan=None, site_id=site_id,
            kind=str(payload.get("kind") or "merged"), mass=output_mass,
            holder_actor_id=env["actor_id"], location=payload.get("location"),
            seal=seal, parents=parent_ids, temp_excursion=excursion,
        )
        links = [(pid, "parent", i) for i, pid in enumerate(parent_ids)]
        links.append((cid, "child", len(parent_ids)))
        for parent in parents:
            parent["status"] = "merged"
            parent["holder_actor_id"] = env["actor_id"]
            parent["temp_excursion"] = parent["temp_excursion"] or excursion
            self._write_container(connection, parent, env)
        return self._record_event(connection, env, links=links, adjudication=adjudication)

    def _apply_weigh(self, connection, env, payload, *, strict, adjudication) -> str:
        actor = self._actor_strict_or_quarantine(connection, env["actor_id"], strict)
        if strict:
            self._require_roles(actor, WRITE_ROLES)
        container = self._require_container(connection, payload.get("container_id"))
        if container["status"] in TERMINAL_STATUSES or container["status"] == "merged":
            raise _Hold("terminal_container", "容器已终态，不能再称量")
        self._expect_version(connection, env, {container["container_id"]: container["version"]})
        mass = self._mass(payload.get("mass_grams"), "mass_grams")
        calibration = payload.get("calibration")
        if strict and not isinstance(calibration, dict):
            raise _Hold("validation_error", "称量必须记录校准依据 calibration 对象")
        for field in ("instrument_id", "calibration_id"):
            if strict and not isinstance(calibration.get(field), str):
                raise _Hold("validation_error", f"calibration.{field} 缺失")
        calibrated_at = self._parse_occurred(calibration.get("calibrated_at")) if calibration else None
        if strict and calibrated_at is None:
            raise _Hold("validation_error", "calibration.calibrated_at 必须是 ISO8601 时间")
        container["mass_grams"] = mass
        container["temp_excursion"] = self._evaluate_environment(
            connection, env, container, payload.get("environment"), strict=strict)
        self._write_container(connection, container, env)
        return self._record_event(connection, env,
                                 links=[(container["container_id"], "target", 0)],
                                 adjudication=adjudication)

    def _apply_seal(self, connection, env, payload, *, strict, adjudication) -> str:
        actor = self._actor_strict_or_quarantine(connection, env["actor_id"], strict)
        if strict:
            self._require_roles(actor, WRITE_ROLES)
        container = self._require_container(connection, payload.get("container_id"))
        self._expect_version(connection, env, {container["container_id"]: container["version"]})
        seal = self._seal_dict(payload, strict=strict)
        if container["status"] in TERMINAL_STATUSES or container["status"] == "merged":
            raise _Hold("terminal_container", "容器已终态，不能加封")
        if container["status"] == "sealed":
            if strict and container["seal_id"] != seal["seal_id"]:
                raise _Hold("seal_mismatch",
                            f"容器已持封签 {container['seal_id']}，未开封不能换贴 {seal['seal_id']}")
            if strict:
                raise _Hold("state_conflict", "容器已处于加封状态")
        container["status"] = "sealed"
        container["seal_id"] = seal["seal_id"]
        container["sealed_by"] = seal.get("sealed_by") or env["actor_id"]
        self._write_container(connection, container, env)
        return self._record_event(connection, env,
                                 links=[(container["container_id"], "target", 0)],
                                 adjudication=adjudication)

    def _apply_open(self, connection, env, payload, *, strict, adjudication) -> str:
        actor = self._actor_strict_or_quarantine(connection, env["actor_id"], strict)
        container = self._require_container(connection, payload.get("container_id"))
        self._expect_version(connection, env, {container["container_id"]: container["version"]})
        if container["status"] in TERMINAL_STATUSES or container["status"] == "merged":
            raise _Hold("terminal_container", "容器已终态，不能开封")
        if container["status"] == "on_loan":
            raise _Hold("state_conflict", "借出未归还的容器不能在站内开封")
        self._check_opening(connection, env, container, payload.get("opened_seal_id"), strict=strict)
        container["status"] = "in_custody"
        self._write_container(connection, container, env)
        return self._record_event(connection, env,
                                 links=[(container["container_id"], "target", 0)],
                                 adjudication=adjudication)

    def _apply_transfer(self, connection, env, payload, *, strict, adjudication) -> str:
        actor = self._actor_strict_or_quarantine(connection, env["actor_id"], strict)
        if strict:
            self._require_roles(actor, WRITE_ROLES)
        items = payload.get("items")
        if not isinstance(items, list) or not items:
            raise _Hold("validation_error", "transfer 需要非空 items 数组")
        containers: list[dict[str, Any]] = []
        versions: dict[str, int] = {}
        for item in items:
            if not isinstance(item, dict):
                raise _Hold("validation_error", "transfer 的每项必须是对象")
            container = self._require_container(connection, item.get("container_id"))
            if container["status"] in TERMINAL_STATUSES or container["status"] == "merged":
                raise _Hold("terminal_container", "终态容器不能交接")
            to_actor = item.get("to_actor_id")
            to_row = connection.execute("SELECT * FROM actors WHERE actor_id=?",
                                        (to_actor,)).fetchone()
            if to_row is None or not to_row["active"]:
                raise _Hold("state_conflict", f"接收人 {to_actor} 不存在或已停用")
            containers.append(container)
            versions[container["container_id"]] = container["version"]
        self._expect_version(connection, env, versions)
        links = []
        for ordinal, (item, container) in enumerate(zip(items, containers)):
            if container["status"] == "on_loan":
                raise _Hold("state_conflict",
                            f"容器 {container['container_id']} 借出中，必须先归还")
            container["holder_actor_id"] = item["to_actor_id"]
            container["holder_location"] = item.get("to_location")
            self._write_container(connection, container, env)
            links.append((container["container_id"], "item", ordinal))
        return self._record_event(connection, env, links=links, adjudication=adjudication)

    def _apply_loan(self, connection, env, payload, *, strict, adjudication) -> str:
        actor = self._actor_strict_or_quarantine(connection, env["actor_id"], strict)
        if strict:
            self._require_roles(actor, WRITE_ROLES)
        borrower_id = payload.get("borrower_actor_id")
        borrower = connection.execute("SELECT * FROM actors WHERE actor_id=?",
                                      (borrower_id,)).fetchone()
        if borrower is None or not borrower["active"]:
            raise _Hold("state_conflict", f"借阅人 {borrower_id} 不存在或已停用")
        due_at = self._parse_occurred(payload.get("due_at")) if payload.get("due_at") else None
        if payload.get("due_at") and due_at is None:
            raise _Hold("validation_error", "due_at 必须是 ISO8601 时间")
        items = payload.get("items")
        if not isinstance(items, list) or not items:
            raise _Hold("validation_error", "loan 需要非空 items 数组")
        containers = []
        versions: dict[str, int] = {}
        for item in items:
            cid = item.get("container_id") if isinstance(item, dict) else None
            container = self._require_container(connection, cid)
            if strict and container["status"] != "sealed":
                raise _Hold("state_conflict",
                            f"容器 {cid} 未加封（{container['status']}），不能借出")
            containers.append(container)
            versions[cid] = container["version"]
        self._expect_version(connection, env, versions)
        links = []
        for ordinal, container in enumerate(containers):
            container["status"] = "on_loan"
            container["holder_actor_id"] = borrower_id
            self._write_container(connection, container, env)
            links.append((container["container_id"], "item", ordinal))
        return self._record_event(connection, env, links=links, adjudication=adjudication)

    def _apply_return(self, connection, env, payload, *, strict, adjudication) -> str:
        actor = self._actor_strict_or_quarantine(connection, env["actor_id"], strict)
        items = payload.get("items")
        if not isinstance(items, list) or not items:
            raise _Hold("validation_error", "return 需要非空 items 数组")
        containers = []
        versions: dict[str, int] = {}
        for item in items:
            if not isinstance(item, dict):
                raise _Hold("validation_error", "return 的每项必须是对象")
            container = self._require_container(connection, item.get("container_id"))
            containers.append((item, container))
            versions[container["container_id"]] = container["version"]
        self._expect_version(connection, env, versions)
        links = []
        for ordinal, (item, container) in enumerate(containers):
            if strict and container["status"] != "on_loan":
                raise _Hold("state_conflict",
                            f"容器 {container['container_id']} 未处于借出状态")
            seal_id = item.get("seal_id")
            intact = bool(item.get("seal_intact", True))
            if strict and (not isinstance(seal_id, str) or not seal_id.strip()):
                raise _Hold("seal_mismatch", "归还必须申报当前封签编号")
            if seal_id != container["seal_id"] or not intact:
                # 归还事件本身成立，但封签异常被永久标记并进入管理员告警。
                container["seal_anomaly"] = True
            mass = self._mass(item.get("mass_grams"), "mass_grams", allow_none=True)
            if mass is not None:
                container["mass_grams"] = mass
            container["status"] = "sealed"
            container["holder_actor_id"] = env["actor_id"]
            container["holder_location"] = item.get("to_location")
            self._write_container(connection, container, env)
            links.append((container["container_id"], "item", ordinal))
        return self._record_event(connection, env, links=links, adjudication=adjudication)

    def _apply_consume(self, connection, env, payload, *, strict, adjudication) -> str:
        actor = self._actor_strict_or_quarantine(connection, env["actor_id"], strict)
        if strict:
            self._require_roles(actor, WRITE_ROLES)
        container = self._require_container(connection, payload.get("container_id"))
        self._expect_version(connection, env, {container["container_id"]: container["version"]})
        if container["status"] in TERMINAL_STATUSES or container["status"] == "merged":
            raise _Hold("terminal_container", f"容器 {container['container_id']} 已终态")
        self._check_opening(connection, env, container, payload.get("opened_seal_id"), strict=strict)
        amount = self._mass(payload.get("amount_grams"), "amount_grams")
        if strict and container["mass_grams"] is not None and amount > container["mass_grams"] \
                + MASS_TOLERANCE_GRAMS:
            raise _Hold("mass_inconsistency",
                        f"消耗量 {amount:.3f}g 超过容器现有质量 {container['mass_grams']:.3f}g")
        container["status"] = "consumed"
        container["holder_location"] = payload.get("location") or container["holder_location"]
        self._write_container(connection, container, env)
        return self._record_event(connection, env,
                                 links=[(container["container_id"], "target", 0)],
                                 adjudication=adjudication)

    def _apply_environment(self, connection, env, payload, *, strict, adjudication) -> str:
        # 环境读数不改变保管关系，但提交者仍必须是有效操作者。
        self._actor_strict_or_quarantine(connection, env["actor_id"], strict)
        container = self._require_container(connection, payload.get("container_id"))
        self._expect_version(connection, env, {container["container_id"]: container["version"]})
        excursion = self._evaluate_environment(connection, env, container, payload, strict=strict)
        container["temp_excursion"] = excursion
        self._write_container(connection, container, env)
        return self._record_event(connection, env,
                                 links=[(container["container_id"], "target", 0)],
                                 adjudication=adjudication)

    def _apply_analysis(self, connection, env, payload, *, strict, adjudication) -> str:
        actor = self._actor_strict_or_quarantine(connection, env["actor_id"], strict)
        if strict and actor["role"] not in OPENING_ROLES:
            raise _Hold("unauthorized_open", f"角色 {actor['role']} 不能提交派生分析")
        container = self._require_container(connection, payload.get("container_id"))
        self._expect_version(connection, env, {container["container_id"]: container["version"]})
        if container["status"] in TERMINAL_STATUSES or container["status"] == "merged":
            raise _Hold("terminal_container", "终态容器不能再做分析")
        if container["status"] == "on_loan":
            raise _Hold("state_conflict", "借出中的容器必须先归还才能分析")
        analysis_id = payload.get("analysis_id")
        if not isinstance(analysis_id, str) or not analysis_id.strip():
            raise _Hold("validation_error", "analysis_id 缺失")
        if connection.execute("SELECT 1 FROM analyses WHERE analysis_id=?",
                              (analysis_id,)).fetchone():
            raise _Hold("state_conflict", f"分析 {analysis_id} 已存在")
        method = payload.get("method")
        if not isinstance(method, str) or not method.strip():
            raise _Hold("validation_error", "method 缺失")
        result = payload.get("result")
        if not isinstance(result, dict) or not result:
            raise _Hold("validation_error", "result 必须是非空对象")
        instrument = payload.get("instrument")
        if strict and not isinstance(instrument, dict):
            raise _Hold("validation_error", "分析必须记录仪器与校准依据 instrument 对象")
        if strict:
            for field in ("instrument_id", "calibration_id"):
                if not isinstance(instrument.get(field), str):
                    raise _Hold("validation_error", f"instrument.{field} 缺失")
            calibrated_at = self._parse_occurred(instrument.get("calibrated_at"))
            if calibrated_at is None:
                raise _Hold("validation_error",
                            "instrument.calibrated_at 必须是 ISO8601 时间")
        amount = self._mass(payload.get("amount_grams"), "amount_grams", allow_none=True)
        self._check_opening(connection, env, container, payload.get("opened_seal_id"), strict=strict)
        if amount is not None and container["mass_grams"] is not None and strict \
                and amount > container["mass_grams"] + MASS_TOLERANCE_GRAMS:
            raise _Hold("mass_inconsistency", "分析用量超过容器现有质量")
        event_id = self._record_event(connection, env, analysis_id=analysis_id,
                                      links=[(container["container_id"], "target", 0)],
                                      adjudication=adjudication)
        connection.execute(
            "INSERT INTO analyses(analysis_id,container_id,method,result_json,result_hash,"
            "instrument_json,event_id,created_by,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
            (analysis_id, container["container_id"], method, canonical_json(result),
             digest(result), canonical_json(instrument) if instrument else None,
             event_id, env["actor_id"], env["occurred_at"]),
        )
        if container["status"] == "sealed":
            container["status"] = "in_custody"
        if amount is not None and container["mass_grams"] is not None:
            container["mass_grams"] = round(container["mass_grams"] - amount, 6)
        self._write_container(connection, container, env)
        return event_id

    # ------------------------------------------------------------- 隔离裁决

    def list_quarantine(self, status: str | None = None) -> list[QuarantineCase]:
        query = "SELECT * FROM quarantine_cases"
        parameters: list[Any] = []
        if status:
            query += " WHERE status=?"
            parameters.append(status)
        query += " ORDER BY created_at, case_id"
        return [self._case_model(dict(row))
                for row in self.database.connection.execute(query, parameters)]

    def get_case(self, case_id: str) -> QuarantineCase:
        row = self.database.connection.execute(
            "SELECT * FROM quarantine_cases WHERE case_id=?", (case_id,)
        ).fetchone()
        if row is None:
            raise NotFoundError("隔离案件不存在")
        return self._case_model(dict(row))

    def _case_model(self, row: dict[str, Any]) -> QuarantineCase:
        return QuarantineCase(
            case_id=row["case_id"], device_id=row["device_id"],
            local_sequence=row["local_sequence"], request_id=row["request_id"],
            event_type=row["event_type"], envelope=_json_loads(row["envelope_json"]),
            actor_id=row["actor_id"], occurred_at=row["occurred_at"],
            reason_code=row["reason_code"], reason_detail=row["reason_detail"],
            status=row["status"], created_event_id=row["created_event_id"],
            decided_by=row["decided_by"], decided_at=row["decided_at"],
            decision_note=row["decision_note"], applied_event_id=row["applied_event_id"],
            created_at=row["created_at"])

    def adjudicate_case(self, *, request_id: str, actor_id: str, case_id: str,
                        decision: str, note: str | None = None) -> WriteReceipt:
        """管理员对隔离事件做出裁决；接受只会追加历史，绝不改写已确认事件。"""

        if decision not in ("accept", "reject"):
            raise ValidationError("decision 必须是 accept 或 reject")
        payload = {"actor_id": actor_id, "case_id": case_id, "decision": decision, "note": note}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require_roles(actor, frozenset({"admin"}))
            case_row = connection.execute(
                "SELECT * FROM quarantine_cases WHERE case_id=?", (case_id,)
            ).fetchone()
            if case_row is None:
                raise NotFoundError("隔离案件不存在")
            case = dict(case_row)
            if case["status"] != "pending":
                raise ConflictError(f"案件已经裁决为 {case['status']}")

            def create() -> tuple[str, str, dict[str, Any]]:
                decided_at = self._now()
                applied_event_id = None
                if decision == "accept":
                    env = _json_loads(case["envelope_json"])
                    env["_strict"] = False
                    adjudication = {"case_id": case_id, "decided_by": actor_id,
                                    "decided_at": decided_at, "note": note,
                                    "original_reason": case["reason_code"]}
                    try:
                        applied_event_id = self._apply_event(
                            connection, env, strict=False, adjudication=adjudication)
                    except _Hold as hold:
                        raise ConflictError(
                            f"事件仍无法接续，不能接受：{hold.reason_code} {hold.detail}") from hold
                    connection.execute(
                        "UPDATE device_event_receipts SET outcome='accepted', event_id=? "
                        "WHERE case_id=? AND outcome='quarantined'",
                        (applied_event_id, case_id))
                    if decision == "reject":
                        connection.execute(
                            "UPDATE device_event_receipts SET outcome='rejected' "
                            "WHERE case_id=? AND outcome='quarantined'", (case_id,))
                # 序列游标按已见最高序列占位，时间游标只认真正入链的事件；
                # 接受与驳回都据此重算，裁决只追加历史，不改写已确认事件。
                self._refresh_device(connection, case["device_id"])
                connection.execute(
                    "UPDATE quarantine_cases SET status=?, decided_by=?, decided_at=?, "
                    "decision_note=?, applied_event_id=? WHERE case_id=?",
                    ("accepted" if decision == "accept" else "rejected",
                     actor_id, decided_at, note, applied_event_id, case_id))
                audit = append_event(
                    connection, actor_id=actor_id,
                    action=f"custody.case_{decision}ed", resource_type="quarantine_case",
                    resource_id=case_id,
                    detail={"decision": decision, "note": note,
                            "applied_event_id": applied_event_id,
                            "original_reason": case["reason_code"]},
                    occurred_at=decided_at)
                return ("quarantine_case", case_id,
                        {"case_id": case_id, "decision": decision,
                         "applied_event_id": applied_event_id, "audit_event_id": audit["event_id"]})

            return self._idempotent(connection, request_id=request_id,
                                    action=f"adjudicate_case_{decision}", payload=payload,
                                    create=create)

    # ----------------------------------------------------------------- 查询

    def get_container(self, container_id: str) -> Container:
        row = self.database.connection.execute(
            "SELECT * FROM containers WHERE container_id=?", (container_id,)
        ).fetchone()
        if row is None:
            raise NotFoundError("容器不存在")
        return self._container_model(dict(row))

    def _container_model(self, row: dict[str, Any]) -> Container:
        parents = tuple(r["parent_id"] for r in self.database.connection.execute(
            "SELECT parent_id FROM container_parents WHERE container_id=? ORDER BY ordinal",
            (row["container_id"],)))
        return Container(
            container_id=row["container_id"], plan_id=row["plan_id"], site_id=row["site_id"],
            kind=row["kind"], status=row["status"], holder_actor_id=row["holder_actor_id"],
            holder_location=row["holder_location"], seal_id=row["seal_id"],
            sealed_by=row["sealed_by"], mass_grams=row["mass_grams"], version=row["version"],
            temp_excursion=bool(row["temp_excursion"]), seal_anomaly=bool(row["seal_anomaly"]),
            last_device_id=row["last_device_id"], source_event_id=row["source_event_id"],
            parents=parents, created_at=row["created_at"], updated_at=row["updated_at"])

    def list_containers(self, *, plan_id: str | None = None,
                        site_id: str | None = None, status: str | None = None) -> list[Container]:
        query = "SELECT * FROM containers"
        clauses: list[str] = []
        parameters: list[Any] = []
        if plan_id is not None:
            clauses.append("plan_id=?")
            parameters.append(plan_id)
        if site_id is not None:
            clauses.append("site_id=?")
            parameters.append(site_id)
        if status is not None:
            clauses.append("status=?")
            parameters.append(status)
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY created_at, container_id"
        return [self._container_model(dict(row))
                for row in self.database.connection.execute(query, parameters)]

    def list_events(self, container_id: str | None = None) -> list[CustodyEvent]:
        conn = self.database.connection
        if container_id is None:
            rows = conn.execute(
                "SELECT * FROM custody_events ORDER BY occurred_at, rowid").fetchall()
        else:
            rows = conn.execute(
                "SELECT ce.* FROM custody_events ce "
                "JOIN custody_event_containers cec ON cec.event_id=ce.event_id "
                "WHERE cec.container_id=? ORDER BY ce.occurred_at, ce.rowid",
                (container_id,)).fetchall()
        return [self._event_model(dict(row)) for row in rows]

    def _event_model(self, row: dict[str, Any]) -> CustodyEvent:
        links = tuple((r["container_id"], r["role"]) for r in self.database.connection.execute(
            "SELECT container_id,role FROM custody_event_containers WHERE event_id=? "
            "ORDER BY ordinal", (row["event_id"],)))
        return CustodyEvent(
            event_id=row["event_id"], device_id=row["device_id"],
            local_sequence=row["local_sequence"], event_type=row["event_type"],
            actor_id=row["actor_id"], payload=_json_loads(row["payload_json"]),
            expected_version=row["expected_version"], occurred_at=row["occurred_at"],
            recorded_at=row["recorded_at"], plan_id=row["plan_id"],
            analysis_id=row["analysis_id"], containers=links)

    def ancestors(self, container_id: str) -> list[str]:
        """递归返回全部父容器（含合并的多父），按谱系深度排序。"""

        rows = self.database.connection.execute(
            "WITH RECURSIVE lineage(a, depth) AS ("
            "SELECT parent_id, 1 FROM container_parents WHERE container_id=? "
            "UNION ALL "
            "SELECT p.parent_id, l.depth+1 FROM container_parents p "
            "JOIN lineage l ON p.container_id=l.a) "
            "SELECT a, MIN(depth) AS d FROM lineage GROUP BY a ORDER BY d, a",
            (container_id,)).fetchall()
        return [r["a"] for r in rows]

    def lineage(self, container_id: str) -> dict[str, Any]:
        container = self.get_container(container_id)
        ancestor_ids = self.ancestors(container_id)
        events = self.list_events(container_id)
        return {
            "container": container.__dict__,
            "parents": [self.get_container(cid).__dict__ for cid in ancestor_ids],
            "events": [_event_dict(e) for e in events],
        }

    def get_analysis(self, analysis_id: str) -> Analysis:
        row = self.database.connection.execute(
            "SELECT * FROM analyses WHERE analysis_id=?", (analysis_id,)
        ).fetchone()
        if row is None:
            raise NotFoundError("分析结果不存在")
        return Analysis(row["analysis_id"], row["container_id"], row["method"],
                        _json_loads(row["result_json"]),
                        _json_loads(row["instrument_json"]) if row["instrument_json"] else None,
                        row["event_id"], row["created_by"], row["created_at"])

    def analysis_provenance(self, analysis_id: str) -> dict[str, Any]:
        """还原分析结果对应的容器谱系与每次保管转移。"""

        analysis = self.get_analysis(analysis_id)
        container = self.get_container(analysis.container_id)
        ancestor_ids = self.ancestors(analysis.container_id)
        subtree = [analysis.container_id, *ancestor_ids]
        placeholders = ",".join("?" * len(subtree))
        conn = self.database.connection
        timeline: list[dict[str, Any]] = []
        rows = conn.execute(
            "SELECT ce.* FROM custody_events ce "
            "JOIN custody_event_containers cec ON cec.event_id=ce.event_id "
            f"WHERE cec.container_id IN ({placeholders}) "
            "GROUP BY ce.event_id ORDER BY ce.occurred_at, ce.rowid",
            tuple(subtree)).fetchall()
        for row in rows:
            event = self._event_model(dict(row))
            item = _event_dict(event)
            item["containers_in_lineage"] = sorted(
                {cid for cid, _role in event.containers if cid in set(subtree)})
            timeline.append(item)
        plan = None
        if container.plan_id:
            plan = self.get_plan(container.plan_id)
        return {
            "analysis": analysis.__dict__,
            "container": container.__dict__,
            "ancestors": [self.get_container(cid).__dict__ for cid in ancestor_ids],
            "plan": plan,
            "custody_timeline": timeline,
            "analysis_event": _event_dict(self._event_model(dict(conn.execute(
                "SELECT * FROM custody_events WHERE event_id=?",
                (analysis.event_id,)).fetchone()))),
        }

    # ------------------------------------------------------------- 告警/核对

    def list_alerts(self, *, offline_before: str) -> dict[str, Any]:
        """识别失联设备、超温、封签异常、超期借出和待裁决样品。"""

        cutoff = self._parse_occurred(offline_before)
        if cutoff is None:
            raise ValidationError("offline_before 必须是 ISO8601 时间")
        conn = self.database.connection
        offline = [dict(r) for r in conn.execute(
            "SELECT device_id,last_seen_at,last_sequence,last_occurred_at FROM devices "
            "WHERE last_seen_at<? ORDER BY last_seen_at", (cutoff,))]
        temperature = [self._container_model(dict(r)).__dict__ for r in conn.execute(
            "SELECT * FROM containers WHERE temp_excursion=1 ORDER BY container_id")]
        seals = [self._container_model(dict(r)).__dict__ for r in conn.execute(
            "SELECT * FROM containers WHERE seal_anomaly=1 ORDER BY container_id")]
        pending = [self._case_model(dict(r)).__dict__ for r in conn.execute(
            "SELECT * FROM quarantine_cases WHERE status='pending' ORDER BY created_at")]
        overdue: list[dict[str, Any]] = []
        for row in conn.execute("SELECT * FROM containers WHERE status='on_loan'"):
            cid = row["container_id"]
            loan = conn.execute(
                "SELECT payload_json FROM custody_events WHERE event_type='loan' AND event_id IN "
                "(SELECT event_id FROM custody_event_containers WHERE container_id=?) "
                "ORDER BY occurred_at DESC, rowid DESC LIMIT 1", (cid,)).fetchone()
            if loan:
                payload = _json_loads(loan["payload_json"])
                due = self._parse_occurred(payload.get("due_at")) if payload.get("due_at") else None
                if due and due < cutoff:
                    overdue.append({"container_id": cid, "due_at": due,
                                    "holder_actor_id": row["holder_actor_id"]})
        return {"offline_devices": offline, "temperature_excursions": temperature,
                "seal_anomalies": seals, "pending_quarantine": pending,
                "overdue_loans": overdue, "checked_at": self._now()}

    def conservation_report(self, plan_id: str | None = None) -> list[dict[str, Any]]:
        """核对父子样品质量没有凭空增加或消失。

        以已确认的监管事件为唯一事实来源重放质量台账：每个计划根容器都有一个质量池，
        分装与合并按比例把池中的质量分配到新容器，损耗、消耗、分析用量分别记账；
        最后现存容器中各池质量之和必须等于 初始质量 - 损耗 - 消耗/分析用量。
        跨计划合并按父容器当时的构成比例分摊，不会重复计数。
        """

        conn = self.database.connection
        if plan_id is not None and conn.execute(
                "SELECT 1 FROM sampling_plans WHERE plan_id=?", (plan_id,)).fetchone() is None:
            raise NotFoundError("采样计划不存在")

        shares: dict[str, dict[str, float]] = {}
        created: dict[str, float] = {}
        root_plan: dict[str, str] = {}
        losses: dict[str, float] = {}
        gone: dict[str, float] = {}

        def add(ledger: dict[str, float], root: str, value: float) -> None:
            ledger[root] = round(ledger.get(root, 0.0) + value, 9)

        def scale(parts: dict[str, float], total: float) -> dict[str, float]:
            base = sum(parts.values())
            if base <= 0:
                return {}
            factor = total / base
            return {root: round(amount * factor, 9) for root, amount in parts.items()}

        rows = conn.execute(
            "SELECT event_id, event_type, payload_json FROM custody_events "
            "ORDER BY occurred_at, rowid").fetchall()
        for row in rows:
            etype = row["event_type"]
            payload = _json_loads(row["payload_json"])

            if etype == "core_generated":
                cid = payload["container_id"]
                mass = payload.get("mass_grams")
                if isinstance(mass, (int, float)) and not isinstance(mass, bool):
                    shares[cid] = {cid: float(mass)}
                    created[cid] = float(mass)
                    root_plan[cid] = payload.get("plan_id")
                continue

            if etype == "aliquot":
                parent = payload["parent_id"]
                child = payload["container_id"]
                child_mass = float(payload.get("mass_grams") or 0.0)
                loss = float(payload.get("loss_grams") or 0.0)
                parts = dict(shares.get(parent, {}))
                total = sum(parts.values())
                if total > 0:
                    shares[child] = scale(parts, child_mass)
                    for root, amount in scale(parts, loss).items():
                        add(losses, root, amount)
                    remaining = max(total - child_mass - loss, 0.0)
                    shares[parent] = scale(parts, remaining)
                else:
                    shares[child] = {}
                continue

            if etype == "merge":
                parents = payload.get("parent_ids") or []
                child = payload["container_id"]
                output_mass = float(payload.get("mass_grams") or 0.0)
                loss = float(payload.get("loss_grams") or 0.0)
                combined: dict[str, float] = {}
                for pid in parents:
                    for root, amount in shares.pop(pid, {}).items():
                        combined[root] = round(combined.get(root, 0.0) + amount, 9)
                shares[child] = scale(combined, output_mass)
                for root, amount in scale(combined, loss).items():
                    add(losses, root, amount)
                continue

            if etype == "weigh":
                cid = payload["container_id"]
                mass = float(payload.get("mass_grams") or 0.0)
                if cid in shares:
                    shares[cid] = scale(shares[cid], mass)
                continue

            if etype == "return":
                for item in payload.get("items") or []:
                    cid = item.get("container_id")
                    mass = item.get("mass_grams")
                    if cid in shares and isinstance(mass, (int, float))                             and not isinstance(mass, bool):
                        shares[cid] = scale(shares[cid], float(mass))
                continue

            if etype in ("consume", "analysis"):
                cid = payload["container_id"]
                amount = float(payload.get("amount_grams") or 0.0)
                parts = dict(shares.get(cid, {}))
                total = sum(parts.values())
                if total > 0 and amount > 0:
                    used = scale(parts, min(amount, total))
                    for root, value in used.items():
                        add(gone, root, value)
                    shares[cid] = scale(parts, max(total - amount, 0.0))
                continue

        statuses = {r["container_id"]: r["status"] for r in conn.execute(
            "SELECT container_id, status FROM containers")}
        live: dict[str, float] = {}
        for cid, parts in shares.items():
            if statuses.get(cid) in ("consumed", "merged", "destroyed"):
                continue
            for root, amount in parts.items():
                add(live, root, amount)

        report: list[dict[str, Any]] = []
        for root_id in sorted(created):
            if plan_id is not None and root_plan.get(root_id) != plan_id:
                continue
            initial = round(created[root_id], 6)
            loss_total = round(losses.get(root_id, 0.0), 6)
            gone_total = round(gone.get(root_id, 0.0), 6)
            live_total = round(live.get(root_id, 0.0), 6)
            expected_live = round(initial - loss_total - gone_total, 6)
            discrepancy = round(live_total - expected_live, 6)
            report.append({
                "plan_id": root_plan.get(root_id),
                "root_container_id": root_id,
                "initial_mass_grams": initial,
                "live_mass_grams": live_total,
                "consumed_or_analyzed_grams": gone_total,
                "recorded_loss_grams": loss_total,
                "expected_live_grams": expected_live,
                "discrepancy_grams": discrepancy,
                "issue": None if abs(discrepancy) <= MASS_TOLERANCE_GRAMS
                else ("mass_increased" if discrepancy > 0 else "mass_missing"),
            })
        return report

    def verify_audit(self) -> tuple[bool, int]:
        from .audit import verify_chain
        return verify_chain(self.database.connection)


def _json_loads(text: str) -> Any:
    import json
    return json.loads(text)


def _event_dict(event: CustodyEvent) -> dict[str, Any]:
    return {"event_id": event.event_id, "event_type": event.event_type,
            "actor_id": event.actor_id, "occurred_at": event.occurred_at,
            "recorded_at": event.recorded_at, "device_id": event.device_id,
            "local_sequence": event.local_sequence, "payload": event.payload,
            "containers": [{"container_id": c, "role": r} for c, r in event.containers]}
