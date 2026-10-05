"""运行样品监管链的离线端到端验收。

场景还原需求背景：采样计划生成岩芯容器，分装、借还、称量、派生分析、合并、
消耗沿父子谱系推进；野外终端恢复连接后补录历史事件，其中混入了重复上传、
时钟倒退、越权开封、数量矛盾和无法接续的事件，全部进入隔离裁决；
最后还原分析结果溯源、管理员异常总览与父子数量核对。
"""

from __future__ import annotations

import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from polar_station_foundation.clock import FixedClock
from polar_station_foundation.errors import ConflictError
from polar_station_foundation.service import DomainService

from .service import CustodyService
from .storage import CustodyDatabase


def _clock(month: int, day: int, hour: int = 8) -> FixedClock:
    return FixedClock(datetime(2026, month, day, hour, 0, tzinfo=timezone.utc))


def run() -> dict[str, object]:
    """执行完整监管链场景并返回验收结果。"""

    with tempfile.TemporaryDirectory() as directory:
        database = CustodyDatabase(Path(directory) / "acceptance.sqlite3")
        clock = _clock(9, 28)
        foundation = DomainService(database, clock)
        service = CustodyService(database, clock)

        # 建档：机构、人员、站点、设备与版本化依据文档
        foundation.register_organization(request_id="req-org", actor_id="bootstrap",
                                         organization_id="org-001", name="极地科考机构")
        foundation.register_actor(request_id="req-admin", actor_id="bootstrap", new_actor_id="admin-001",
                                  display_name="实验室管理员", role="admin", organization_id="org-001")
        foundation.register_actor(request_id="req-op1", actor_id="admin-001", new_actor_id="op-001",
                                  display_name="样品管理员", role="operator", organization_id="org-001")
        foundation.register_actor(request_id="req-op2", actor_id="admin-001", new_actor_id="op-002",
                                  display_name="现场队员", role="operator", organization_id="org-001")
        foundation.register_actor(request_id="req-rev", actor_id="admin-001", new_actor_id="rev-001",
                                  display_name="质量审核员", role="reviewer", organization_id="org-001")
        foundation.register_site(request_id="req-site", actor_id="admin-001", site_id="site-001",
                                 organization_id="org-001", name="沿岸实验室", timezone_name="Asia/Shanghai")
        service.register_device(request_id="req-dev", actor_id="op-001", device_id="dev-field-001",
                                site_id="site-001", label="野外钻探终端")
        service.register_reference(request_id="req-scale", actor_id="op-001", doc_id="scale-01",
                                   category="weighing_scale", site_id="site-001",
                                   content={"model": "XS205", "serial": "SN-8842"})
        service.register_reference(request_id="req-cal", actor_id="op-001", doc_id="cal-2026-09",
                                   category="calibration", site_id="site-001",
                                   content={"instrument": "scale-01", "certificate": "CAL-8842",
                                            "valid_until": "2027-03-01"})
        service.register_reference(request_id="req-seal", actor_id="op-001", doc_id="seal-batch-a",
                                   category="seal", site_id="site-001",
                                   content={"prefix": "SEAL-A", "issued_to": "op-001"})
        service.register_reference(request_id="req-person", actor_id="op-001", doc_id="person-op-002",
                                   category="personnel", site_id="site-001",
                                   content={"name": "现场队员", "qualification": "冰芯操作二级"})
        service.register_reference(request_id="req-env", actor_id="op-001", doc_id="env-log-01",
                                   category="environment", site_id="site-001",
                                   content={"logger": "freezer-corridor", "interval_s": 300})
        service.register_plan(request_id="req-plan", actor_id="op-001", plan_id="plan-001", site_id="site-001",
                              title="冰芯钻取计划 002", unit="g", max_temperature_c=-18.0,
                              description="沿岸实验室接收的钻芯批次",
                              containers=[
                                  {"container_id": "core-001", "label": "岩芯 001", "quantity": 1200,
                                   "custodian_id": "op-001", "location": "冷柜A", "seal_id": "SEAL-A-0001"},
                                  {"container_id": "core-002", "label": "岩芯 002", "quantity": 800,
                                   "custodian_id": "op-001", "location": "冷柜A"},
                              ])

        # 10-01：分装岩芯 001，随后野外终端补录 9 月末的历史事件
        clock = _clock(10, 1)
        foundation.clock = clock
        service.clock = clock
        split = service.submit_event(
            actor_id="op-001", site_id="site-001", request_id="req-split",
            event_type="split", occurred_at="2026-10-01T08:10:00Z", base_versions={"core-001": 1},
            references=[{"doc_id": "seal-batch-a", "version": 1}],
            detail={"parent_id": "core-001", "children": [
                {"container_id": "tube-001", "label": "分装管 001", "quantity": 300, "seal_id": "SEAL-A-0002"},
                {"container_id": "tube-002", "label": "分装管 002", "quantity": 300, "seal_id": "SEAL-A-0003"},
                {"container_id": "tube-003", "label": "分装管 003", "quantity": 300, "seal_id": "SEAL-A-0004"},
            ]})
        seq1 = service.submit_event(
            actor_id="op-002", site_id="site-001", device_id="dev-field-001", local_sequence=1,
            event_type="environment", occurred_at="2026-09-29T08:00:00Z", base_versions={"core-002": 1},
            references=[{"doc_id": "env-log-01", "version": 1}],
            detail={"container_id": "core-002", "temperature_c": -21.4})
        seq2 = service.submit_event(
            actor_id="op-001", site_id="site-001", device_id="dev-field-001", local_sequence=2,
            event_type="transfer", occurred_at="2026-09-30T08:00:00Z", base_versions={"core-002": 2},
            references=[{"doc_id": "person-op-002", "version": 1}],
            detail={"container_id": "core-002", "to_custodian_id": "op-002"})
        replay2 = service.submit_event(
            actor_id="op-001", site_id="site-001", device_id="dev-field-001", local_sequence=2,
            event_type="transfer", occurred_at="2026-09-30T08:00:00Z", base_versions={"core-002": 2},
            references=[{"doc_id": "person-op-002", "version": 1}],
            detail={"container_id": "core-002", "to_custodian_id": "op-002"})
        seq3 = service.submit_event(
            actor_id="op-002", site_id="site-001", device_id="dev-field-001", local_sequence=3,
            event_type="unseal", occurred_at="2026-09-30T09:00:00Z", base_versions={"tube-002": 1},
            detail={"container_id": "tube-002", "seal_id": "SEAL-A-0003"})
        seq4 = service.submit_event(
            actor_id="op-002", site_id="site-001", device_id="dev-field-001", local_sequence=4,
            event_type="consume", occurred_at="2026-09-30T10:00:00Z", base_versions={"core-002": 3},
            detail={"container_id": "core-002", "amount": 9999, "purpose": "记录错误"})
        seq5 = service.submit_event(
            actor_id="op-002", site_id="site-001", device_id="dev-field-001", local_sequence=5,
            event_type="environment", occurred_at="2026-09-29T07:00:00Z", base_versions={"core-002": 3},
            detail={"container_id": "core-002", "temperature_c": -20.1})
        seq6 = service.submit_event(
            actor_id="op-002", site_id="site-001", device_id="dev-field-001", local_sequence=6,
            event_type="weigh", occurred_at="2026-09-30T11:00:00Z", base_versions={"core-002": 3},
            references=[{"doc_id": "scale-01", "version": 1}, {"doc_id": "cal-2026-09", "version": 1}],
            detail={"container_id": "core-002", "observed_quantity": 800.0})
        conflict2 = service.submit_event(
            actor_id="op-001", site_id="site-001", device_id="dev-field-001", local_sequence=2,
            event_type="transfer", occurred_at="2026-09-30T08:05:00Z", base_versions={"core-002": 2},
            detail={"container_id": "core-002", "to_custodian_id": "op-002"})

        # 10-02：借出归还、开封称量、派生分析、再借出、消耗、超温记录与合并
        clock = _clock(10, 2)
        foundation.clock = clock
        service.clock = clock
        service.submit_event(actor_id="op-001", site_id="site-001", request_id="req-lend-1",
                             event_type="lend", occurred_at="2026-10-02T08:10:00Z",
                             base_versions={"tube-001": 1},
                             detail={"container_id": "tube-001", "borrower_id": "op-002",
                                     "due_back_at": "2026-10-03T08:00:00Z"})
        service.submit_event(actor_id="op-002", site_id="site-001", request_id="req-return-1",
                             event_type="return", occurred_at="2026-10-02T09:00:00Z",
                             base_versions={"tube-001": 2},
                             detail={"container_id": "tube-001", "receiver_id": "op-001"})
        service.submit_event(actor_id="op-001", site_id="site-001", request_id="req-unseal-1",
                             event_type="unseal", occurred_at="2026-10-02T09:10:00Z",
                             base_versions={"tube-001": 3},
                             detail={"container_id": "tube-001", "seal_id": "SEAL-A-0002"})
        service.submit_event(actor_id="op-001", site_id="site-001", request_id="req-weigh-1",
                             event_type="weigh", occurred_at="2026-10-02T09:20:00Z",
                             base_versions={"tube-001": 4},
                             references=[{"doc_id": "scale-01", "version": 1},
                                         {"doc_id": "cal-2026-09", "version": 1}],
                             detail={"container_id": "tube-001", "observed_quantity": 300.2})
        service.submit_event(actor_id="op-001", site_id="site-001", request_id="req-derive-1",
                             event_type="derive", occurred_at="2026-10-02T10:00:00Z",
                             base_versions={"tube-001": 5},
                             detail={"parent_id": "tube-001",
                                     "analysis": {"container_id": "ana-001", "result_id": "result-001",
                                                  "quantity": 50, "method": "离子色谱"}})
        service.submit_event(actor_id="op-001", site_id="site-001", request_id="req-lend-2",
                             event_type="lend", occurred_at="2026-10-02T10:30:00Z",
                             base_versions={"tube-002": 1},
                             detail={"container_id": "tube-002", "borrower_id": "op-002",
                                     "due_back_at": "2026-10-03T08:00:00Z"})
        service.submit_event(actor_id="op-002", site_id="site-001", request_id="req-consume-1",
                             event_type="consume", occurred_at="2026-10-02T11:00:00Z",
                             base_versions={"core-002": 4},
                             detail={"container_id": "core-002", "amount": 100, "purpose": "预实验"})
        service.submit_event(actor_id="op-002", site_id="site-001", request_id="req-env-1",
                             event_type="environment", occurred_at="2026-10-02T11:30:00Z",
                             base_versions={"core-002": 5},
                             references=[{"doc_id": "env-log-01", "version": 1}],
                             detail={"container_id": "core-002", "temperature_c": -15.2})
        service.submit_event(actor_id="op-001", site_id="site-001", request_id="req-merge-1",
                             event_type="merge", occurred_at="2026-10-02T13:00:00Z",
                             base_versions={"tube-001": 6, "tube-003": 1},
                             detail={"source_ids": ["tube-001", "tube-003"],
                                     "target": {"container_id": "pool-001", "label": "合并池 001",
                                                "seal_id": "SEAL-A-0005"}})

        # 10-04：研究人员溯源、管理员总览、隔离裁决与数量核对
        clock = _clock(10, 4)
        foundation.clock = clock
        service.clock = clock
        provenance = service.get_provenance("result-001")
        overview_before = service.admin_overview("site-001")
        accept_blocked = False
        service.decide_quarantine(request_id="req-dec-4", actor_id="admin-001",
                                  case_id=seq4.case_id, decision="reject", note="消耗数量明显错误")
        try:
            service.decide_quarantine(request_id="req-dec-3", actor_id="admin-001",
                                      case_id=seq3.case_id, decision="accept",
                                      note="尝试补录越权开封")
        except ConflictError:
            accept_blocked = True
        service.decide_quarantine(request_id="req-dec-3b", actor_id="admin-001",
                                  case_id=seq3.case_id, decision="reject",
                                  note="越权开封不予采纳")
        service.decide_quarantine(request_id="req-dec-5", actor_id="rev-001",
                                  case_id=seq5.case_id, decision="reject", note="设备时钟倒退")
        overview_after = service.admin_overview("site-001")
        reconciliation = service.reconcile_plan("plan-001")
        valid, event_count = foundation.verify_audit()
        case_reasons = {case["case_id"]: case["reason"]
                        for case in service.list_quarantine(site_id="site-001")}
        result = {
            "status": "ok",
            "audit_valid": valid,
            "audit_events": event_count,
            "split_confirmed": split.status == "confirmed",
            "device_replay_matched": replay2.replayed and replay2.event_id == seq2.event_id,
            "conflicting_reupload_quarantined": (
                conflict2.status == "quarantined"
                and case_reasons.get(conflict2.case_id) == "conflicting_reupload"),
            "quarantine_reasons": {
                "unauthorized_unseal": case_reasons.get(seq3.case_id),
                "contradiction": case_reasons.get(seq4.case_id),
                "clock_rollback": case_reasons.get(seq5.case_id),
            },
            "accept_blocked": accept_blocked,
            "provenance": {"containers": len(provenance["lineage"]),
                           "custody_events": len(provenance["custody_events"])},
            "lost_contact": [item["container_id"] for item in overview_before["lost_contact"]],
            "over_temperature": [item["container_id"] for item in overview_before["over_temperature"]],
            "seal_anomalies_before": len(overview_before["seal_anomalies"]),
            "pending_before": len(overview_before["pending_adjudication"]),
            "pending_after": len(overview_after["pending_adjudication"]),
            "reconciliation": reconciliation,
        }
        database.close()
        return result


def main() -> int:
    """打印验收结果并设置退出码。"""

    result = run()
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    healthy = (result["status"] == "ok" and result["audit_valid"]
               and result["reconciliation"]["balanced"] and result["device_replay_matched"]
               and result["accept_blocked"])
    return 0 if healthy else 1


if __name__ == "__main__":
    raise SystemExit(main())
