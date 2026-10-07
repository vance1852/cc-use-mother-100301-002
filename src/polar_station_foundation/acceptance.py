"""运行基础服务与样品监管链的离线端到端验收。"""

from __future__ import annotations

import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from .clock import FixedClock
from .custody import CustodyService
from .service import DomainService
from .storage import Database

CALIBRATION = {"instrument_id": "bal-1", "calibration_id": "cal-2026-09",
               "calibrated_at": "2026-09-01T00:00:00Z"}
INSTRUMENT = {"instrument_id": "irms-7", "calibration_id": "cal-2026-09",
              "calibrated_at": "2026-09-01T00:00:00Z"}


def _event(device, sequence, at, event_type, payload, *, actor="operator-001", expected=None):
    envelope = {"device_id": device, "local_sequence": sequence, "event_type": event_type,
                "actor_id": actor, "occurred_at": at, "payload": payload}
    if expected:
        envelope["expected_version"] = expected
    return envelope


def run() -> dict[str, object]:
    """执行登记、谱系、离线回放、隔离裁决与溯源的完整链路。"""

    with tempfile.TemporaryDirectory() as directory:
        database = Database(Path(directory) / "acceptance.sqlite3")
        clock = FixedClock(datetime(2026, 10, 7, 8, 0, tzinfo=timezone.utc))
        service = DomainService(database, clock)
        custody = CustodyService(database, clock)

        service.register_organization(request_id="req-org", actor_id="bootstrap",
                                      organization_id="org-001", name="示范科考机构")
        service.register_actor(request_id="req-admin", actor_id="bootstrap",
                               new_actor_id="admin-001", display_name="系统管理员",
                               role="admin", organization_id="org-001")
        service.register_actor(request_id="req-operator", actor_id="admin-001",
                               new_actor_id="operator-001", display_name="站务负责人",
                               role="operator", organization_id="org-001")
        service.register_actor(request_id="req-researcher", actor_id="admin-001",
                               new_actor_id="researcher-001", display_name="研究员",
                               role="reviewer", organization_id="org-001")
        service.register_site(request_id="req-site", actor_id="operator-001",
                              site_id="site-001", organization_id="org-001",
                              name="沿岸实验室", timezone_name="Asia/Shanghai")

        custody.create_sampling_plan(
            request_id="req-plan", actor_id="operator-001", site_id="site-001",
            plan_id="plan-ice-001",
            cores=[{"container_id": "core-a", "kind": "sea_ice_core", "interval": "0-30cm",
                    "mass_grams": 1000.0, "expected_seal_id": "seal-a"},
                   {"container_id": "core-b", "kind": "sea_ice_core", "interval": "30-60cm",
                    "mass_grams": 800.0, "expected_seal_id": "seal-b-registered"}])

        # 设备 dev-field 离线采集，恢复连接后按本地序列批量提交。
        events = [
            _event("dev-field", 1, "2026-10-01T08:00:00Z", "core_generated",
                   {"plan_id": "plan-ice-001", "container_id": "core-a", "mass_grams": 1000.0,
                    "seal": {"seal_id": "seal-a"}, "location": "现场冷库"}),
            # core-b 的封签曾被离线补录，与采样计划预登编号不一致 -> 隔离。
            _event("dev-field", 2, "2026-10-01T08:05:00Z", "core_generated",
                   {"plan_id": "plan-ice-001", "container_id": "core-b", "mass_grams": 800.0,
                    "seal": {"seal_id": "seal-b-field"}, "location": "现场冷库"}),
            _event("dev-field", 3, "2026-10-01T09:00:00Z", "aliquot",
                   {"parent_id": "core-a", "container_id": "tube-a1", "kind": "tube",
                    "mass_grams": 300.0, "opened_seal_id": "seal-a",
                    "parent_mass_after": 700.0, "seal": {"seal_id": "seal-a1"},
                    "reseal": {"seal_id": "seal-a-r1"}}),
            # 时钟倒退 -> 隔离。
            _event("dev-field", 4, "2026-10-01T08:30:00Z", "weigh",
                   {"container_id": "core-a", "mass_grams": 699.0, "calibration": CALIBRATION}),
        ]
        outcomes = [custody.submit_event(event) for event in events]
        assert outcomes[0].accepted
        seal_case = next(c for c in custody.list_quarantine("pending")
                         if c.reason_code == "seal_mismatch")
        clock_case = next(c for c in custody.list_quarantine("pending")
                          if c.reason_code == "clock_regression")

        # 管理员核对现场记录后接受补录封签、驳回倒退时钟；二者都只追加审计历史。
        custody.adjudicate_case(request_id="req-accept-seal", actor_id="admin-001",
                                case_id=seal_case.case_id, decision="accept",
                                note="现场补录封签，来源确认")
        custody.adjudicate_case(request_id="req-reject-clock", actor_id="admin-001",
                                case_id=clock_case.case_id, decision="reject",
                                note="设备时钟错误，事件作废")

        # 重复上传原始 seq=2：只能回放接受后的原结果。
        replay = custody.submit_event(events[1])
        assert replay.replayed and replay.accepted

        # 设备游标越过已裁决的 seq 2/4 后继续：分装、借出、归还、分析。
        accepted = custody.submit_event(_event(
            "dev-field", 5, "2026-10-01T10:00:00Z", "aliquot",
            {"parent_id": "core-b", "container_id": "tube-b1", "kind": "tube",
             "mass_grams": 200.0, "opened_seal_id": "seal-b-field",
             "parent_mass_after": 600.0, "seal": {"seal_id": "seal-b1"}}))
        assert accepted.accepted

        assert custody.submit_event({
            "event_type": "loan", "actor_id": "operator-001",
            "occurred_at": "2026-10-02T08:00:00Z",
            "payload": {"borrower_actor_id": "researcher-001",
                        "due_at": "2026-10-05T08:00:00Z",
                        "items": [{"container_id": "tube-a1"}]}}).accepted
        # 归还时封签与借出时不符：事件成立但封签异常被永久标记。
        assert custody.submit_event({
            "event_type": "return", "actor_id": "researcher-001",
            "occurred_at": "2026-10-06T08:00:00Z",
            "payload": {"items": [{"container_id": "tube-a1", "seal_id": "seal-unknown",
                                   "seal_intact": False, "mass_grams": 300.0,
                                   "to_location": "沿岸实验室"}]}}).accepted
        assert custody.submit_event({
            "event_type": "open", "actor_id": "researcher-001",
            "occurred_at": "2026-10-06T09:00:00Z",
            "payload": {"container_id": "tube-a1", "opened_seal_id": "seal-a1"}}).accepted
        assert custody.submit_event({
            "event_type": "analysis", "actor_id": "researcher-001",
            "occurred_at": "2026-10-06T10:00:00Z",
            "payload": {"analysis_id": "analysis-001", "container_id": "tube-a1",
                        "method": "isotope_ratio", "amount_grams": 10.0,
                        "result": {"d18o": -30.21, "unit": "per_mil"},
                        "instrument": INSTRUMENT}}).accepted

        provenance = custody.analysis_provenance("analysis-001")
        assert [c["container_id"] for c in provenance["ancestors"]] == ["core-a"]
        timeline_types = {e["event_type"] for e in provenance["custody_timeline"]}
        assert {"core_generated", "aliquot", "loan", "return"} <= timeline_types

        alerts = custody.list_alerts(offline_before="2026-10-07T07:00:00Z")
        assert any(c["container_id"] == "tube-a1"
                   for c in alerts["seal_anomalies"])
        assert not alerts["pending_quarantine"]

        report = custody.conservation_report("plan-ice-001")
        assert all(row["issue"] is None for row in report)
        valid, event_count = custody.verify_audit()
        database.close()
        return {"status": "ok", "containers": 5, "analysis_ancestors": 1,
                "custody_timeline_events": len(provenance["custody_timeline"]),
                "seal_anomalies": 1, "conservation_roots": len(report),
                "audit_events": event_count, "audit_valid": valid,
                "replay_replayed": replay.replayed}


def main() -> int:
    """打印验收结果并设置退出码。"""

    result = run()
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result["status"] == "ok" and result["audit_valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
