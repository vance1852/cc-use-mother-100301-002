import unittest
from datetime import datetime, timezone

from polar_station_foundation.clock import FixedClock
from polar_station_foundation.errors import (
    ConflictError,
    NotFoundError,
    PermissionDenied,
    ValidationError,
)
from polar_station_foundation.service import DomainService

from polar_station_custody import CustodyDatabase, CustodyService


class CustodyServiceTest(unittest.TestCase):
    def setUp(self):
        self.database = CustodyDatabase()
        clock = FixedClock(datetime(2026, 10, 1, 8, 0, tzinfo=timezone.utc))
        self.foundation = DomainService(self.database, clock)
        self.service = CustodyService(self.database, clock)
        self.foundation.register_organization(request_id="org", actor_id="bootstrap",
                                              organization_id="o1", name="科考机构")
        self.foundation.register_actor(request_id="admin", actor_id="bootstrap", new_actor_id="admin1",
                                       display_name="管理员", role="admin", organization_id="o1")
        self.foundation.register_actor(request_id="op1", actor_id="admin1", new_actor_id="op1",
                                       display_name="样品管理员", role="operator", organization_id="o1")
        self.foundation.register_actor(request_id="op2", actor_id="admin1", new_actor_id="op2",
                                       display_name="现场人员", role="operator", organization_id="o1")
        self.foundation.register_actor(request_id="rev1", actor_id="admin1", new_actor_id="rev1",
                                       display_name="审核员", role="reviewer", organization_id="o1")
        self.foundation.register_actor(request_id="aud1", actor_id="admin1", new_actor_id="aud1",
                                       display_name="审计员", role="auditor", organization_id="o1")
        self.foundation.register_site(request_id="site", actor_id="admin1", site_id="s1",
                                      organization_id="o1", name="沿岸实验室", timezone_name="Asia/Shanghai")
        self.service.register_device(request_id="dev", actor_id="op1", device_id="dev1",
                                     site_id="s1", label="野外终端一")
        self.service.register_reference(request_id="ref-scale", actor_id="op1", doc_id="scale1",
                                        category="weighing_scale", site_id="s1",
                                        content={"model": "XS205", "serial": "SN-1"})
        self.service.register_reference(request_id="ref-cal", actor_id="op1", doc_id="cal1",
                                        category="calibration", site_id="s1",
                                        content={"scale": "scale1", "valid_until": "2027-01-01"})
        self.service.register_plan(request_id="plan", actor_id="op1", plan_id="p1", site_id="s1",
                                   title="冰芯计划", unit="g", max_temperature_c=-18.0,
                                   containers=[
                                       {"container_id": "core1", "label": "岩芯一", "quantity": 1000,
                                        "custodian_id": "op1", "location": "冷柜A",
                                        "seal_id": "SEAL-A-0001"},
                                   ])
        self._request_counter = 0

    def tearDown(self):
        self.database.close()

    def submit(self, **kwargs):
        self._request_counter += 1
        kwargs.setdefault("actor_id", "op1")
        kwargs.setdefault("site_id", "s1")
        if "device_id" not in kwargs:
            kwargs.setdefault("request_id", f"req-{self._request_counter}")
        kwargs.setdefault("occurred_at", "2026-10-01T09:00:00Z")
        return self.service.submit_event(**kwargs)

    def split_core(self, children, base=1, **kwargs):
        return self.submit(event_type="split", base_versions={"core1": base},
                           detail={"parent_id": "core1", "children": children}, **kwargs)

    def test_plan_registration_creates_lineage_roots(self):
        view = self.service.get_container("core1")
        self.assertEqual("root", view["container"]["kind"])
        self.assertEqual(1000.0, view["container"]["quantity"])
        self.assertEqual(1, view["container"]["state_version"])
        self.assertEqual("register", view["history"][0]["event_type"])

    def test_plan_registration_replay_returns_same_receipt(self):
        first = self.service.register_plan(
            request_id="plan2", actor_id="op1", plan_id="p2", site_id="s1", title="第二计划", unit="ml",
            containers=[{"container_id": "core2", "label": "岩芯二", "quantity": 50,
                         "custodian_id": "op1", "location": "冷柜B"}])
        second = self.service.register_plan(
            request_id="plan2", actor_id="op1", plan_id="p2", site_id="s1", title="第二计划", unit="ml",
            containers=[{"container_id": "core2", "label": "岩芯二", "quantity": 50,
                         "custodian_id": "op1", "location": "冷柜B"}])
        self.assertFalse(first.replayed)
        self.assertTrue(second.replayed)

    def test_split_advances_lineage_and_requires_current_base(self):
        receipt = self.split_core(children=[
            {"container_id": "t1", "quantity": 400},
            {"container_id": "t2", "quantity": 250},
        ])
        self.assertEqual("confirmed", receipt.status)
        view = self.service.get_container("core1")
        self.assertEqual(350.0, view["container"]["quantity"])
        self.assertEqual(2, view["container"]["state_version"])
        self.assertEqual(["t1", "t2"], view["children"])
        self.assertEqual(["core1"], self.service.get_container("t1")["container"]["parents"])
        stale = self.split_core(children=[{"container_id": "t3", "quantity": 10}], base=1)
        self.assertEqual("quarantined", stale.status)
        case = self.service.list_quarantine(status="pending")[0]
        self.assertEqual("discontinuity", case["reason"])

    def test_split_overshoot_is_quarantined_without_touching_state(self):
        receipt = self.split_core(children=[{"container_id": "t1", "quantity": 1200}])
        self.assertEqual("quarantined", receipt.status)
        view = self.service.get_container("core1")
        self.assertEqual(1000.0, view["container"]["quantity"])
        self.assertEqual(1, view["container"]["state_version"])
        with self.assertRaises(NotFoundError):
            self.service.get_container("t1")

    def test_merge_combines_sources_into_pool(self):
        self.split_core(children=[{"container_id": "t1", "quantity": 400},
                                  {"container_id": "t2", "quantity": 300}])
        receipt = self.submit(event_type="merge", base_versions={"t1": 1, "t2": 1},
                              detail={"source_ids": ["t1", "t2"],
                                      "target": {"container_id": "pool1", "label": "合并池"}})
        self.assertEqual("confirmed", receipt.status)
        pool = self.service.get_container("pool1")["container"]
        self.assertEqual(700.0, pool["quantity"])
        self.assertEqual("pool", pool["kind"])
        self.assertEqual(["t1", "t2"], pool["parents"])
        self.assertEqual("merged", self.service.get_container("t1")["container"]["status"])
        self.assertEqual(0.0, self.service.get_container("t2")["container"]["quantity"])

    def test_lend_return_flow_and_overdue_detection(self):
        self.split_core(children=[{"container_id": "t1", "quantity": 100}])
        lend = self.submit(event_type="lend", base_versions={"t1": 1},
                           detail={"container_id": "t1", "borrower_id": "op2",
                                   "due_back_at": "2026-09-30T08:00:00Z"})
        self.assertEqual("confirmed", lend.status)
        container = self.service.get_container("t1")["container"]
        self.assertEqual("lent", container["status"])
        self.assertEqual("op2", container["custodian_id"])
        overview = self.service.admin_overview("s1")
        self.assertEqual(["t1"], [item["container_id"] for item in overview["lost_contact"]])
        # 借出人归还
        returned = self.submit(actor_id="op2", event_type="return", base_versions={"t1": 2},
                               detail={"container_id": "t1", "receiver_id": "op1"})
        self.assertEqual("confirmed", returned.status)
        self.assertEqual("active", self.service.get_container("t1")["container"]["status"])
        # 未借出的容器不能归还
        again = self.submit(event_type="return", base_versions={"t1": 3},
                            detail={"container_id": "t1", "receiver_id": "op1"})
        self.assertEqual("quarantined", again.status)

    def test_consume_to_terminal_blocks_further_events(self):
        self.split_core(children=[{"container_id": "t1", "quantity": 100}])
        first = self.submit(event_type="consume", base_versions={"t1": 1},
                            detail={"container_id": "t1", "amount": 100, "purpose": "实验"})
        self.assertEqual("confirmed", first.status)
        self.assertEqual("consumed", self.service.get_container("t1")["container"]["status"])
        second = self.submit(event_type="consume", base_versions={"t1": 2},
                             detail={"container_id": "t1", "amount": 1})
        self.assertEqual("quarantined", second.status)

    def test_derive_and_provenance_restores_chain_and_transfers(self):
        self.split_core(children=[{"container_id": "t1", "quantity": 500}])
        self.submit(event_type="lend", base_versions={"t1": 1},
                    detail={"container_id": "t1", "borrower_id": "op2",
                            "due_back_at": "2026-10-09T08:00:00Z"})
        self.submit(actor_id="op2", event_type="return", base_versions={"t1": 2},
                    detail={"container_id": "t1", "receiver_id": "op1"})
        self.submit(event_type="derive", base_versions={"t1": 3},
                    detail={"parent_id": "t1",
                            "analysis": {"container_id": "a1", "result_id": "r1", "quantity": 50}})
        provenance = self.service.get_provenance("r1")
        self.assertEqual("a1", provenance["analysis_container"]["container_id"])
        self.assertEqual(["core1", "t1", "a1"],
                         [item["container_id"] for item in provenance["lineage"]])
        types = [event["event_type"] for event in provenance["custody_events"]]
        self.assertEqual(["register", "split", "lend", "return", "derive"], types)
        lend_event = provenance["custody_events"][2]
        self.assertEqual("op1", lend_event["parties"]["from_custodian"]["actor_id"])
        self.assertEqual("op2", lend_event["parties"]["borrower"]["actor_id"])
        with self.assertRaises(NotFoundError):
            self.service.get_provenance("missing")

    def test_weigh_keeps_reference_snapshot_of_that_version(self):
        self.split_core(children=[{"container_id": "t1", "quantity": 500}])
        receipt = self.submit(event_type="weigh", base_versions={"t1": 1},
                              references=[{"doc_id": "scale1", "version": 1},
                                          {"doc_id": "cal1", "version": 1}],
                              detail={"container_id": "t1", "observed_quantity": 500.1})
        self.assertEqual("confirmed", receipt.status)
        # 依据文档更新到新版本后，历史事件仍保留当时版本的内容
        self.service.register_reference(request_id="ref-scale-2", actor_id="op1", doc_id="scale1",
                                        category="weighing_scale", site_id="s1",
                                        content={"model": "XS205", "serial": "SN-1", "firmware": "2.0"})
        history = self.service.get_container("t1")["history"]
        weigh_event = [event for event in history if event["event_type"] == "weigh"][0]
        snapshot = {ref["doc_id"]: ref for ref in weigh_event["references"]}
        self.assertEqual(1, snapshot["scale1"]["version"])
        self.assertNotIn("firmware", snapshot["scale1"]["content"])
        versions = self.service.list_reference_versions("scale1")
        self.assertEqual([1, 2], [item["version"] for item in versions])

    def test_weigh_beyond_tolerance_is_contradiction(self):
        self.split_core(children=[{"container_id": "t1", "quantity": 500}])
        receipt = self.submit(event_type="weigh", base_versions={"t1": 1},
                              detail={"container_id": "t1", "observed_quantity": 480})
        self.assertEqual("quarantined", receipt.status)

    def test_unseal_authorization_and_seal_match(self):
        # 非保管人开封 → 越权开封
        intruder = self.submit(actor_id="op2", event_type="unseal", base_versions={"core1": 1},
                               detail={"container_id": "core1", "seal_id": "SEAL-A-0001"})
        self.assertEqual("quarantined", intruder.status)
        cases = self.service.list_quarantine(status="pending")
        self.assertEqual("unauthorized_unseal", cases[0]["reason"])
        # 保管人但封签编号不符 → 矛盾
        mismatch = self.submit(event_type="unseal", base_versions={"core1": 1},
                               detail={"container_id": "core1", "seal_id": "SEAL-A-9999"})
        self.assertEqual("quarantined", mismatch.status)
        # 保管人且编号一致 → 确认
        opened = self.submit(event_type="unseal", base_versions={"core1": 1},
                             detail={"container_id": "core1", "seal_id": "SEAL-A-0001"})
        self.assertEqual("confirmed", opened.status)
        self.assertIsNone(self.service.get_container("core1")["container"]["seal_id"])

    def test_device_reupload_returns_original_result(self):
        first = self.submit(device_id="dev1", local_sequence=1, event_type="environment",
                            occurred_at="2026-09-30T08:00:00Z", base_versions={"core1": 1},
                            detail={"container_id": "core1", "temperature_c": -20.0})
        self.assertEqual("confirmed", first.status)
        replay = self.submit(device_id="dev1", local_sequence=1, event_type="environment",
                             occurred_at="2026-09-30T08:00:00Z", base_versions={"core1": 1},
                             detail={"container_id": "core1", "temperature_c": -20.0})
        self.assertTrue(replay.replayed)
        self.assertEqual(first.event_id, replay.event_id)
        conflict = self.submit(device_id="dev1", local_sequence=1, event_type="environment",
                               occurred_at="2026-09-30T08:00:00Z", base_versions={"core1": 1},
                               detail={"container_id": "core1", "temperature_c": -19.0})
        self.assertEqual("quarantined", conflict.status)
        self.assertEqual(first.event_id, conflict.event_id)
        cases = self.service.list_quarantine(status="pending")
        self.assertEqual("conflicting_reupload", cases[0]["reason"])

    def test_device_clock_rollback_and_out_of_order_upload(self):
        self.submit(device_id="dev1", local_sequence=5, event_type="environment",
                    occurred_at="2026-09-30T10:00:00Z", base_versions={"core1": 1},
                    detail={"container_id": "core1", "temperature_c": -21.0})
        # 乱序补传：序列更小、时间更早，符合设备时钟单调性，应予确认
        earlier = self.submit(device_id="dev1", local_sequence=3, event_type="environment",
                              occurred_at="2026-09-30T09:00:00Z", base_versions={"core1": 2},
                              detail={"container_id": "core1", "temperature_c": -22.0})
        self.assertEqual("confirmed", earlier.status)
        # 序列更大但时间早于已确认的低序列事件 → 时钟倒退
        rollback = self.submit(device_id="dev1", local_sequence=6, event_type="environment",
                               occurred_at="2026-09-30T08:30:00Z", base_versions={"core1": 3},
                               detail={"container_id": "core1", "temperature_c": -20.0})
        self.assertEqual("quarantined", rollback.status)
        cases = self.service.list_quarantine(status="pending")
        self.assertEqual("clock_rollback", cases[0]["reason"])

    def test_future_dated_device_event_is_quarantined(self):
        receipt = self.submit(device_id="dev1", local_sequence=1, event_type="environment",
                              occurred_at="2026-10-01T09:00:00Z", base_versions={"core1": 1},
                              detail={"container_id": "core1", "temperature_c": -20.0})
        self.assertEqual("quarantined", receipt.status)
        cases = self.service.list_quarantine(status="pending")
        self.assertEqual("clock_anomaly", cases[0]["reason"])

    def test_adjudication_accept_appends_when_event_becomes_applicable(self):
        # 设备 B 先上传了针对子容器 t1 的事件，但产生 t1 的分装尚未同步 → 无法接续
        pending = self.submit(device_id="dev1", local_sequence=1, event_type="weigh",
                              occurred_at="2026-09-30T08:00:00Z", base_versions={"t1": 1},
                              detail={"container_id": "t1", "observed_quantity": 400})
        self.assertEqual("quarantined", pending.status)
        self.split_core(children=[{"container_id": "t1", "quantity": 400}])
        # 分装同步后事件重新校验通过，accept 将其追加进历史
        decided = self.service.decide_quarantine(request_id="dec-1", actor_id="rev1",
                                                 case_id=pending.case_id, decision="accept",
                                                 note="分装已同步，可以接续")
        self.assertFalse(decided.replayed)
        view = self.service.get_container("t1")
        self.assertEqual(2, view["container"]["state_version"])
        self.assertEqual("weigh", view["history"][-1]["event_type"])
        # 重复上传该设备序列现在返回已确认的结果
        replay = self.submit(device_id="dev1", local_sequence=1, event_type="weigh",
                             occurred_at="2026-09-30T08:00:00Z", base_versions={"t1": 1},
                             detail={"container_id": "t1", "observed_quantity": 400})
        self.assertTrue(replay.replayed)
        self.assertEqual("confirmed", replay.status)

    def test_adjudication_accept_fails_when_history_would_be_rewritten(self):
        self.split_core(children=[{"container_id": "t1", "quantity": 400}])
        stale = self.submit(event_type="consume", base_versions={"t1": 99},
                            detail={"container_id": "t1", "amount": 10})
        self.assertEqual("quarantined", stale.status)
        with self.assertRaises(ConflictError):
            self.service.decide_quarantine(request_id="dec-x", actor_id="admin1",
                                           case_id=stale.case_id, decision="accept")
        # 案件仍待裁决，驳回后终态
        self.service.decide_quarantine(request_id="dec-y", actor_id="admin1",
                                       case_id=stale.case_id, decision="reject", note="版本无法接续")
        cases = {case["case_id"]: case["status"] for case in self.service.list_quarantine()}
        self.assertEqual("rejected", cases[stale.case_id])
        with self.assertRaises(ConflictError):
            self.service.decide_quarantine(request_id="dec-z", actor_id="admin1",
                                           case_id=stale.case_id, decision="reject")

    def test_confirmed_history_is_never_modified_by_quarantine(self):
        self.split_core(children=[{"container_id": "t1", "quantity": 400}])
        before = self.service.get_container("t1")["container"]
        self.submit(actor_id="op2", event_type="unseal", base_versions={"t1": 1},
                    detail={"container_id": "t1", "seal_id": "X"})
        self.submit(event_type="consume", base_versions={"t1": 1},
                    detail={"container_id": "t1", "amount": 99999})
        after = self.service.get_container("t1")["container"]
        self.assertEqual(before, after)
        valid, _ = self.foundation.verify_audit()
        self.assertTrue(valid)

    def test_admin_overview_flags_temperature_seal_and_pending(self):
        self.split_core(children=[{"container_id": "t1", "quantity": 400}])
        self.submit(event_type="environment", base_versions={"t1": 1},
                    detail={"container_id": "t1", "temperature_c": -12.5})
        self.submit(actor_id="op2", event_type="unseal", base_versions={"t1": 2},
                    detail={"container_id": "t1", "seal_id": "S1"})
        overview = self.service.admin_overview("s1")
        self.assertEqual(["t1"], [item["container_id"] for item in overview["over_temperature"]])
        self.assertEqual(-12.5, overview["over_temperature"][0]["peak_temperature_c"])
        self.assertEqual(["t1"], [item["container_id"] for item in overview["seal_anomalies"]])
        self.assertEqual(1, len(overview["pending_adjudication"]))
        self.assertEqual([], overview["quantity_imbalances"])

    def test_reconcile_balanced_and_detects_tampering(self):
        self.split_core(children=[{"container_id": "t1", "quantity": 400}])
        self.submit(event_type="consume", base_versions={"t1": 1},
                    detail={"container_id": "t1", "amount": 50})
        summary = self.service.reconcile_plan("p1")
        self.assertTrue(summary["balanced"])
        self.assertEqual(1000.0, summary["registered_total"])
        self.assertEqual(950.0, summary["current_total"])
        self.assertEqual(50.0, summary["consumed_total"])
        # 直接篡改台账数量（模拟凭空增加），核对必须发现
        self.database.connection.execute(
            "UPDATE containers SET quantity=quantity+1 WHERE container_id='t1'")
        tampered = self.service.reconcile_plan("p1")
        self.assertFalse(tampered["balanced"])
        self.assertTrue(any(v["issue"] == "台账数量与事件链不符" for v in tampered["violations"]))

    def test_permissions(self):
        with self.assertRaises(PermissionDenied):
            self.submit(actor_id="aud1", event_type="weigh", base_versions={"core1": 1},
                        detail={"container_id": "core1", "observed_quantity": 1000})
        with self.assertRaises(PermissionDenied):
            self.submit(actor_id="rev1", event_type="weigh", base_versions={"core1": 1},
                        detail={"container_id": "core1", "observed_quantity": 1000})
        with self.assertRaises(PermissionDenied):
            self.service.register_device(request_id="dev-x", actor_id="aud1", device_id="dev2",
                                         site_id="s1", label="非法设备")
        # 裁决允许 admin 与 reviewer
        pending = self.submit(event_type="consume", base_versions={"core1": 1},
                              detail={"container_id": "core1", "amount": 99999})
        with self.assertRaises(PermissionDenied):
            self.service.decide_quarantine(request_id="dec-p", actor_id="op1",
                                           case_id=pending.case_id, decision="reject")
        decided = self.service.decide_quarantine(request_id="dec-p2", actor_id="rev1",
                                                 case_id=pending.case_id, decision="reject")
        self.assertFalse(decided.replayed)

    def test_online_event_request_id_conflict(self):
        self.submit(request_id="fixed", event_type="weigh", base_versions={"core1": 1},
                    detail={"container_id": "core1", "observed_quantity": 1000})
        with self.assertRaises(ConflictError):
            self.submit(request_id="fixed", event_type="weigh", base_versions={"core1": 1},
                        detail={"container_id": "core1", "observed_quantity": 999})

    def test_unknown_container_event_is_discontinuity_not_error(self):
        receipt = self.submit(event_type="weigh", base_versions={"ghost": 1},
                              detail={"container_id": "ghost", "observed_quantity": 1})
        self.assertEqual("quarantined", receipt.status)
        cases = self.service.list_quarantine(status="pending")
        self.assertEqual("discontinuity", cases[0]["reason"])

    def test_missing_reference_version_is_contradiction(self):
        receipt = self.submit(event_type="weigh", base_versions={"core1": 1},
                              references=[{"doc_id": "scale1", "version": 9}],
                              detail={"container_id": "core1", "observed_quantity": 1000})
        self.assertEqual("quarantined", receipt.status)
        cases = self.service.list_quarantine(status="pending")
        self.assertEqual("contradiction", cases[0]["reason"])

    def test_validation_errors(self):
        with self.assertRaises(ValidationError):
            self.submit(event_type="fly", base_versions={}, detail={})
        with self.assertRaises(ValidationError):
            self.submit(event_type="weigh", base_versions={"core1": 0},
                        detail={"container_id": "core1", "observed_quantity": 1})
        with self.assertRaises(ValidationError):
            self.submit(device_id="dev1", local_sequence=1, request_id="x",
                        event_type="weigh", base_versions={"core1": 1},
                        detail={"container_id": "core1", "observed_quantity": 1})
        with self.assertRaises(NotFoundError):
            self.submit(device_id="ghost", local_sequence=1, event_type="weigh",
                        occurred_at="2026-09-30T08:00:00Z", base_versions={"core1": 1},
                        detail={"container_id": "core1", "observed_quantity": 1})


if __name__ == "__main__":
    unittest.main()
