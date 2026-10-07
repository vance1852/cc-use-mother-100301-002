import unittest
from datetime import datetime, timezone

from polar_station_foundation.clock import FixedClock
from polar_station_foundation.custody import CustodyService
from polar_station_foundation.errors import ConflictError, NotFoundError, PermissionDenied, ValidationError
from polar_station_foundation.service import DomainService
from polar_station_foundation.storage import Database

CAL = {"instrument_id": "bal-1", "calibration_id": "cal-1",
       "calibrated_at": "2026-09-01T00:00:00Z"}
INST = {"instrument_id": "irms-7", "calibration_id": "cal-1",
        "calibrated_at": "2026-09-01T00:00:00Z"}


class CustodyTestBase(unittest.TestCase):
    def setUp(self):
        self.database = Database()
        self.clock = FixedClock(datetime(2026, 10, 7, 8, tzinfo=timezone.utc))
        self.service = DomainService(self.database, self.clock)
        self.custody = CustodyService(self.database, self.clock)
        self.service.register_organization(request_id="org", actor_id="bootstrap",
                                           organization_id="o1", name="极地机构")
        self.service.register_actor(request_id="admin", actor_id="bootstrap", new_actor_id="a1",
                                    display_name="管理员", role="admin", organization_id="o1")
        self.service.register_actor(request_id="op", actor_id="a1", new_actor_id="op1",
                                    display_name="操作员", role="operator", organization_id="o1")
        self.service.register_actor(request_id="rv", actor_id="a1", new_actor_id="rv1",
                                    display_name="研究员", role="reviewer", organization_id="o1")
        self.service.register_actor(request_id="au", actor_id="a1", new_actor_id="au1",
                                    display_name="审计员", role="auditor", organization_id="o1")
        self.service.register_site(request_id="site", actor_id="op1", site_id="s1",
                                   organization_id="o1", name="沿岸实验室",
                                   timezone_name="Asia/Shanghai")
        self.custody.create_sampling_plan(
            request_id="plan", actor_id="op1", site_id="s1", plan_id="p1",
            cores=[{"container_id": "core-1", "kind": "sea_ice_core", "mass_grams": 1000.0,
                    "expected_seal_id": "seal-1"},
                   {"container_id": "core-2", "kind": "sea_ice_core", "mass_grams": 800.0,
                    "expected_seal_id": "seal-2"}])

    def tearDown(self):
        self.database.close()

    def submit(self, etype, payload, *, device=None, sequence=None, at="2026-10-01T08:00:00Z",
               actor="op1", expected=None):
        envelope = {"event_type": etype, "actor_id": actor, "occurred_at": at, "payload": payload}
        if device is not None:
            envelope["device_id"] = device
            envelope["local_sequence"] = sequence
        if expected:
            envelope["expected_version"] = expected
        return self.custody.submit_event(envelope)


class PlanAndLineageTest(CustodyTestBase):
    def test_plan_generates_declared_containers_with_genealogy(self):
        result = self.submit("core_generated",
                             {"plan_id": "p1", "container_id": "core-1", "mass_grams": 1000.0,
                              "seal": {"seal_id": "seal-1"}}, device="d1", sequence=1)
        self.assertTrue(result.accepted)
        container = self.custody.get_container("core-1")
        self.assertEqual("p1", container.plan_id)
        self.assertEqual("sealed", container.status)
        self.assertEqual(1, container.version)
        self.assertEqual("seal-1", container.seal_id)

    def test_undeclared_container_cannot_be_generated(self):
        result = self.submit("core_generated",
                             {"plan_id": "p1", "container_id": "ghost", "mass_grams": 1.0,
                              "seal": {"seal_id": "s"}}, device="d1", sequence=1)
        self.assertFalse(result.accepted)
        self.assertEqual("parent_not_generated", result.reason_code)

    def test_offline_resealed_seal_is_quarantined(self):
        result = self.submit("core_generated",
                             {"plan_id": "p1", "container_id": "core-2", "mass_grams": 800.0,
                              "seal": {"seal_id": "seal-field"}}, device="d1", sequence=1)
        self.assertEqual("seal_mismatch", result.reason_code)
        case = self.custody.get_case(result.case_id)
        self.assertEqual("pending", case.status)

    def test_auditor_cannot_create_plan(self):
        with self.assertRaises(PermissionDenied):
            self.custody.create_sampling_plan(request_id="x", actor_id="au1", site_id="s1",
                                              plan_id="p2", cores=[{"container_id": "c",
                                                                   "kind": "k"}])


class OfflineReplayTest(CustodyTestBase):
    def _core(self):
        return self.submit("core_generated",
                           {"plan_id": "p1", "container_id": "core-1", "mass_grams": 1000.0,
                            "seal": {"seal_id": "seal-1"}}, device="d1", sequence=1)

    def test_duplicate_upload_returns_original_result(self):
        first = self._core()
        replay = self._core()
        self.assertTrue(first.accepted and not first.replayed)
        self.assertTrue(replay.replayed and replay.accepted)
        self.assertEqual(first.event_id, replay.event_id)
        events = self.custody.list_events()
        self.assertEqual(1, len(events))

    def test_same_sequence_different_content_quarantines_and_keeps_original(self):
        self._core()
        evil = self.submit("core_generated",
                           {"plan_id": "p1", "container_id": "core-1", "mass_grams": 999.0,
                            "seal": {"seal_id": "seal-1"}}, device="d1", sequence=1)
        self.assertEqual("duplicate_mismatch", evil.reason_code)
        self.assertIsNone(evil.event_id)
        self.assertEqual(1000.0, self.custody.get_container("core-1").mass_grams)

    def test_quarantined_sequence_replays_case_until_decided(self):
        bad = self.submit("core_generated",
                          {"plan_id": "p1", "container_id": "core-2", "mass_grams": 800.0,
                           "seal": {"seal_id": "seal-field"}}, device="d1", sequence=1)
        replay = self.submit("core_generated",
                             {"plan_id": "p1", "container_id": "core-2", "mass_grams": 800.0,
                              "seal": {"seal_id": "seal-field"}}, device="d1", sequence=1)
        self.assertTrue(replay.replayed)
        self.assertEqual(bad.case_id, replay.case_id)

    def test_sequence_gap_is_quarantined_but_does_not_block_later_sync(self):
        self._core()
        gap = self.submit("weigh", {"container_id": "core-1", "mass_grams": 999.0,
                                    "calibration": CAL},
                          device="d1", sequence=3, at="2026-10-01T09:00:00Z")
        self.assertEqual("sequence_gap", gap.reason_code)
        # seq=2 仍然可以补传，不会因为 seq=3 已占位而被拒绝。
        ok = self.submit("weigh", {"container_id": "core-1", "mass_grams": 1000.0,
                                   "calibration": CAL},
                         device="d1", sequence=2, at="2026-10-01T08:30:00Z")
        self.assertTrue(ok.accepted, ok)

    def test_clock_regression_is_quarantined(self):
        self._core()
        result = self.submit("weigh", {"container_id": "core-1", "mass_grams": 999.0,
                                       "calibration": CAL},
                             device="d1", sequence=2, at="2026-09-30T00:00:00Z")
        self.assertEqual("clock_regression", result.reason_code)


class EventRulesTest(CustodyTestBase):
    def setUp(self):
        super().setUp()
        self.submit("core_generated",
                    {"plan_id": "p1", "container_id": "core-1", "mass_grams": 1000.0,
                     "seal": {"seal_id": "seal-1"}}, device="d1", sequence=1)

    def test_aliquot_respects_parent_child_chain_and_mass(self):
        ok = self.submit("aliquot",
                         {"parent_id": "core-1", "container_id": "tube-1", "kind": "tube",
                          "mass_grams": 300.0, "opened_seal_id": "seal-1",
                          "parent_mass_after": 700.0, "seal": {"seal_id": "seal-t1"},
                          "reseal": {"seal_id": "seal-1b"}}, at="2026-10-01T09:00:00Z")
        self.assertTrue(ok.accepted)
        child = self.custody.get_container("tube-1")
        self.assertEqual(("core-1",), child.parents)
        self.assertEqual(700.0, self.custody.get_container("core-1").mass_grams)

    def test_over_aliquot_quarantines_and_does_not_mutate(self):
        result = self.submit("aliquot",
                             {"parent_id": "core-1", "container_id": "tube-x", "kind": "tube",
                              "mass_grams": 1200.0, "opened_seal_id": "seal-1",
                              "parent_mass_after": 0.0}, at="2026-10-01T09:00:00Z")
        self.assertEqual("mass_inconsistency", result.reason_code)
        with self.assertRaises(NotFoundError):
            self.custody.get_container("tube-x")
        self.assertEqual(1000.0, self.custody.get_container("core-1").mass_grams)

    def test_unauthorized_open_is_quarantined(self):
        result = self.submit("open", {"container_id": "core-1", "opened_seal_id": "seal-1"},
                             actor="au1", at="2026-10-01T09:00:00Z")
        self.assertEqual("unauthorized_open", result.reason_code)
        self.assertEqual("sealed", self.custody.get_container("core-1").status)

    def test_wrong_seal_on_open_is_quarantined(self):
        result = self.submit("open", {"container_id": "core-1", "opened_seal_id": "seal-other"},
                             at="2026-10-01T09:00:00Z")
        self.assertEqual("seal_mismatch", result.reason_code)

    def test_unknown_container_is_unlinkable(self):
        result = self.submit("weigh", {"container_id": "nope", "mass_grams": 1.0,
                                       "calibration": CAL}, at="2026-10-01T09:00:00Z")
        self.assertEqual("unknown_container", result.reason_code)

    def test_optimistic_version_conflict_is_quarantined(self):
        result = self.submit("weigh", {"container_id": "core-1", "mass_grams": 998.0,
                                       "calibration": CAL},
                             at="2026-10-01T09:00:00Z", expected={"core-1": 5})
        self.assertEqual("state_conflict", result.reason_code)

    def test_consume_beyond_mass_is_quarantined(self):
        result = self.submit("consume", {"container_id": "core-1", "amount_grams": 2000.0,
                                         "opened_seal_id": "seal-1"},
                             at="2026-10-01T09:00:00Z")
        self.assertEqual("mass_inconsistency", result.reason_code)

    def test_terminal_container_rejects_further_events(self):
        self.submit("consume", {"container_id": "core-1", "amount_grams": 1000.0,
                                "opened_seal_id": "seal-1"}, at="2026-10-01T09:00:00Z")
        result = self.submit("weigh", {"container_id": "core-1", "mass_grams": 1.0,
                                       "calibration": CAL}, at="2026-10-01T10:00:00Z")
        self.assertEqual("terminal_container", result.reason_code)

    def test_loan_requires_seal_and_return_flags_seal_anomaly(self):
        not_sealed = self.submit("loan", {"borrower_actor_id": "rv1",
                                          "items": [{"container_id": "core-1"}]},
                                 at="2026-10-01T09:00:00Z")
        # core-1 已加封，因此借出成立；这里先验证封签不符的归还被标记。
        self.assertTrue(not_sealed.accepted)
        ret = self.submit("return",
                          {"items": [{"container_id": "core-1", "seal_id": "tampered",
                                      "seal_intact": False, "mass_grams": 1000.0}]},
                          actor="rv1", at="2026-10-02T09:00:00Z")
        self.assertTrue(ret.accepted)
        self.assertTrue(self.custody.get_container("core-1").seal_anomaly)

    def test_weigh_requires_calibration_basis(self):
        result = self.submit("weigh", {"container_id": "core-1", "mass_grams": 999.0},
                             at="2026-10-01T09:00:00Z")
        self.assertEqual("validation_error", result.reason_code)

    def test_temperature_excursion_is_flagged(self):
        self.submit("environment", {"container_id": "core-1", "temp_c": -2.0,
                                    "temp_min": -25.0, "temp_max": -10.0},
                    at="2026-10-01T09:00:00Z")
        self.assertTrue(self.custody.get_container("core-1").temp_excursion)


class AdjudicationTest(CustodyTestBase):
    def test_admin_accept_appends_history_and_allows_replay(self):
        bad = self.submit("core_generated",
                          {"plan_id": "p1", "container_id": "core-2", "mass_grams": 800.0,
                           "seal": {"seal_id": "seal-field"}}, device="d1", sequence=1)
        self.custody.adjudicate_case(request_id="q1", actor_id="a1", case_id=bad.case_id,
                                     decision="accept", note="确认补录")
        self.assertEqual("sealed", self.custody.get_container("core-2").status)
        replay = self.submit("core_generated",
                             {"plan_id": "p1", "container_id": "core-2", "mass_grams": 800.0,
                              "seal": {"seal_id": "seal-field"}}, device="d1", sequence=1)
        self.assertTrue(replay.replayed and replay.accepted)

    def test_non_admin_cannot_adjudicate(self):
        bad = self.submit("core_generated",
                          {"plan_id": "p1", "container_id": "core-2", "mass_grams": 800.0,
                           "seal": {"seal_id": "seal-field"}}, device="d1", sequence=1)
        with self.assertRaises(PermissionDenied):
            self.custody.adjudicate_case(request_id="q1", actor_id="op1",
                                         case_id=bad.case_id, decision="accept")

    def test_case_cannot_be_decided_twice(self):
        bad = self.submit("core_generated",
                          {"plan_id": "p1", "container_id": "core-2", "mass_grams": 800.0,
                           "seal": {"seal_id": "seal-field"}}, device="d1", sequence=1)
        self.custody.adjudicate_case(request_id="q1", actor_id="a1", case_id=bad.case_id,
                                     decision="reject")
        with self.assertRaises(ConflictError):
            self.custody.adjudicate_case(request_id="q2", actor_id="a1", case_id=bad.case_id,
                                         decision="accept")

    def test_accept_still_unlinkable_event_is_refused(self):
        bad = self.submit("weigh", {"container_id": "ghost", "mass_grams": 1.0,
                                    "calibration": CAL}, device="d1", sequence=1)
        with self.assertRaises(ConflictError):
            self.custody.adjudicate_case(request_id="q1", actor_id="a1", case_id=bad.case_id,
                                         decision="accept")

    def test_admin_can_accept_event_with_stale_optimistic_version(self):
        self.submit("core_generated",
                    {"plan_id": "p1", "container_id": "core-1", "mass_grams": 1000.0,
                     "seal": {"seal_id": "seal-1"}}, device="d1", sequence=1)
        self.submit("weigh", {"container_id": "core-1", "mass_grams": 999.0,
                              "calibration": CAL}, device="d1", sequence=2,
                    at="2026-10-01T09:00:00Z")
        # 第三条事件携带过期版本号（容器当前已是 v2）-> 隔离，管理员核对后接受。
        stale = self.submit("weigh", {"container_id": "core-1", "mass_grams": 998.0,
                                      "calibration": CAL}, device="d1", sequence=3,
                            at="2026-10-01T09:30:00Z", expected={"core-1": 1})
        self.assertEqual("state_conflict", stale.reason_code)
        self.custody.adjudicate_case(request_id="q1", actor_id="a1", case_id=stale.case_id,
                                     decision="accept", note="离线并发，以现场读数为准")
        self.assertEqual(998.0, self.custody.get_container("core-1").mass_grams)


class ProvenanceAndReportTest(CustodyTestBase):
    def _build_chain(self):
        self.submit("core_generated",
                    {"plan_id": "p1", "container_id": "core-1", "mass_grams": 1000.0,
                     "seal": {"seal_id": "seal-1"}}, device="d1", sequence=1)
        self.submit("aliquot",
                    {"parent_id": "core-1", "container_id": "tube-1", "kind": "tube",
                     "mass_grams": 300.0, "opened_seal_id": "seal-1",
                     "parent_mass_after": 700.0, "seal": {"seal_id": "seal-t1"},
                     "reseal": {"seal_id": "seal-1b"}}, at="2026-10-01T09:00:00Z")
        self.submit("loan", {"borrower_actor_id": "rv1",
                             "due_at": "2026-10-05T08:00:00Z",
                             "items": [{"container_id": "tube-1"}]},
                    at="2026-10-02T08:00:00Z")
        self.submit("return",
                    {"items": [{"container_id": "tube-1", "seal_id": "seal-t1",
                                "seal_intact": True, "mass_grams": 300.0}]},
                    actor="rv1", at="2026-10-03T08:00:00Z")
        self.submit("open", {"container_id": "tube-1", "opened_seal_id": "seal-t1"},
                    actor="rv1", at="2026-10-03T09:00:00Z")
        self.submit("analysis",
                    {"analysis_id": "an-1", "container_id": "tube-1", "method": "isotope",
                     "amount_grams": 10.0, "result": {"d18o": -30.2},
                     "instrument": INST}, actor="rv1", at="2026-10-03T10:00:00Z")

    def test_analysis_provenance_restores_lineage_and_transfers(self):
        self._build_chain()
        provenance = self.custody.analysis_provenance("an-1")
        self.assertEqual("tube-1", provenance["container"]["container_id"])
        self.assertEqual(["core-1"], [c["container_id"] for c in provenance["ancestors"]])
        types_ = [e["event_type"] for e in provenance["custody_timeline"]]
        self.assertIn("loan", types_)
        self.assertIn("return", types_)
        self.assertIn("aliquot", types_)

    def test_conservation_report_balances(self):
        self._build_chain()
        report = self.custody.conservation_report("p1")
        root = next(r for r in report if r["root_container_id"] == "core-1")
        self.assertIsNone(root["issue"])
        # 1000 = 现存(700 + 290) + 分析消耗 10
        self.assertAlmostEqual(990.0, root["live_mass_grams"], places=3)
        self.assertAlmostEqual(10.0, root["consumed_or_analyzed_grams"], places=3)

    def test_conservation_detects_mass_missing_after_unexplained_weigh(self):
        self.submit("core_generated",
                    {"plan_id": "p1", "container_id": "core-1", "mass_grams": 1000.0,
                     "seal": {"seal_id": "seal-1"}}, device="d1", sequence=1)
        self.submit("weigh", {"container_id": "core-1", "mass_grams": 900.0,
                              "calibration": CAL}, at="2026-10-01T09:00:00Z")
        root = next(r for r in self.custody.conservation_report("p1")
                    if r["root_container_id"] == "core-1")
        self.assertEqual("mass_missing", root["issue"])

    def test_merge_conservation_balances_across_parents(self):
        self.submit("core_generated",
                    {"plan_id": "p1", "container_id": "core-1", "mass_grams": 1000.0,
                     "seal": {"seal_id": "seal-1"}}, device="d1", sequence=1, at="2026-10-01T08:00:00Z")
        self.submit("core_generated",
                    {"plan_id": "p1", "container_id": "core-2", "mass_grams": 800.0,
                     "seal": {"seal_id": "seal-2"}}, device="d1", sequence=2, at="2026-10-01T08:05:00Z")
        self.submit("aliquot",
                    {"parent_id": "core-1", "container_id": "t1", "kind": "tube",
                     "mass_grams": 300.0, "opened_seal_id": "seal-1",
                     "parent_mass_after": 700.0, "seal": {"seal_id": "s-t1"},
                     "reseal": {"seal_id": "seal-1b"}}, at="2026-10-01T09:00:00Z")
        self.submit("aliquot",
                    {"parent_id": "core-2", "container_id": "t2", "kind": "tube",
                     "mass_grams": 200.0, "opened_seal_id": "seal-2",
                     "parent_mass_after": 600.0, "seal": {"seal_id": "s-t2"},
                     "reseal": {"seal_id": "seal-2b"}}, at="2026-10-01T09:05:00Z")
        merged = self.submit("merge",
                             {"parent_ids": ["t1", "t2"], "container_id": "pool",
                              "kind": "pool", "mass_grams": 490.0, "loss_grams": 10.0,
                              "opened_seal_ids": ["s-t1", "s-t2"],
                              "seal": {"seal_id": "s-p"}}, at="2026-10-01T10:00:00Z")
        self.assertTrue(merged.accepted, merged)
        for root in self.custody.conservation_report("p1"):
            self.assertIsNone(root["issue"], root)

    def test_alerts_identify_offline_temperature_seal_and_pending(self):
        self._build_chain()
        # 制造一条超温记录
        self.submit("environment", {"container_id": "core-1", "temp_c": 0.0,
                                    "temp_min": -25.0, "temp_max": -10.0},
                    at="2026-10-04T00:00:00Z")
        # 制造一条待裁决事件
        self.submit("open", {"container_id": "core-1", "opened_seal_id": "wrong"},
                    actor="rv1", at="2026-10-04T01:00:00Z")
        alerts = self.custody.list_alerts(offline_before="2026-10-08T00:00:00Z")
        self.assertTrue(any(d["device_id"] == "d1" for d in alerts["offline_devices"]))
        self.assertTrue(any(c["container_id"] == "core-1"
                            for c in alerts["temperature_excursions"]))
        self.assertTrue(any(c["reason_code"] == "seal_mismatch"
                            for c in alerts["pending_quarantine"]))


if __name__ == "__main__":
    unittest.main()
