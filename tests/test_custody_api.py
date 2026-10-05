import unittest
from datetime import datetime, timezone

from polar_station_foundation.clock import FixedClock
from polar_station_foundation.service import DomainService

from polar_station_custody import CustodyDatabase, CustodyService
from polar_station_custody.api import route


class CustodyApiTest(unittest.TestCase):
    def setUp(self):
        self.database = CustodyDatabase()
        clock = FixedClock(datetime(2026, 10, 1, 8, 0, tzinfo=timezone.utc))
        self.foundation = DomainService(self.database, clock)
        self.custody = CustodyService(self.database, clock)
        self.foundation.register_organization(request_id="org", actor_id="bootstrap",
                                              organization_id="o1", name="科考机构")
        self.foundation.register_actor(request_id="admin", actor_id="bootstrap", new_actor_id="a1",
                                       display_name="管理员", role="admin", organization_id="o1")
        self.foundation.register_actor(request_id="op", actor_id="a1", new_actor_id="op1",
                                       display_name="操作员", role="operator", organization_id="o1")
        self.foundation.register_site(request_id="site", actor_id="a1", site_id="s1",
                                      organization_id="o1", name="实验室", timezone_name="Asia/Shanghai")
        self.custody.register_plan(request_id="plan", actor_id="op1", plan_id="p1", site_id="s1",
                                   title="计划", unit="g",
                                   containers=[{"container_id": "c1", "label": "岩芯", "quantity": 100,
                                                "custodian_id": "op1", "location": "冷柜A"}])

    def tearDown(self):
        self.database.close()

    def call(self, method, path, body=None, actor="op1"):
        return route(self.custody, self.foundation, method, path, body,
                     {"X-Actor-Id": actor})

    def test_foundation_health_delegated(self):
        status, payload = self.call("GET", "/health")
        self.assertEqual(200, status)
        self.assertEqual("ok", payload["status"])

    def test_submit_event_and_replay_via_http(self):
        body = {"site_id": "s1", "request_id": "r1", "event_type": "weigh",
                "occurred_at": "2026-10-01T07:00:00Z", "base_versions": {"c1": 1},
                "detail": {"container_id": "c1", "observed_quantity": 100}}
        status, payload = self.call("POST", "/custody/events", body)
        self.assertEqual(201, status)
        self.assertEqual("confirmed", payload["status"])
        status, payload = self.call("POST", "/custody/events", body)
        self.assertEqual(200, status)
        self.assertTrue(payload["replayed"])

    def test_quarantined_event_returns_202(self):
        body = {"site_id": "s1", "request_id": "r2", "event_type": "consume",
                "occurred_at": "2026-10-01T07:00:00Z", "base_versions": {"c1": 1},
                "detail": {"container_id": "c1", "amount": 9999}}
        status, payload = self.call("POST", "/custody/events", body)
        self.assertEqual(202, status)
        self.assertEqual("quarantined", payload["status"])
        self.assertIsNotNone(payload["case_id"])

    def test_queries_via_http(self):
        self.call("POST", "/custody/events",
                  {"site_id": "s1", "request_id": "r3", "event_type": "derive",
                   "occurred_at": "2026-10-01T07:00:00Z", "base_versions": {"c1": 1},
                   "detail": {"parent_id": "c1",
                              "analysis": {"container_id": "a1", "result_id": "res1", "quantity": 10}}})
        status, payload = self.call("GET", "/custody/provenance?result_id=res1")
        self.assertEqual(200, status)
        self.assertEqual(2, len(payload["lineage"]))
        status, payload = self.call("GET", "/custody/reconciliation?plan_id=p1")
        self.assertEqual(200, status)
        self.assertTrue(payload["balanced"])
        status, payload = self.call("GET", "/custody/overview?site_id=s1")
        self.assertEqual(200, status)
        self.assertIn("pending_adjudication", payload)
        status, payload = self.call("GET", "/custody/containers?container_id=c1")
        self.assertEqual(200, status)
        self.assertEqual("c1", payload["container"]["container_id"])

    def test_device_event_via_http(self):
        self.call("POST", "/custody/devices",
                  {"request_id": "d1", "device_id": "dev1", "site_id": "s1", "label": "终端"})
        status, payload = self.call("POST", "/custody/events",
                                    {"site_id": "s1", "device_id": "dev1", "local_sequence": 1,
                                     "event_type": "environment", "occurred_at": "2026-09-30T08:00:00Z",
                                     "base_versions": {"c1": 1},
                                     "detail": {"container_id": "c1", "temperature_c": -20.0}})
        self.assertEqual(201, status)

    def test_unknown_custody_route_returns_404(self):
        status, payload = self.call("GET", "/custody/missing")
        self.assertEqual(404, status)
        self.assertEqual("route_not_found", payload["error"])

    def test_invalid_body_returns_400(self):
        status, payload = self.call("POST", "/custody/events", {"site_id": "s1"})
        self.assertEqual(400, status)
        self.assertEqual("invalid_request", payload["error"])

    def test_missing_actor_returns_404(self):
        status, payload = self.call("POST", "/custody/events",
                                    {"site_id": "s1", "request_id": "r9", "event_type": "weigh",
                                     "base_versions": {"c1": 1},
                                     "detail": {"container_id": "c1", "observed_quantity": 1}},
                                    actor="")
        self.assertEqual(404, status)


if __name__ == "__main__":
    unittest.main()
