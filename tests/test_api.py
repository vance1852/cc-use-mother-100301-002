import unittest

from polar_station_foundation.api import route
from polar_station_foundation.custody import CustodyService
from polar_station_foundation.service import DomainService
from polar_station_foundation.storage import Database


class ApiTest(unittest.TestCase):
    def setUp(self):
        self.database = Database()
        self.service = DomainService(self.database)

    def tearDown(self):
        self.database.close()

    def test_health_is_available_without_actor(self):
        status, payload = route(self.service, "GET", "/health", None)
        self.assertEqual(200, status)
        self.assertEqual("ok", payload["status"])

    def test_unknown_route_returns_404(self):
        status, payload = route(self.service, "GET", "/missing", None)
        self.assertEqual(404, status)
        self.assertEqual("route_not_found", payload["error"])

    def test_invalid_json_shape_returns_400(self):
        status, payload = route(self.service, "POST", "/organizations", {"request_id": "x"},
                                {"X-Actor-Id": "bootstrap"})
        self.assertEqual(400, status)
        self.assertEqual("invalid_request", payload["error"])


class CustodyApiTest(unittest.TestCase):
    def setUp(self):
        self.database = Database()
        self.service = DomainService(self.database)
        self.custody = CustodyService(self.database)
        self.headers = {"X-Actor-Id": "bootstrap", "X-Request-Id": ""}
        self.service.register_organization(request_id="org", actor_id="bootstrap",
                                           organization_id="o1", name="机构")
        self.service.register_actor(request_id="admin", actor_id="bootstrap", new_actor_id="a1",
                                    display_name="管理员", role="admin", organization_id="o1")
        self.service.register_actor(request_id="op", actor_id="a1", new_actor_id="op1",
                                    display_name="操作员", role="operator", organization_id="o1")
        self.service.register_site(request_id="site", actor_id="op1", site_id="s1",
                                   organization_id="o1", name="实验室", timezone_name="x")

    def tearDown(self):
        self.database.close()

    def call(self, method, path, body=None, actor="op1", request_id=""):
        return route(self.service, method, path, body or {},
                     {"X-Actor-Id": actor, "X-Request-Id": request_id},
                     custody=self.custody)

    def test_plan_event_quarantine_adjudication_and_provenance_over_http(self):
        status, payload = self.call(
            "POST", "/sampling-plans",
            {"site_id": "s1", "plan_id": "p1",
             "cores": [{"container_id": "core-1", "kind": "sea_ice_core", "mass_grams": 500.0,
                        "expected_seal_id": "seal-1"}]}, request_id="plan")
        self.assertEqual(201, status)

        status, payload = self.call(
            "POST", "/custody-events",
            {"event_type": "core_generated", "actor_id": "op1",
             "occurred_at": "2026-10-01T08:00:00Z",
             "device_id": "d1", "local_sequence": 1,
             "payload": {"plan_id": "p1", "container_id": "core-1", "mass_grams": 500.0,
                         "seal": {"seal_id": "seal-1"}}})
        self.assertEqual(200, status)
        self.assertTrue(payload["accepted"])
        # 同序列重传 -> replayed
        status, payload = self.call(
            "POST", "/custody-events",
            {"event_type": "core_generated", "actor_id": "op1",
             "occurred_at": "2026-10-01T08:00:00Z",
             "device_id": "d1", "local_sequence": 1,
             "payload": {"plan_id": "p1", "container_id": "core-1", "mass_grams": 500.0,
                         "seal": {"seal_id": "seal-1"}}})
        self.assertTrue(payload["replayed"])

        status, payload = self.call("GET", "/containers?plan_id=p1")
        self.assertEqual(200, status)
        self.assertEqual("core-1", payload["items"][0]["container_id"])

        status, payload = self.call(
            "POST", "/custody-events",
            {"event_type": "analysis", "actor_id": "op1",
             "occurred_at": "2026-10-01T09:00:00Z",
             "payload": {"analysis_id": "an-1", "container_id": "core-1",
                         "method": "m", "amount_grams": 5.0, "result": {"v": 1},
                         "opened_seal_id": "seal-1",
                         "instrument": {"instrument_id": "i", "calibration_id": "c",
                                        "calibrated_at": "2026-09-01T00:00:00Z"}}})
        self.assertTrue(payload["accepted"], payload)

        status, payload = self.call("GET", "/analyses/an-1/provenance")
        self.assertEqual(200, status)
        self.assertEqual("core-1", payload["container"]["container_id"])
        self.assertTrue(payload["custody_timeline"])

        # 异常事件进入隔离，管理员在 HTTP 上裁决
        status, payload = self.call(
            "POST", "/custody-events",
            {"event_type": "open", "actor_id": "op1",
             "occurred_at": "2026-10-01T10:00:00Z",
             "payload": {"container_id": "core-1", "opened_seal_id": "wrong"}})
        self.assertEqual(200, status)
        self.assertFalse(payload["accepted"])
        case_id = payload["case_id"]
        status, payload = self.call(
            "POST", f"/quarantine-cases/{case_id}/adjudication",
            {"decision": "reject", "note": "封签不符"}, actor="a1", request_id="dec1")
        self.assertEqual(201, status)
        status, payload = self.call("GET", "/quarantine-cases?status=rejected")
        self.assertEqual("rejected", payload["items"][0]["status"])

        status, payload = self.call("GET", "/alerts?before=2026-10-02T00:00:00Z")
        self.assertEqual(200, status)
        self.assertIn("offline_devices", payload)
        status, payload = self.call("GET", "/conservation-report?plan_id=p1")
        self.assertEqual(200, status)
        self.assertIsNone(payload["items"][0]["issue"])


if __name__ == "__main__":
    unittest.main()
