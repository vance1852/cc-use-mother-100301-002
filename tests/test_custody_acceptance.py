import unittest

from polar_station_custody.acceptance import run


class CustodyAcceptanceTest(unittest.TestCase):
    def test_offline_acceptance(self):
        result = run()
        self.assertEqual("ok", result["status"])
        self.assertTrue(result["audit_valid"])
        self.assertTrue(result["split_confirmed"])
        self.assertTrue(result["device_replay_matched"])
        self.assertTrue(result["conflicting_reupload_quarantined"])
        self.assertEqual("unauthorized_unseal", result["quarantine_reasons"]["unauthorized_unseal"])
        self.assertEqual("contradiction", result["quarantine_reasons"]["contradiction"])
        self.assertEqual("clock_rollback", result["quarantine_reasons"]["clock_rollback"])
        self.assertTrue(result["accept_blocked"])
        self.assertEqual(3, result["provenance"]["containers"])
        self.assertEqual(["tube-002"], result["lost_contact"])
        self.assertEqual(["core-002"], result["over_temperature"])
        self.assertEqual(1, result["seal_anomalies_before"])
        self.assertEqual(4, result["pending_before"])
        self.assertEqual(1, result["pending_after"])
        reconciliation = result["reconciliation"]
        self.assertTrue(reconciliation["balanced"])
        self.assertEqual(2000.0, reconciliation["registered_total"])
        self.assertEqual(1900.0, reconciliation["current_total"])
        self.assertEqual(100.0, reconciliation["consumed_total"])


if __name__ == "__main__":
    unittest.main()
