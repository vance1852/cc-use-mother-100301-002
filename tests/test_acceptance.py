import unittest

from polar_station_foundation.acceptance import run


class AcceptanceTest(unittest.TestCase):
    def test_offline_acceptance(self):
        result = run()
        self.assertEqual("ok", result["status"])
        self.assertTrue(result["audit_valid"])
        self.assertTrue(result["replay_replayed"])
        self.assertEqual(5, result["containers"])
        self.assertEqual(2, result["conservation_roots"])
        self.assertTrue(all(result["custody_timeline_events"] >= 1 for _ in [0]))


if __name__ == "__main__":
    unittest.main()
