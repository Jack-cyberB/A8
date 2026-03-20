import unittest

from backend.server import REPO


class RepositoryTests(unittest.TestCase):
    def test_buildings(self):
        data = REPO.query_buildings()
        self.assertGreater(data["count"], 0)
        self.assertTrue(data["items"][0]["start_time"])

    def test_trend_returns_points(self):
        first_building = REPO.query_buildings()["items"][0]["building_id"]
        data = REPO.query_trend(building_id=first_building, start_time=None, end_time=None)
        self.assertGreater(data["point_count"], 100)
        self.assertGreater(data["summary"]["total_kwh"], 0)

    def test_anomaly_paging_and_sort(self):
        data = REPO.query_anomalies(
            building_id=None,
            start_time=None,
            end_time=None,
            anomaly_type=None,
            severity="high",
            page=1,
            page_size=10,
            sort="severity_desc",
        )
        self.assertLessEqual(data["count"], 10)
        self.assertGreaterEqual(data["total_count"], data["count"])
        self.assertEqual(data["page"], 1)

    def test_diagnosis_structure(self):
        anomalies = REPO.query_anomalies(None, None, None, None, None, 1, 10, "timestamp_desc")["items"]
        payload = {
            "message": "请分析这个异常",
            "anomaly_id": anomalies[0]["anomaly_id"],
        }
        data = REPO.diagnose(payload)
        diagnosis = data["diagnosis"]
        self.assertIn("anomaly_name", diagnosis)
        self.assertTrue(diagnosis["causes"])
        self.assertTrue(diagnosis["steps"])
        self.assertTrue(diagnosis["prevention"])
        self.assertIn("confidence", diagnosis)
        self.assertIn("risk_level", diagnosis)

    def test_metrics_overview(self):
        data = REPO.query_metrics_overview(None, None, None)
        self.assertIn("total_kwh", data)
        self.assertIn("anomaly_count", data)
        self.assertGreater(data["total_kwh"], 0)

    def test_saving_potential(self):
        data = REPO.query_saving_potential(None, None, None)
        self.assertIn("method_peak_excess_pct", data)
        self.assertIn("method_peer_compare", data)

    def test_anomaly_detail_extended_fields(self):
        anomalies = REPO.query_anomalies(None, None, None, None, None, 1, 5, "timestamp_desc")["items"]
        detail = REPO.query_anomaly_detail(anomalies[0]["anomaly_id"])
        self.assertIsNotNone(detail)
        self.assertIn("impact", detail)
        self.assertIn("baseline_window", detail)
        self.assertIn("peer_compare", detail)


if __name__ == "__main__":
    unittest.main()
