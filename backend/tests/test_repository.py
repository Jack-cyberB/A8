import unittest
from unittest import mock

from backend.server import REPO


class RepositoryTests(unittest.TestCase):
    def _first_anomaly_id(self) -> int:
        data = REPO.query_anomalies(None, None, None, None, None, None, 1, 5, "timestamp_desc")
        self.assertGreater(data["count"], 0)
        return int(data["items"][0]["anomaly_id"])

    def test_buildings(self):
        data = REPO.query_buildings()
        self.assertGreater(data["count"], 0)

    def test_anomaly_action_flow(self):
        anomalies = REPO.query_anomalies(None, None, None, None, None, "new", 1, 50, "timestamp_desc")["items"]
        self.assertGreater(len(anomalies), 0)
        aid = int(anomalies[0]["anomaly_id"])

        s1 = REPO.apply_anomaly_action({"anomaly_id": aid, "action": "ack", "assignee": "tester", "note": "ack"})
        self.assertEqual(s1["status"], "acknowledged")

        s2 = REPO.apply_anomaly_action({"anomaly_id": aid, "action": "resolve", "assignee": "tester", "note": "done"})
        self.assertEqual(s2["status"], "resolved")

        history = REPO.query_anomaly_history(aid)
        self.assertIsNotNone(history)
        self.assertGreaterEqual(history["count"], 2)

    def test_anomaly_list_has_status_fields(self):
        data = REPO.query_anomalies(None, None, None, None, None, None, 1, 10, "timestamp_desc")
        self.assertGreater(data["count"], 0)
        item = data["items"][0]
        self.assertIn("status", item)
        self.assertIn("assignee", item)

    def test_anomaly_detail_has_processing_summary(self):
        aid = self._first_anomaly_id()
        detail = REPO.query_anomaly_detail(aid)
        self.assertIsNotNone(detail)
        self.assertIn("processing_summary", detail)

    def test_diagnose_template_shape(self):
        aid = self._first_anomaly_id()
        result = REPO.diagnose({"message": "请分析异常", "anomaly_id": aid, "provider": "template"})
        d = result["diagnosis"]
        self.assertIn("conclusion", d)
        self.assertIn("causes", d)
        self.assertIn("provider", d)
        self.assertIn("latency_ms", d)
        self.assertFalse(d["fallback_used"])

    def test_diagnose_llm_fallback(self):
        aid = self._first_anomaly_id()
        result = REPO.diagnose({"message": "请分析异常", "anomaly_id": aid, "provider": "llm", "simulate_llm_failure": True})
        d = result["diagnosis"]
        self.assertTrue(d["fallback_used"])
        self.assertEqual(d["provider"], "template_provider")

    def test_diagnose_llm_success_mock(self):
        aid = self._first_anomaly_id()
        fake_response = {
            "choices": [
                {
                    "message": {
                        "content": '{"conclusion":"LLM结论","causes":["原因A"],"steps":["步骤1"],"prevention":["预防1"],"recommended_actions":["动作1"],"evidence":["证据1"],"confidence":0.81,"risk_level":"high"}'
                    }
                }
            ]
        }
        with mock.patch.dict("os.environ", {"OPENAI_API_KEY": "dummy-key"}, clear=False):
            with mock.patch("backend.server.LLMDiagnoseProvider._call_chat_completion", return_value=fake_response):
                result = REPO.diagnose({"message": "请分析异常", "anomaly_id": aid, "provider": "llm"})

        d = result["diagnosis"]
        self.assertFalse(d["fallback_used"])
        self.assertEqual(d["provider"], "llm_provider")
        self.assertEqual(d["conclusion"], "LLM结论")
        self.assertEqual(d["causes"][0], "原因A")

    def test_ai_stats_shape(self):
        aid = self._first_anomaly_id()
        _ = REPO.diagnose({"message": "请分析异常", "anomaly_id": aid, "provider": "template"})
        stats = REPO.query_ai_stats(24)
        self.assertIn("total_calls", stats)
        self.assertIn("fallback_rate_pct", stats)
        self.assertGreaterEqual(stats["total_calls"], 1)


if __name__ == "__main__":
    unittest.main()
