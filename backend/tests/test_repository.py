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

    def test_analysis_interfaces_electricity(self):
        buildings = REPO.query_buildings()["items"]
        building_id = buildings[0]["building_id"]
        summary = REPO.query_analysis_summary(building_id, None, None, "electricity")
        trend = REPO.query_analysis_trend(building_id, None, None, "electricity")
        distribution = REPO.query_analysis_distribution(building_id, None, None, "electricity")
        compare = REPO.query_analysis_compare(building_id, None, None, "electricity")

        self.assertEqual(summary["metric_type"], "electricity")
        self.assertGreater(summary["total_value"], 0)
        self.assertGreater(len(trend["series"]), 0)
        self.assertIn("overlay_available", trend)
        self.assertIn("comparison_series", trend)
        self.assertIn("markers", trend)
        self.assertEqual(len(distribution["hourly_profile"]), 24)
        self.assertEqual(len(distribution["weekday_weekend_split"]), 2)
        self.assertIn("weekday_peak_hours", distribution)
        self.assertIn("night_base_load", distribution)
        self.assertEqual(len(compare["items"]), 2)
        self.assertIn("peer_percentile", compare["peer_group"])
        self.assertIn("gap_pct", compare["peer_group"])

    def test_analysis_insights_shape(self):
        buildings = REPO.query_buildings()["items"]
        building_id = buildings[0]["building_id"]
        insights = REPO.query_analysis_insights(building_id, None, None, "electricity")

        self.assertIn("scope_summary", insights)
        self.assertIn("trend_findings", insights)
        self.assertIn("weather_findings", insights)
        self.assertIn("compare_findings", insights)
        self.assertIn("saving_opportunities", insights)
        self.assertIn("anomaly_windows", insights)
        self.assertGreater(insights["scope_summary"]["point_count"], 0)

    def test_analysis_interfaces_unsupported_metric(self):
        with self.assertRaises(ValueError):
            REPO.query_analysis_summary(None, None, None, "water")

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
        self.assertIn("trace_id", d)
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

    def test_ai_analyze_template_shape(self):
        buildings = REPO.query_buildings()["items"]
        result = REPO.analyze(
            {
                "provider": "template",
                "building_id": buildings[0]["building_id"],
                "metric_type": "electricity",
            }
        )
        analysis = result["analysis"]
        self.assertIn("summary", analysis)
        self.assertIn("findings", analysis)
        self.assertIn("energy_saving_suggestions", analysis)
        self.assertIn("operations_suggestions", analysis)
        self.assertIn("trace_id", analysis)
        self.assertFalse(analysis["fallback_used"])

    def test_ai_analyze_llm_success_mock(self):
        buildings = REPO.query_buildings()["items"]
        fake_response = {
            "choices": [
                {
                    "message": {
                        "content": '{"summary":"LLM分析","findings":["发现1"],"possible_causes":["原因1"],"energy_saving_suggestions":["建议1"],"operations_suggestions":["动作1"],"evidence":["证据1"]}'
                    }
                }
            ]
        }
        with mock.patch.dict("os.environ", {"OPENAI_API_KEY": "dummy-key"}, clear=False):
            with mock.patch("backend.server.LLMDiagnoseProvider._call_chat_completion", return_value=fake_response):
                result = REPO.analyze({"provider": "llm", "building_id": buildings[0]["building_id"], "metric_type": "electricity"})

        analysis = result["analysis"]
        self.assertEqual(analysis["provider"], "llm_provider")
        self.assertEqual(analysis["summary"], "LLM分析")
        self.assertEqual(analysis["findings"][0], "发现1")

    def test_ai_stats_shape(self):
        aid = self._first_anomaly_id()
        _ = REPO.diagnose({"message": "请分析异常", "anomaly_id": aid, "provider": "template"})
        stats = REPO.query_ai_stats(24)
        self.assertIn("total_calls", stats)
        self.assertIn("fallback_rate_pct", stats)
        self.assertGreaterEqual(stats["total_calls"], 1)

    def test_anomaly_note_upsert_and_detail(self):
        aid = self._first_anomaly_id()
        saved = REPO.upsert_anomaly_note(
            {
                "anomaly_id": aid,
                "cause_confirmed": "传感器抖动",
                "action_taken": "复位并巡检",
                "result_summary": "负荷恢复稳定",
                "recurrence_risk": "low",
                "reviewer": "qa",
            }
        )
        self.assertEqual(saved["anomaly_id"], aid)
        self.assertEqual(saved["recurrence_risk"], "low")
        detail = REPO.query_anomaly_detail(aid)
        self.assertEqual(detail["postmortem_note"]["result_summary"], "负荷恢复稳定")

    def test_export_csv_and_health(self):
        csv_text = REPO.export_anomalies_csv(None, None, None, None, None, None)
        self.assertIn("anomaly_id,building_id,building_name", csv_text)
        health = REPO.query_system_health()
        self.assertIn("status", health)
        self.assertIn("recent_regression", health)

    def test_ai_evaluate_and_feedback(self):
        aid = self._first_anomaly_id()
        diagnosed = REPO.diagnose({"message": "请分析异常", "anomaly_id": aid, "provider": "template"})
        trace_id = diagnosed["diagnosis"]["trace_id"]
        feedback = REPO.save_ai_feedback({"trace_id": trace_id, "label": "useful"})
        self.assertEqual(feedback["label"], "useful")
        evaluate = REPO.query_ai_evaluate(24)
        self.assertIn("template", evaluate)
        self.assertIn("feedback", evaluate)


if __name__ == "__main__":
    unittest.main()
