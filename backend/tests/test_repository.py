import os
import json
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest import mock

os.environ["STORAGE_BACKEND"] = "file"

from backend.mysql_support import MySQLClient
from backend.server import (
    DEMO_DATA_FILE,
    DICT_FILE,
    FileRepository,
    KNOWLEDGE_FILE,
    METADATA_FILE,
    NORMALIZED_DATA_FILE,
    NOTE_LOG_FILE,
    REGRESSION_SUMMARY_FILE,
    REPO,
    WEATHER_FILE,
    MySQLRepository,
    create_repository,
)


class FakeMySQLClient:
    def __init__(self, counts=None, rows=None):
        self.counts = counts or {}
        self.rows = rows or {}
        self.executed = []

    def ensure_schema(self):
        self.executed.append("ensure_schema")

    def health(self):
        return {
            "configured": True,
            "available": True,
            "connected": True,
            "host": "127.0.0.1",
            "port": 3306,
            "database": "a8_test",
            "user": "root",
            "error": "",
        }

    def query_scalar(self, sql, include_database=True, timeout_sec=60):
        if "FROM ai_calls WHERE trace_id" in sql:
            return "1"
        for table_name, value in self.counts.items():
            if f"FROM {table_name}" in sql:
                return str(value)
        if "FROM buildings" in sql:
            return str(len(self.rows.get("buildings", [])))
        if "FROM energy_timeseries" in sql:
            return str(len(self.rows.get("energy_timeseries", [])))
        if "FROM weather_timeseries" in sql:
            return str(len(self.rows.get("weather_timeseries", [])))
        if "FROM anomaly_actions" in sql:
            return str(len(self.rows.get("anomaly_actions", [])))
        if "FROM anomaly_notes" in sql:
            return str(len(self.rows.get("anomaly_notes", [])))
        if "FROM ai_calls" in sql:
            return str(len(self.rows.get("ai_calls", [])))
        if "FROM system_snapshots" in sql:
            return str(len(self.rows.get("system_snapshots", [])))
        return "0"

    def query_json_rows(self, sql, include_database=True, timeout_sec=60):
        if "FROM buildings" in sql:
            return list(self.rows.get("buildings", []))
        if "FROM energy_timeseries" in sql:
            return list(self.rows.get("energy_timeseries", []))
        if "FROM weather_timeseries" in sql:
            return list(self.rows.get("weather_timeseries", []))
        if "FROM anomaly_actions" in sql:
            return list(self.rows.get("anomaly_actions", []))
        if "FROM anomaly_notes" in sql:
            return list(self.rows.get("anomaly_notes", []))
        if "FROM ai_calls" in sql:
            return list(self.rows.get("ai_calls", []))
        if "FROM system_snapshots" in sql:
            return list(self.rows.get("system_snapshots", []))
        return []

    def query_rows(self, sql, params=None, include_database=True, timeout_sec=60):
        if "SELECT DISTINCT building_id" in sql and "FROM energy_timeseries" in sql:
            seen = set()
            items = []
            for row in self.rows.get("energy_timeseries", []):
                building_id = str(row.get("building_id", "")).strip()
                if building_id and building_id not in seen:
                    seen.add(building_id)
                    items.append({"building_id": building_id})
            return items
        if "AVG(value) AS avg_value" in sql and "FROM energy_timeseries" in sql:
            candidate_ids = list(params or [])
            grouped = {}
            for row in self.rows.get("energy_timeseries", []):
                building_id = str(row.get("building_id", "")).strip()
                if candidate_ids and building_id not in candidate_ids:
                    continue
                grouped.setdefault(building_id, []).append(float(row.get("electricity_kwh") or row.get("value") or 0.0))
            return [
                {"building_id": building_id, "avg_value": round(sum(values) / len(values), 6)}
                for building_id, values in grouped.items()
                if values
            ]
        return []

    def execute(self, sql, include_database=True, timeout_sec=60):
        self.executed.append(sql)


class RepositoryTests(unittest.TestCase):
    _mysql_seed_cache = None

    def _first_anomaly_id(self) -> int:
        data = REPO.query_anomalies(None, None, None, None, None, None, 1, 5, "timestamp_desc")
        self.assertGreater(data["count"], 0)
        return int(data["items"][0]["anomaly_id"])

    def _empty_runtime_files(self, root: Path) -> tuple[Path, Path, Path, Path]:
        action_file = root / "anomaly_actions.jsonl"
        ai_file = root / "ai_calls.jsonl"
        note_file = root / "anomaly_notes.jsonl"
        regression_file = root / "regression_summary.json"
        action_file.write_text("", encoding="utf-8")
        ai_file.write_text("", encoding="utf-8")
        note_file.write_text("", encoding="utf-8")
        regression_file.write_text(json.dumps({"status": "unknown", "steps": []}, ensure_ascii=False), encoding="utf-8")
        return action_file, ai_file, note_file, regression_file

    @classmethod
    def _mysql_seed_rows(cls):
        if cls._mysql_seed_cache is None:
            weather_rows = []
            for site_id, by_time in REPO.weather_by_site.items():
                for timestamp, values in by_time.items():
                    weather_rows.append(
                        {
                            "site_id": site_id,
                            "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                            "temperature_c": values.get("temperature_c", 0.0),
                            "wind_speed": values.get("wind_speed", 0.0),
                        }
                    )
            cls._mysql_seed_cache = {
                "buildings": [dict(meta) for meta in REPO.bdq2_metadata.values()],
                "energy_timeseries": [
                    {
                        "record_id": row.get("record_id"),
                        "building_id": row.get("building_id"),
                        "building_name": row.get("building_name"),
                        "building_type": row.get("building_type"),
                        "timestamp": row["timestamp"].strftime("%Y-%m-%d %H:%M:%S"),
                        "hour": row.get("hour"),
                        "electricity_kwh": row.get("electricity_kwh"),
                        "source": row.get("source", "normalized"),
                    }
                    for row in REPO.rows
                ],
                "weather_timeseries": weather_rows,
                "anomaly_actions": [],
                "anomaly_notes": [],
                "ai_calls": [],
                "system_snapshots": [],
            }
        return {
            key: [dict(item) for item in value]
            for key, value in cls._mysql_seed_cache.items()
        }

    def test_buildings(self):
        data = REPO.query_buildings()
        self.assertGreater(data["count"], 0)

    def test_create_repository_defaults_to_mysql(self):
        sentinel = object()

        class StubMySQLRepository:
            def __init__(self, *args, **kwargs):
                self.marker = sentinel

        with mock.patch.dict("os.environ", {"STORAGE_BACKEND": ""}, clear=False):
            with mock.patch("backend.server.MySQLRepository", StubMySQLRepository):
                repo = create_repository()
        self.assertIs(repo.marker, sentinel)

    def test_create_repository_uses_file_backend_when_requested(self):
        with mock.patch.dict("os.environ", {"STORAGE_BACKEND": "file"}, clear=False):
            repo = create_repository()
        self.assertIsInstance(repo, FileRepository)

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
        if insights["saving_opportunities"]:
            first_opportunity = insights["saving_opportunities"][0]
            self.assertIn("estimated_kwh", first_opportunity)
            self.assertIn("estimated_loss_kwh", first_opportunity)

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
        self.assertIn("rule_name", item)
        self.assertIn("baseline_value", item)
        self.assertIn("threshold_value", item)
        self.assertIn("trigger_window", item)
        self.assertGreater(len(data.get("available_types", [])), 0)

    def test_anomaly_detail_has_processing_summary(self):
        aid = self._first_anomaly_id()
        detail = REPO.query_anomaly_detail(aid)
        self.assertIsNotNone(detail)
        self.assertIn("processing_summary", detail)
        self.assertIn("rule_explanation", detail)

    def test_anomaly_types_are_humanized(self):
        data = REPO.query_anomalies(None, None, None, None, None, None, 1, 50, "timestamp_desc")
        self.assertTrue(any(not key.startswith("anomaly_") for key in data.get("by_type", {}).keys()))

    def test_anomaly_detection_has_multiple_rule_types(self):
        types = {str(item.get("anomaly_type")) for item in REPO.anomalies}
        self.assertGreaterEqual(len(types), 4)

    def test_diagnose_template_shape(self):
        aid = self._first_anomaly_id()
        result = REPO.diagnose({"message": "请分析异常", "anomaly_id": aid, "provider": "template"})
        d = result["diagnosis"]
        self.assertIn("conclusion", d)
        self.assertIn("causes", d)
        self.assertGreaterEqual(len(d["causes"]), 3)
        self.assertGreaterEqual(len(d["steps"]), 3)
        self.assertGreaterEqual(len(d["recommended_actions"]), 3)
        self.assertIn("provider", d)
        self.assertIn("latency_ms", d)
        self.assertIn("trace_id", d)
        self.assertIn("data_evidence", d)
        self.assertGreaterEqual(len(d["data_evidence"]), 1)
        self.assertFalse(d["fallback_used"])

    def test_diagnose_stream_emits_structured_template_and_done(self):
        aid = self._first_anomaly_id()
        with mock.patch.dict("os.environ", {"OPENAI_API_KEY": ""}, clear=False):
            events = list(REPO.diagnose_stream_events({"message": "请分析异常", "anomaly_id": aid, "provider": "auto"}))
        self.assertGreaterEqual(len(events), 2)
        self.assertEqual(events[0][0], "template")
        self.assertGreaterEqual(len(events[0][1].get("steps", [])), 3)
        self.assertEqual(events[-1][0], "done")
        self.assertTrue(events[-1][1].get("fallback_used"))

    def test_diagnose_llm_failure_raises(self):
        aid = self._first_anomaly_id()
        with self.assertRaises(RuntimeError) as ctx:
            REPO.diagnose({"message": "请分析异常", "anomaly_id": aid, "provider": "llm", "simulate_llm_failure": True})
        self.assertIn("Simulated llm failure", str(ctx.exception))

    def test_diagnose_llm_success_mock(self):
        aid = self._first_anomaly_id()
        fake_response = {
            "choices": [
                {
                    "message": {
                        "content": '{"conclusion":"LLM结论","causes":["空调系统在午间异常满负荷运行，和当前偏差水平一致","新风联动未按课表切换，导致教学时段额外抬升负荷","局部照明或专用设备长开，使该时点明显高于24h基线"],"steps":["先核对该时段总表、分项表和分路数据，确认不是采集坏值","再检查空调、新风和照明排程及手自动状态","最后结合现场值班记录和课表核实是否存在计划外用能"],"prevention":["按课表固化教学楼空调和新风排程","建立午间高负荷阈值告警并联动分项回路","把异常时段操作记录纳入每周复盘"],"recommended_actions":["立即复核午间空调和新风运行状态","临时关闭不必要的高负荷回路并观察曲线回落","通知值班人员核对现场是否存在计划外设备开启"],"evidence":["证据1"],"confidence":0.81,"risk_level":"high"}'
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
        self.assertIn("空调系统", d["causes"][0])

    def test_diagnose_default_provider_prefers_llm(self):
        aid = self._first_anomaly_id()
        fake_response = {
            "choices": [
                {
                    "message": {
                        "content": '{"conclusion":"默认LLM结论","causes":["空调系统在午间异常满负荷运行，和当前偏差水平一致","新风联动未按课表切换，导致教学时段额外抬升负荷","局部照明或专用设备长开，使该时点明显高于24h基线"],"steps":["先核对该时段总表、分项表和分路数据，确认不是采集坏值","再检查空调、新风和照明排程及手自动状态","最后结合现场值班记录和课表核实是否存在计划外用能"],"prevention":["按课表固化教学楼空调和新风排程","建立午间高负荷阈值告警并联动分项回路","把异常时段操作记录纳入每周复盘"],"recommended_actions":["立即复核午间空调和新风运行状态","临时关闭不必要的高负荷回路并观察曲线回落","通知值班人员核对现场是否存在计划外设备开启"],"evidence":["证据1"],"confidence":0.76,"risk_level":"medium"}'
                    }
                }
            ]
        }
        with mock.patch.dict("os.environ", {"OPENAI_API_KEY": "dummy-key"}, clear=False):
            with mock.patch("backend.server.LLMDiagnoseProvider._call_chat_completion", return_value=fake_response):
                result = REPO.diagnose({"message": "请分析异常", "anomaly_id": aid})

        d = result["diagnosis"]
        self.assertEqual(d["requested_provider"], "auto")
        self.assertEqual(d["provider"], "llm_provider")
        self.assertFalse(d["fallback_used"])

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
        self.assertGreaterEqual(len(analysis["findings"]), 3)
        self.assertGreaterEqual(len(analysis["energy_saving_suggestions"]), 3)
        self.assertGreaterEqual(len(analysis["operations_suggestions"]), 3)
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

    def test_ai_analyze_llm_failure_raises(self):
        buildings = REPO.query_buildings()["items"]
        with self.assertRaises(RuntimeError) as ctx:
            REPO.analyze(
                {
                    "provider": "llm",
                    "building_id": buildings[0]["building_id"],
                    "metric_type": "electricity",
                    "simulate_llm_failure": True,
                }
            )
        self.assertIn("Simulated llm failure", str(ctx.exception))

    def test_ai_analyze_default_provider_prefers_llm(self):
        buildings = REPO.query_buildings()["items"]
        fake_response = {
            "choices": [
                {
                    "message": {
                        "content": '{"summary":"默认LLM分析","findings":["发现1"],"possible_causes":["原因1"],"energy_saving_suggestions":["建议1"],"operations_suggestions":["动作1"],"evidence":["证据1"]}'
                    }
                }
            ]
        }
        with mock.patch.dict("os.environ", {"OPENAI_API_KEY": "dummy-key"}, clear=False):
            with mock.patch("backend.server.LLMDiagnoseProvider._call_chat_completion", return_value=fake_response):
                result = REPO.analyze({"building_id": buildings[0]["building_id"], "metric_type": "electricity"})

        analysis = result["analysis"]
        self.assertEqual(analysis["requested_provider"], "auto")
        self.assertEqual(analysis["provider"], "llm_provider")
        self.assertFalse(analysis["fallback_used"])

    def test_ragflow_success_used_for_analysis_evidence(self):
        buildings = REPO.query_buildings()["items"]
        ragflow_items = [
            {
                "chunk_id": "rag-1",
                "title": "03-校园与教育建筑运维场景.md",
                "section": "相似度 0.88",
                "excerpt": "教学楼在高温时段应优先核查通风与空调排程。",
                "source_type": "ragflow",
            }
        ]
        with mock.patch.object(
            REPO,
            "_retrieve_ragflow_knowledge",
            return_value={
                "items": ragflow_items,
                "knowledge_source": "ragflow",
                "retrieval_hit_count": 1,
                "retrieval_error_type": "",
            },
        ):
            result = REPO.analyze({"provider": "template", "building_id": buildings[0]["building_id"], "metric_type": "electricity"})

        analysis = result["analysis"]
        self.assertEqual(analysis["knowledge_source"], "ragflow")
        self.assertEqual(analysis["retrieval_hit_count"], 1)
        self.assertEqual(analysis["evidence"][0]["source_type"], "ragflow")

    def test_ragflow_empty_falls_back_to_local_knowledge(self):
        aid = self._first_anomaly_id()
        local_items = [
            {
                "chunk_id": "local-1",
                "title": "local",
                "section": "chunk-1",
                "excerpt": "夜间基线偏高通常与常开设备相关。",
                "source_type": "local_knowledge",
            }
        ]
        with mock.patch.object(
            REPO,
            "_retrieve_ragflow_knowledge",
            return_value={
                "items": [],
                "knowledge_source": "none",
                "retrieval_hit_count": 0,
                "retrieval_error_type": "empty_result",
            },
        ):
            with mock.patch.object(REPO, "_search_local_knowledge", return_value=local_items):
                result = REPO.diagnose({"message": "请分析异常", "anomaly_id": aid, "provider": "template"})

        diagnosis = result["diagnosis"]
        self.assertEqual(diagnosis["knowledge_source"], "local")
        self.assertEqual(diagnosis["retrieval_error_type"], "empty_result")
        self.assertEqual(diagnosis["evidence"][0]["source_type"], "local_knowledge")

    def test_ragflow_chat_proxy_uses_native_chat_completion(self):
        native_result = {
            "answer": "结论：[ID:3]教育机构能耗定额应按分类和统计周期管理。",
            "reference": {
                "chunks": [
                    {
                        "id": "std-ref-1",
                        "dataset_id": "dataset-std-1",
                        "document_name": "DB37T 2671-2019 教育机构能源消耗定额标准.md",
                        "content": "教育机构能源消耗定额应按分类和统计周期管理。",
                        "similarity": 0.9132,
                    }
                ]
            },
            "session_id": "ragflow-session-1",
            "message_id": "ragflow-message-1",
            "latency_ms": 420,
        }
        with mock.patch.object(REPO, "_answer_knowledge_question", side_effect=AssertionError("should not use DeepSeek rewrite path")):
            with mock.patch.object(REPO, "_ragflow_chat_completion", return_value=native_result):
                with mock.patch.object(
                    REPO,
                    "_ragflow_settings",
                    return_value={"chat_id": "chat-native-1", "standard_dataset_ids": ["dataset-std-1"]},
                ):
                    result = REPO.ask_ragflow_chat({"question": "教育机构能耗定额标准是什么？"})

        self.assertEqual(result["provider"], "ragflow_chat")
        self.assertEqual(result["session_id"], "ragflow-session-1")
        self.assertEqual(result["message_id"], "ragflow-message-1")
        self.assertEqual(result["knowledge_source"], "standard")
        self.assertEqual(result["answer"], "结论：[ID:3]教育机构能耗定额应按分类和统计周期管理。")
        self.assertEqual(result["references"][0]["source_type"], "standard")
        self.assertEqual(result["references"][0]["title"], "DB37T 2671-2019 教育机构能源消耗定额标准")
        self.assertEqual(result["references"][0]["document_key"], "DB37T 2671-2019 教育机构能源消耗定额标准")
        self.assertEqual(result["references"][0]["snippet_text"], "教育机构能源消耗定额应按分类和统计周期管理。")

    def test_clean_ragflow_answer_text_removes_inline_citations(self):
        cleaned = REPO._clean_ragflow_answer_text("建议先核查空调排程[ID:3][ID:0][1]。\n\n")
        self.assertEqual(cleaned, "建议先核查空调排程。")

    def test_postprocess_knowledge_answer_preserves_decimal_ranges(self):
        text = "根据资料，通风窗下沿宜设在距室内楼地面以上0.10m~0.15m高度处，以利于通风。"
        processed = REPO._postprocess_knowledge_answer(text)
        self.assertIn("0.10m~0.15m高度处", processed)
        self.assertNotIn("• 10m", processed)

    def test_postprocess_knowledge_answer_only_converts_line_start_numbering(self):
        text = "1. 先检查排程。\n2、再检查新风阀。\n室温建议控制在1.5℃波动范围内。"
        processed = REPO._postprocess_knowledge_answer(text)
        self.assertIn("1. 先检查排程。", processed)
        self.assertIn("2、再检查新风阀。", processed)
        self.assertIn("1.5℃波动范围内", processed)

    def test_postprocess_knowledge_answer_only_removes_heading_line(self):
        text = "结论：\n教学楼应优先采用自然通风，并核查空调设定。"
        processed = REPO._postprocess_knowledge_answer(text)
        self.assertEqual(processed, "教学楼应优先采用自然通风，并核查空调设定。")

    def test_merge_knowledge_retrieval_results_deduplicates_and_preserves_mixed_source(self):
        merged = REPO._merge_knowledge_retrieval_results(
            "教学楼通风要求",
            {
                "items": [
                    {
                        "chunk_id": "scene-1",
                        "title": "校园运维场景",
                        "excerpt": "教学楼空调运行时应检查新风与排风。",
                        "source_type": "ragflow",
                        "similarity": 0.71,
                    }
                ]
            },
            {
                "items": [
                    {
                        "chunk_id": "std-1",
                        "title": "GB 50365-2019 空调通风系统运行管理标准",
                        "excerpt": "通风系统运行管理应结合监测数据进行调整。",
                        "source_type": "standard",
                        "similarity": 0.69,
                    }
                ]
            },
            limit=4,
        )
        self.assertEqual(merged["knowledge_source"], "mixed")
        self.assertEqual(len(merged["items"]), 2)
        self.assertEqual({item["source_type"] for item in merged["items"]}, {"ragflow", "standard"})

    def test_extract_ragflow_excerpt_prefers_useful_qa_content(self):
        excerpt = REPO._extract_ragflow_excerpt(
            {
                "content": "Question: 教学楼空调系统夜间负荷偏高怎么办？\nAnswer: <p>应先检查排程、送风模式和新风阀状态。</p>",
            }
        )
        self.assertIn("问题：教学楼空调系统夜间负荷偏高怎么办？", excerpt)
        self.assertIn("答案：应先检查排程、送风模式和新风阀状态。", excerpt)

    def test_noisy_ragflow_chunk_is_filtered(self):
        noisy = REPO._extract_ragflow_excerpt({"content": "Question: 文化场馆与文保环境控制\nAnswer: <p>共 60 条问答。</p>"})
        self.assertEqual(noisy, "")
        self.assertTrue(REPO._is_noisy_ragflow_chunk({"content": "[fQTQT w w w . x u e t u t u . c o m\n" * 3}, ""))

    def test_knowledge_route_for_question_distinguishes_standard_and_scene(self):
        self.assertEqual(REPO._knowledge_route_for_question("教育机构能耗定额标准是什么？"), "standard")
        self.assertEqual(REPO._knowledge_route_for_question("教学楼夜间负荷偏高一般先排查什么？"), "scene")
        self.assertEqual(REPO._knowledge_route_for_question("公共建筑节能监测系统有哪些要求，实际运维该怎么做？"), "mixed")

    def test_ragflow_dataset_route_promotes_conceptual_scene_question_to_mixed(self):
        with mock.patch.object(
            REPO,
            "_ragflow_settings",
            return_value={
                "scene_dataset_ids": ["scene-a"],
                "standard_dataset_ids": ["std-a"],
            },
        ):
            route, dataset_ids = REPO._ragflow_dataset_route("教学楼夏季白天闷热可能与哪些热环境和通风因素有关？")
        self.assertEqual(route, "mixed")
        self.assertEqual(dataset_ids, ["scene-a", "std-a"])

    def test_merge_ragflow_stream_text_supports_incremental_and_cumulative_chunks(self):
        full_text = ""
        full_text, delta = REPO._merge_ragflow_stream_text(full_text, "第一段")
        self.assertEqual(full_text, "第一段")
        self.assertEqual(delta, "第一段")

        full_text, delta = REPO._merge_ragflow_stream_text(full_text, "第二段")
        self.assertEqual(full_text, "第一段第二段")
        self.assertEqual(delta, "第二段")

        full_text, delta = REPO._merge_ragflow_stream_text(full_text, "第一段第二段第三段")
        self.assertEqual(full_text, "第一段第二段第三段")
        self.assertEqual(delta, "第三段")

    def test_ragflow_reference_limit_for_answer_expands_to_max_citation(self):
        self.assertEqual(REPO._ragflow_reference_limit_for_answer("说明见[ID:0][ID:7]。"), 8)
        self.assertEqual(REPO._ragflow_reference_limit_for_answer("无引用。"), 6)
        self.assertEqual(REPO._ragflow_reference_limit_for_answer("大量引用[ID:15]。"), 12)

    def test_ragflow_chat_stream_events_use_native_stream_and_preserve_full_answer(self):
        event_iter = iter(
            [
                {
                    "answer": "结论：[ID:3]先核查运行管理制度",
                    "reference": {"chunks": []},
                    "final": False,
                    "id": "msg-stream-1",
                    "session_id": "stream-session-1",
                },
                {
                    "answer": "结论：[ID:3]先核查运行管理制度，再结合监测数据复核设备状态。",
                    "reference": {"chunks": []},
                    "final": False,
                    "id": "msg-stream-1",
                    "session_id": "stream-session-1",
                },
                {
                    "answer": "",
                    "reference": {
                        "chunks": [
                            {
                                "id": "std-ref-1",
                                "dataset_id": "dataset-std-1",
                                "document_name": "GB 50365-2019 空调通风系统运行管理标准.md",
                                "content": "运行管理应结合设备状态和监测数据开展。",
                                "similarity": 0.8765,
                            }
                        ]
                    },
                    "final": True,
                    "id": "msg-stream-1",
                    "session_id": "stream-session-1",
                },
            ]
        )
        with mock.patch.object(REPO, "_answer_knowledge_question", side_effect=AssertionError("should not use DeepSeek rewrite path")):
            with mock.patch.object(
                REPO,
                "_ragflow_chat_stream_completion",
                return_value={"session_id": "stream-session-1", "event_iter": event_iter},
            ):
                with mock.patch.object(
                    REPO,
                    "_ragflow_settings",
                    return_value={"chat_id": "chat-native-1", "standard_dataset_ids": ["dataset-std-1"]},
                ):
                    events = list(REPO.ragflow_chat_stream_events({"question": "空调通风系统运行管理有哪些要求？"}))

        self.assertEqual([name for name, _ in events], ["start", "token", "token", "done"])
        self.assertEqual(events[1][1]["text"], "结论：[ID:3]先核查运行管理制度")
        self.assertEqual(events[2][1]["text"], "，再结合监测数据复核设备状态。")
        self.assertEqual(events[3][1]["answer"], "结论：[ID:3]先核查运行管理制度，再结合监测数据复核设备状态。")
        self.assertEqual(events[3][1]["message_id"], "msg-stream-1")
        self.assertEqual(events[3][1]["references"][0]["source_type"], "standard")
        self.assertEqual(events[3][1]["references"][0]["document_key"], "GB 50365-2019 空调通风系统运行管理标准")
        self.assertEqual(events[3][1]["references"][0]["snippet_text"], "运行管理应结合设备状态和监测数据开展。")

    def test_reference_document_lookup_resolves_main_kb_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            file_path = root / "07-车库与消防安全运行.md"
            file_path.write_text("# 车库与消防安全运行\n\n原文内容。\n", encoding="utf-8")
            with mock.patch.object(REPO, "_knowledge_document_root_for_source", return_value=root):
                result = REPO.ask_ragflow_reference_document(
                    {
                        "title": "07-车库与消防安全运行-part03",
                        "source_type": "ragflow",
                        "document_key": "07-车库与消防安全运行-part03",
                    }
                )

        self.assertEqual(result["title"], "07-车库与消防安全运行")
        self.assertEqual(result["document_key"], "07-车库与消防安全运行")
        self.assertEqual(result["format"], "markdown")
        self.assertIn("原文内容。", result["content"])

    def test_retrieve_ragflow_knowledge_uses_readable_title_and_similarity(self):
        fake_payload = {
            "code": 0,
            "data": {
                "chunks": [
                    {
                        "id": "chunk-1",
                        "dataset_id": "dataset-scene",
                        "document_id": "doc-1",
                        "document_keyword": "03-校园与教育建筑运维场景.md",
                        "similarity": 0.8123,
                        "content": "Question: 教学楼白天闷热怎么办？\nAnswer: <p>优先检查外窗开启、空调排程和新风阀。</p>",
                    }
                ]
            },
        }

        class FakeResponse:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def read(self):
                return json.dumps(fake_payload).encode("utf-8")

        with mock.patch("backend.server.urlopen", return_value=FakeResponse()):
            with mock.patch.object(
                REPO,
                "_ragflow_settings",
                return_value={
                    "configured": True,
                    "api_key": "test",
                    "base_url": "http://127.0.0.1:8088/api/v1",
                    "timeout_sec": 3.0,
                    "dataset_ids": ["dataset-scene"],
                    "scene_dataset_ids": ["dataset-scene"],
                    "standard_dataset_ids": [],
                    "top_k": 6,
                    "similarity_threshold": 0.2,
                    "vector_similarity_weight": 0.45,
                },
            ):
                result = REPO._retrieve_ragflow_knowledge("教学楼白天闷热怎么办？", limit=3, dataset_ids=["dataset-scene"])

        self.assertEqual(result["knowledge_source"], "ragflow")
        self.assertEqual(result["items"][0]["title"], "03-校园与教育建筑运维场景")
        self.assertEqual(result["items"][0]["similarity"], 0.8123)

    def test_ai_stats_shape(self):
        aid = self._first_anomaly_id()
        _ = REPO.diagnose({"message": "请分析异常", "anomaly_id": aid, "provider": "template"})
        stats = REPO.query_ai_stats(24)
        self.assertIn("total_calls", stats)
        self.assertIn("fallback_rate_pct", stats)
        self.assertIn("knowledge_sources", stats)
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
        with mock.patch.dict(
            "os.environ",
            {
                "RAGFLOW_BASE_URL": "http://127.0.0.1:8088/api/v1",
                "RAGFLOW_DATASET_IDS": "dataset-a,dataset-b",
                "RAGFLOW_STANDARD_DATASET_IDS": "dataset-std-1,dataset-std-2",
                "RAGFLOW_API_KEY": "rag-key",
                "RAGFLOW_CHAT_ID": "chat-1",
            },
            clear=False,
        ):
            health = REPO.query_system_health()
        self.assertIn("status", health)
        self.assertIn("recent_regression", health)
        self.assertIn("ragflow", health)
        self.assertTrue(health["ragflow"]["configured"])
        self.assertEqual(health["ragflow"]["dataset_count"], 2)
        self.assertEqual(health["ragflow"]["standard_dataset_count"], 2)
        self.assertTrue(health["ragflow"]["standard_configured"])
        self.assertTrue(health["ragflow"]["chat_ready"])
        self.assertEqual(health["ragflow"]["chat_id"], "chat-1")

    def test_ai_evaluate_and_feedback(self):
        aid = self._first_anomaly_id()
        diagnosed = REPO.diagnose({"message": "请分析异常", "anomaly_id": aid, "provider": "template"})
        trace_id = diagnosed["diagnosis"]["trace_id"]
        feedback = REPO.save_ai_feedback({"trace_id": trace_id, "label": "useful"})
        self.assertEqual(feedback["label"], "useful")
        evaluate = REPO.query_ai_evaluate(24)
        self.assertIn("template", evaluate)
        self.assertIn("feedback", evaluate)

    def test_mysql_repository_reports_storage_health(self):
        with tempfile.TemporaryDirectory() as tmp:
            action_file, ai_file, note_file, regression_file = self._empty_runtime_files(Path(tmp))
            repo = MySQLRepository(
                DEMO_DATA_FILE,
                NORMALIZED_DATA_FILE,
                METADATA_FILE,
                WEATHER_FILE,
                DICT_FILE,
                KNOWLEDGE_FILE,
                action_file,
                ai_file,
                note_file,
                regression_file,
                mysql_client=FakeMySQLClient(),
            )
            health = repo.query_system_health()
        self.assertEqual(health["storage"]["backend"], "mysql")
        self.assertTrue(health["storage"]["mysql"]["connected"])

    def test_mysql_repository_writes_action_events(self):
        fake = FakeMySQLClient(rows=self._mysql_seed_rows())
        with tempfile.TemporaryDirectory() as tmp:
            action_file, ai_file, note_file, regression_file = self._empty_runtime_files(Path(tmp))
            repo = MySQLRepository(
                DEMO_DATA_FILE,
                NORMALIZED_DATA_FILE,
                METADATA_FILE,
                WEATHER_FILE,
                DICT_FILE,
                KNOWLEDGE_FILE,
                action_file,
                ai_file,
                note_file,
                regression_file,
                mysql_client=fake,
            )
            anomalies = repo.query_anomalies(None, None, None, None, None, "new", 1, 20, "timestamp_desc")["items"]
            self.assertGreater(len(anomalies), 0)
            repo.apply_anomaly_action({"anomaly_id": anomalies[0]["anomaly_id"], "action": "ack", "assignee": "mysql", "note": "ok"})
        self.assertTrue(any("INSERT INTO anomaly_actions" in sql for sql in fake.executed))


class MySQLDriverIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.database = f"a8_test_codex_{uuid.uuid4().hex[:8]}"
        cls.client = MySQLClient(
            host="127.0.0.1",
            port=3306,
            database=cls.database,
            user="root",
            password="root",
        )
        cls.client.ensure_schema()

    @classmethod
    def tearDownClass(cls):
        admin = MySQLClient(
            host="127.0.0.1",
            port=3306,
            database="mysql",
            user="root",
            password="root",
        )
        admin.execute(f"DROP DATABASE IF EXISTS `{cls.database}`")

    def _empty_runtime_files(self, root: Path) -> tuple[Path, Path, Path, Path]:
        action_file = root / "anomaly_actions.jsonl"
        ai_file = root / "ai_calls.jsonl"
        note_file = root / "anomaly_notes.jsonl"
        regression_file = root / "regression_summary.json"
        action_file.write_text("", encoding="utf-8")
        ai_file.write_text("", encoding="utf-8")
        note_file.write_text("", encoding="utf-8")
        regression_file.write_text(json.dumps({"status": "unknown", "steps": []}, ensure_ascii=False), encoding="utf-8")
        return action_file, ai_file, note_file, regression_file

    def setUp(self):
        for table in [
            "anomaly_actions",
            "anomaly_notes",
            "ai_calls",
            "system_snapshots",
            "weather_timeseries",
            "energy_timeseries",
            "buildings",
        ]:
            self.client.execute(f"DELETE FROM {table}")

    def test_mysql_client_health_reports_connected(self):
        health = self.client.health()
        self.assertTrue(health["configured"])
        self.assertTrue(health["connected"])
        self.assertEqual(health["database"], self.database)

    def test_mysql_repository_roundtrip_for_storage_tables(self):
        now = "2026-03-28 10:00:00"
        self.client.execute(
            """
            INSERT INTO buildings (
                building_id, site_id, primaryspaceusage, sub_primaryspaceusage, peer_category,
                display_category, display_name, created_at, updated_at
            ) VALUES (
                'Panther_education_Genevieve', 'Panther', 'Education', 'Classroom',
                'teaching_building', '教学楼', 'Panther_education_Genevieve（教学楼）',
                %s, %s
            )
            """,
            params=(now, now),
        )
        self.client.execute(
            """
            INSERT INTO ai_calls (
                trace_id, event_time, requested_provider, provider, scene, building_id, anomaly_id,
                has_message, result_risk_level, knowledge_source, retrieval_hit_count, retrieval_error_type,
                fallback_used, field_complete, latency_ms, success, error_type, feedback_label, created_at, updated_at
            ) VALUES (
                'trace-test-1', %s, 'template', 'template_provider', 'diagnose', 'Panther_education_Genevieve',
                NULL, 1, 'medium', 'ragflow', 2, '', 0, 1, 320, 1, '', '', %s, %s
            )
            """,
            params=(now, now, now),
        )

        with tempfile.TemporaryDirectory() as tmp:
            action_file, ai_file, note_file, regression_file = self._empty_runtime_files(Path(tmp))
            repo = MySQLRepository(
                DEMO_DATA_FILE,
                NORMALIZED_DATA_FILE,
                METADATA_FILE,
                WEATHER_FILE,
                DICT_FILE,
                KNOWLEDGE_FILE,
                action_file,
                ai_file,
                note_file,
                regression_file,
                mysql_client=self.client,
            )
            repo._append_action_event(
                {
                    "anomaly_id": 9001,
                    "action": "ack",
                    "status_before": "new",
                    "status": "acknowledged",
                    "assignee": "mysql",
                    "note": "已确认",
                    "created_at": now,
                }
            )
            repo._append_note_event(
                {
                    "anomaly_id": 9001,
                    "cause_confirmed": "阈值验证",
                    "action_taken": "人工复核",
                    "result_summary": "确认有效",
                    "recurrence_risk": "low",
                    "reviewer": "qa",
                    "updated_at": now,
                }
            )
            feedback = repo.save_ai_feedback({"trace_id": "trace-test-1", "label": "useful"})
            health = repo.query_system_health()

        buildings = self.client.query_rows("SELECT building_id, display_name FROM buildings")
        actions = self.client.query_rows("SELECT anomaly_id, action_name, status_after FROM anomaly_actions")
        notes = self.client.query_rows("SELECT anomaly_id, recurrence_risk FROM anomaly_notes")
        ai_calls = self.client.query_rows("SELECT trace_id, feedback_label FROM ai_calls WHERE trace_id = 'trace-test-1'")

        self.assertEqual(buildings[0]["building_id"], "Panther_education_Genevieve")
        self.assertEqual(buildings[0]["display_name"], "Panther_education_Genevieve（教学楼）")
        self.assertEqual(actions[0]["action_name"], "ack")
        self.assertEqual(actions[0]["status_after"], "acknowledged")
        self.assertEqual(notes[0]["recurrence_risk"], "low")
        self.assertEqual(feedback["label"], "useful")
        self.assertEqual(ai_calls[0]["feedback_label"], "useful")
        self.assertEqual(health["storage"]["backend"], "mysql")
        self.assertTrue(health["storage"]["mysql"]["connected"])


if __name__ == "__main__":
    unittest.main()
