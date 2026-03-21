from __future__ import annotations

import csv
import datetime as dt
import io
import json
import math
import os
import re
import socket
import time
import uuid
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlparse
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = ROOT / ".env"
DEMO_DATA_FILE = ROOT / "data" / "energy_dataset.csv"
NORMALIZED_DATA_FILE = ROOT / "data" / "normalized" / "energy_normalized.csv"
METADATA_FILE = ROOT / "data" / "raw" / "bdg2" / "data" / "metadata" / "metadata.csv"
WEATHER_FILE = ROOT / "data" / "raw" / "bdg2" / "data" / "weather" / "weather.csv"
DICT_FILE = ROOT / "data" / "ai_dictionary.json"
KNOWLEDGE_FILE = ROOT / "data" / "normalized" / "knowledge_chunks.jsonl"
RUNTIME_DIR = ROOT / "data" / "runtime"
ACTION_LOG_FILE = RUNTIME_DIR / "anomaly_actions.jsonl"
AI_CALL_LOG_FILE = RUNTIME_DIR / "ai_calls.jsonl"
NOTE_LOG_FILE = RUNTIME_DIR / "anomaly_notes.jsonl"
REGRESSION_SUMMARY_FILE = RUNTIME_DIR / "regression_summary.json"
FRONTEND_DIR = ROOT / "frontend"

TIME_FMT = "%Y-%m-%d %H:%M:%S"
CARBON_FACTOR = 0.785
SEVERITY_SCORE = {"low": 1, "medium": 2, "high": 3}
SUPPORTED_ANALYSIS_METRICS = {"electricity"}

STATUS_NEW = "new"
STATUS_ACK = "acknowledged"
STATUS_IGNORED = "ignored"
STATUS_RESOLVED = "resolved"
STATUS_VALUES = {STATUS_NEW, STATUS_ACK, STATUS_IGNORED, STATUS_RESOLVED}

ACTION_TO_STATUS = {
    "ack": STATUS_ACK,
    "ignore": STATUS_IGNORED,
    "resolve": STATUS_RESOLVED,
}

ALLOWED_TRANSITIONS = {
    STATUS_NEW: {"ack", "ignore"},
    STATUS_ACK: {"resolve", "ignore"},
    STATUS_IGNORED: set(),
    STATUS_RESOLVED: set(),
}


def load_local_env() -> None:
    if not ENV_FILE.exists():
        return
    try:
        for raw_line in ENV_FILE.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip().lstrip("\ufeff")
            value = value.strip().strip("'\"")
            if key and key not in os.environ:
                os.environ[key] = value
    except OSError:
        return


load_local_env()


def parse_time(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    value = value.strip()
    for fmt in (TIME_FMT, "%Y-%m-%d"):
        try:
            return dt.datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def to_iso(ts: dt.datetime) -> str:
    return ts.strftime(TIME_FMT)


class DiagnoseProvider:
    name = "base_provider"

    def diagnose(self, repo: "EnergyRepository", payload: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError


class TemplateDiagnoseProvider(DiagnoseProvider):
    name = "template_provider"

    def diagnose(self, repo: "EnergyRepository", payload: dict[str, Any]) -> dict[str, Any]:
        return repo._diagnose_by_template(payload)


class LLMDiagnoseProvider(DiagnoseProvider):
    name = "llm_provider"

    def _extract_json_object(self, text: str) -> dict[str, Any]:
        raw = (text or "").strip()
        if not raw:
            raise ValueError("empty llm content")

        # 1) direct JSON parse
        try:
            obj = json.loads(raw)
            if isinstance(obj, dict):
                return obj
        except json.JSONDecodeError:
            pass

        # 2) fenced code block parse
        fence_match = re.search(r"```(?:json)?\s*([\s\S]*?)```", raw, re.IGNORECASE)
        if fence_match:
            candidate = fence_match.group(1).strip()
            try:
                obj = json.loads(candidate)
                if isinstance(obj, dict):
                    return obj
            except json.JSONDecodeError:
                pass

        # 3) first {...} parse
        start = raw.find("{")
        end = raw.rfind("}")
        if start >= 0 and end > start:
            candidate = raw[start : end + 1]
            try:
                obj = json.loads(candidate)
                if isinstance(obj, dict):
                    return obj
            except json.JSONDecodeError:
                pass

        raise ValueError("llm content is not valid json object")

    def _coerce_list_of_str(self, value: Any) -> list[str]:
        if isinstance(value, list):
            out = []
            for item in value:
                if item is None:
                    continue
                s = str(item).strip()
                if s:
                    out.append(s)
            return out
        if value is None:
            return []
        single = str(value).strip()
        return [single] if single else []

    def _sanitize_llm_result(self, llm_obj: dict[str, Any]) -> dict[str, Any]:
        confidence_raw = llm_obj.get("confidence", 0.6)
        try:
            confidence = float(confidence_raw)
        except (TypeError, ValueError):
            confidence = 0.6
        confidence = max(0.0, min(1.0, confidence))

        risk_level = str(llm_obj.get("risk_level", "")).strip().lower()
        if risk_level not in {"low", "medium", "high"}:
            risk_level = "high" if confidence >= 0.8 else "medium" if confidence >= 0.6 else "low"

        evidence_val = llm_obj.get("evidence", [])
        evidence: list[dict[str, str]] = []
        if isinstance(evidence_val, list):
            for idx, item in enumerate(evidence_val, start=1):
                if isinstance(item, dict):
                    evidence.append(
                        {
                            "chunk_id": str(item.get("chunk_id", f"llm-{idx}")),
                            "title": str(item.get("title", "LLM")),
                            "section": str(item.get("section", "")),
                            "excerpt": str(item.get("excerpt", "")).strip(),
                        }
                    )
                else:
                    txt = str(item).strip()
                    if txt:
                        evidence.append(
                            {
                                "chunk_id": f"llm-{idx}",
                                "title": "LLM",
                                "section": "",
                                "excerpt": txt,
                            }
                        )

        return {
            "conclusion": str(llm_obj.get("conclusion", "")).strip(),
            "causes": self._coerce_list_of_str(llm_obj.get("causes", llm_obj.get("possible_causes"))),
            "steps": self._coerce_list_of_str(llm_obj.get("steps")),
            "prevention": self._coerce_list_of_str(llm_obj.get("prevention")),
            "recommended_actions": self._coerce_list_of_str(llm_obj.get("recommended_actions")),
            "evidence": evidence,
            "confidence": round(confidence, 2),
            "risk_level": risk_level,
        }

    def _call_chat_completion(
        self,
        *,
        base_url: str,
        api_key: str,
        model: str,
        timeout_sec: float,
        messages: list[dict[str, str]],
    ) -> dict[str, Any]:
        endpoint = f"{base_url.rstrip('/')}/chat/completions"
        body = json.dumps(
            {
                "model": model,
                "messages": messages,
                "temperature": 0.2,
                "response_format": {"type": "json_object"},
            }
        ).encode("utf-8")
        req = Request(
            endpoint,
            data=body,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}",
            },
        )
        with urlopen(req, timeout=timeout_sec) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        return data

    def diagnose(self, repo: "EnergyRepository", payload: dict[str, Any]) -> dict[str, Any]:
        if payload.get("simulate_llm_failure"):
            raise RuntimeError("Simulated llm failure")

        api_key = os.getenv("OPENAI_API_KEY", "").strip()
        if not api_key:
            raise RuntimeError("LLM provider not configured")

        base_url = os.getenv("OPENAI_BASE_URL", "https://api.deepseek.com").strip() or "https://api.deepseek.com"
        model = os.getenv("OPENAI_MODEL", "deepseek-chat").strip() or "deepseek-chat"
        timeout_sec = float(os.getenv("OPENAI_TIMEOUT_SEC", "20"))
        max_retries = int(os.getenv("OPENAI_MAX_RETRIES", "2"))

        template_result = repo._diagnose_by_template(payload)
        diag_template = template_result.get("diagnosis", {})
        context = template_result.get("context", {})

        anomaly_name = str(diag_template.get("anomaly_name", ""))
        evidence = diag_template.get("evidence", [])
        evidence_text = "\n".join(
            [
                f"- {str(x.get('title', ''))}: {str(x.get('excerpt', ''))}"
                for x in evidence[:3]
                if isinstance(x, dict)
            ]
        ).strip()
        prompt_message = str(payload.get("message", "")).strip()
        if not prompt_message:
            prompt_message = "请基于异常上下文给出诊断建议。"

        system_prompt = (
            "你是建筑能源运维诊断助手，面向真实运维排障场景。必须只输出一个JSON对象，不要输出其他文本。"
            "JSON字段必须包含：conclusion, causes, steps, prevention, recommended_actions, evidence, confidence, risk_level。"
            "其中causes/steps/prevention/recommended_actions为字符串数组，evidence为数组。risk_level只能是low/medium/high。"
            "请使用中文，优先给出可执行的排查步骤和处理动作，不要泛泛而谈。"
        )
        user_prompt = (
            f"异常类型: {anomaly_name}\n"
            f"异常上下文: {json.dumps(context, ensure_ascii=False)}\n"
            f"模板诊断摘要: {diag_template.get('conclusion', '')}\n"
            f"知识证据:\n{evidence_text}\n"
            f"用户问题: {prompt_message}\n"
            "请结合异常发生时间、偏差比例、当前建筑和知识证据，优先输出可执行的排查顺序与风险判断。\n"
            "请输出严格JSON。"
        )

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]

        last_err: Exception | None = None
        for attempt in range(max_retries + 1):
            try:
                response = self._call_chat_completion(
                    base_url=base_url,
                    api_key=api_key,
                    model=model,
                    timeout_sec=timeout_sec,
                    messages=messages,
                )
                choices = response.get("choices", [])
                if not choices:
                    raise RuntimeError("llm response missing choices")
                content = choices[0].get("message", {}).get("content", "")
                if isinstance(content, list):
                    content = "".join(
                        str(part.get("text", "")) if isinstance(part, dict) else str(part)
                        for part in content
                    )
                llm_obj = self._extract_json_object(str(content))
                llm_diag = self._sanitize_llm_result(llm_obj)

                merged = dict(diag_template)
                for key in ("conclusion", "causes", "steps", "prevention", "recommended_actions", "evidence", "confidence", "risk_level"):
                    if llm_diag.get(key):
                        merged[key] = llm_diag[key]
                merged["possible_causes"] = merged.get("causes", [])
                return {"diagnosis": merged, "context": context}
            except HTTPError as exc:
                retryable = exc.code == 429 or 500 <= exc.code < 600
                last_err = RuntimeError(f"llm http status {exc.code}")
                if retryable and attempt < max_retries:
                    time.sleep(0.6 * (attempt + 1))
                    continue
                break
            except (URLError, TimeoutError, socket.timeout) as exc:
                last_err = RuntimeError(f"llm network error: {type(exc).__name__}")
                if attempt < max_retries:
                    time.sleep(0.6 * (attempt + 1))
                    continue
                break
            except Exception as exc:
                last_err = RuntimeError(f"llm parse error: {type(exc).__name__}")
                break

        raise RuntimeError(str(last_err or "llm unknown error"))


class EnergyRepository:
    def __init__(
        self,
        demo_data_file: Path,
        normalized_data_file: Path,
        metadata_file: Path,
        weather_file: Path,
        dict_file: Path,
        knowledge_file: Path,
        action_log_file: Path,
        ai_call_log_file: Path,
        note_log_file: Path,
        regression_summary_file: Path,
    ) -> None:
        self.demo_data_file = demo_data_file
        self.normalized_data_file = normalized_data_file
        self.metadata_file = metadata_file
        self.weather_file = weather_file
        self.dict_file = dict_file
        self.knowledge_file = knowledge_file
        self.action_log_file = action_log_file
        self.ai_call_log_file = ai_call_log_file
        self.note_log_file = note_log_file
        self.regression_summary_file = regression_summary_file

        self.rows = self._load_rows()
        self.building_site_map = self._load_building_site_map()
        self.weather_by_site = self._load_weather_by_site()
        self.by_building: dict[str, list[dict[str, Any]]] = {}
        self.stats: dict[str, dict[str, float]] = {}
        self.anomalies: list[dict[str, Any]] = []
        self.buildings_meta: dict[str, dict[str, Any]] = {}
        self.dict_data = self._load_dictionary()
        self.knowledge_chunks = self._load_knowledge_chunks()
        self.providers: dict[str, DiagnoseProvider] = {
            "template": TemplateDiagnoseProvider(),
            "llm": LLMDiagnoseProvider(),
        }

        self.action_events: list[dict[str, Any]] = []
        self.action_index: dict[int, dict[str, Any]] = {}
        self.ai_events: list[dict[str, Any]] = []
        self.note_events: list[dict[str, Any]] = []
        self.note_index: dict[int, dict[str, Any]] = {}

        self._prepare_indexes()
        self._ensure_runtime_storage()
        self._load_actions()
        self._load_ai_events()
        self._load_notes()

    def _ensure_runtime_storage(self) -> None:
        self.action_log_file.parent.mkdir(parents=True, exist_ok=True)
        if not self.action_log_file.exists():
            self.action_log_file.write_text("", encoding="utf-8")
        if not self.ai_call_log_file.exists():
            self.ai_call_log_file.write_text("", encoding="utf-8")
        if not self.note_log_file.exists():
            self.note_log_file.write_text("", encoding="utf-8")
        if not self.regression_summary_file.exists():
            self.regression_summary_file.write_text(
                json.dumps(
                    {
                        "updated_at": None,
                        "all_ok": False,
                        "status": "unknown",
                        "steps": [],
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

    def _load_rows(self) -> list[dict[str, Any]]:
        if self.normalized_data_file.exists():
            return self._load_rows_normalized(self.normalized_data_file)
        return self._load_rows_demo(self.demo_data_file)

    def _load_rows_normalized(self, file_path: Path) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        idx = 1
        with file_path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for raw in reader:
                ts = parse_time(raw.get("timestamp", ""))
                if not ts:
                    continue
                try:
                    kwh = float(raw.get("electricity_kwh", ""))
                except ValueError:
                    continue
                rows.append(
                    {
                        "record_id": idx,
                        "building_id": raw.get("building_id", "UNKNOWN"),
                        "building_name": raw.get("building_name", "UNKNOWN"),
                        "building_type": raw.get("building_type", "unknown"),
                        "timestamp": ts,
                        "hour": ts.hour,
                        "electricity_kwh": kwh,
                        "source": raw.get("source", "normalized"),
                    }
                )
                idx += 1
        return rows

    def _load_rows_demo(self, file_path: Path) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        with file_path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for raw in reader:
                ts = parse_time(raw.get("timestamp", ""))
                if not ts:
                    continue
                rows.append(
                    {
                        "record_id": int(raw.get("record_id", len(rows) + 1)),
                        "building_id": raw.get("building_id", "UNKNOWN"),
                        "building_name": raw.get("building_name", "UNKNOWN"),
                        "building_type": raw.get("building_type", "unknown"),
                        "timestamp": ts,
                        "hour": int(raw.get("hour", ts.hour)),
                        "electricity_kwh": float(raw.get("electricity_kwh", 0.0)),
                        "source": "demo",
                    }
                )
        return rows

    def _load_dictionary(self) -> dict[str, Any]:
        with self.dict_file.open("r", encoding="utf-8-sig") as f:
            return json.load(f).get("anomaly_type_dict", {})

    def _load_building_site_map(self) -> dict[str, str]:
        if not self.metadata_file.exists():
            return {}
        mapping: dict[str, str] = {}
        with self.metadata_file.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for raw in reader:
                building_id = str(raw.get("building_id", "")).strip()
                site_id = str(raw.get("site_id", "")).strip()
                if building_id and site_id:
                    mapping[building_id] = site_id
        return mapping

    def _load_weather_by_site(self) -> dict[str, dict[dt.datetime, dict[str, float]]]:
        if not self.weather_file.exists():
            return {}
        weather_by_site: dict[str, dict[dt.datetime, dict[str, float]]] = {}
        with self.weather_file.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for raw in reader:
                site_id = str(raw.get("site_id", "")).strip()
                timestamp = parse_time(raw.get("timestamp", ""))
                if not site_id or not timestamp:
                    continue
                try:
                    temperature = float(raw.get("airTemperature", ""))
                except (TypeError, ValueError):
                    continue
                try:
                    wind_speed = float(raw.get("windSpeed", ""))
                except (TypeError, ValueError):
                    wind_speed = 0.0
                weather_by_site.setdefault(site_id, {})[timestamp] = {
                    "temperature_c": round(temperature, 2),
                    "wind_speed": round(wind_speed, 2),
                }
        return weather_by_site

    def _load_knowledge_chunks(self) -> list[dict[str, Any]]:
        if not self.knowledge_file.exists():
            return []
        chunks: list[dict[str, Any]] = []
        with self.knowledge_file.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    chunks.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
        return chunks

    def _prepare_indexes(self) -> None:
        for row in self.rows:
            self.by_building.setdefault(row["building_id"], []).append(row)

        for building_id, items in self.by_building.items():
            items.sort(key=lambda r: r["timestamp"])
            values = [r["electricity_kwh"] for r in items]
            mean_val = sum(values) / len(values)
            variance = sum((v - mean_val) ** 2 for v in values) / len(values)
            std_val = math.sqrt(variance)
            self.stats[building_id] = {
                "mean": mean_val,
                "std": std_val,
                "spike_threshold": mean_val + 2 * std_val,
                "high_load_threshold": mean_val * 1.5,
            }
            self.buildings_meta[building_id] = {
                "building_id": building_id,
                "building_name": items[0]["building_name"],
                "building_type": items[0]["building_type"],
                "site_id": self.building_site_map.get(building_id),
                "record_count": len(items),
                "start_time": items[0]["timestamp"],
                "end_time": items[-1]["timestamp"],
            }

        self.anomalies = self._detect_anomalies()

    def _detect_anomalies(self) -> list[dict[str, Any]]:
        anomalies: list[dict[str, Any]] = []
        a_id = 1

        for building_id, items in self.by_building.items():
            stat = self.stats[building_id]
            spike_threshold = stat["spike_threshold"]
            high_threshold = stat["high_load_threshold"]

            for row in items:
                if row["electricity_kwh"] > spike_threshold:
                    anomalies.append(self._to_anomaly(a_id, row, "anomaly_spike", spike_threshold, stat["mean"]))
                    a_id += 1

            for row in items:
                if 8 <= row["hour"] <= 20 and row["electricity_kwh"] < 0.5:
                    anomalies.append(self._to_anomaly(a_id, row, "anomaly_offline", 0.5, stat["mean"]))
                    a_id += 1

            consec = 0
            for row in items:
                if row["electricity_kwh"] > high_threshold:
                    consec += 1
                else:
                    consec = 0
                if consec >= 4:
                    anomalies.append(
                        self._to_anomaly(a_id, row, "anomaly_sustained_high_load", high_threshold, stat["mean"])
                    )
                    a_id += 1

        anomalies.sort(key=lambda x: x["timestamp"], reverse=True)
        return anomalies

    def _to_anomaly(
        self,
        anomaly_id: int,
        row: dict[str, Any],
        anomaly_type: str,
        threshold: float,
        stat_mean: float,
    ) -> dict[str, Any]:
        diff_pct = ((row["electricity_kwh"] - stat_mean) / stat_mean * 100) if stat_mean else 0.0
        severity = "high" if abs(diff_pct) > 60 else "medium" if abs(diff_pct) > 30 else "low"
        return {
            "anomaly_id": anomaly_id,
            "record_id": row["record_id"],
            "building_id": row["building_id"],
            "building_name": row["building_name"],
            "building_type": row["building_type"],
            "timestamp": row["timestamp"],
            "electricity_kwh": round(row["electricity_kwh"], 4),
            "mean_kwh": round(stat_mean, 4),
            "deviation_pct": round(diff_pct, 2),
            "threshold": round(threshold, 4),
            "anomaly_type": anomaly_type,
            "severity": severity,
        }

    def _load_actions(self) -> None:
        self.action_events = []
        with self.action_log_file.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not isinstance(event, dict):
                    continue
                try:
                    event["anomaly_id"] = int(event.get("anomaly_id"))
                except (ValueError, TypeError):
                    continue
                event["created_at"] = str(event.get("created_at", ""))
                self.action_events.append(event)
        self._rebuild_action_index()

    def _rebuild_action_index(self) -> None:
        self.action_index = {}
        for e in sorted(self.action_events, key=lambda x: x.get("created_at", "")):
            aid = int(e["anomaly_id"])
            bucket = self.action_index.setdefault(
                aid,
                {
                    "status": STATUS_NEW,
                    "assignee": "",
                    "note": "",
                    "last_action_at": "",
                    "history": [],
                },
            )
            bucket["status"] = str(e.get("status", STATUS_NEW))
            bucket["assignee"] = str(e.get("assignee", ""))
            bucket["note"] = str(e.get("note", ""))
            bucket["last_action_at"] = str(e.get("created_at", ""))
            bucket["history"].append(
                {
                    "action": str(e.get("action", "")),
                    "status": str(e.get("status", STATUS_NEW)),
                    "assignee": str(e.get("assignee", "")),
                    "note": str(e.get("note", "")),
                    "created_at": str(e.get("created_at", "")),
                }
            )

    def _append_action_event(self, event: dict[str, Any]) -> None:
        with self.action_log_file.open("a", encoding="utf-8") as f:
            f.write(json.dumps(event, ensure_ascii=False) + "\n")
        self.action_events.append(event)
        self._rebuild_action_index()

    def _load_notes(self) -> None:
        self.note_events = []
        with self.note_log_file.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not isinstance(event, dict):
                    continue
                try:
                    event["anomaly_id"] = int(event.get("anomaly_id"))
                except (ValueError, TypeError):
                    continue
                event["updated_at"] = str(event.get("updated_at", ""))
                self.note_events.append(event)
        self._rebuild_note_index()

    def _rebuild_note_index(self) -> None:
        self.note_index = {}
        for e in sorted(self.note_events, key=lambda x: x.get("updated_at", "")):
            self.note_index[int(e["anomaly_id"])] = {
                "anomaly_id": int(e["anomaly_id"]),
                "cause_confirmed": str(e.get("cause_confirmed", "")),
                "action_taken": str(e.get("action_taken", "")),
                "result_summary": str(e.get("result_summary", "")),
                "recurrence_risk": str(e.get("recurrence_risk", "")),
                "reviewer": str(e.get("reviewer", "")),
                "updated_at": str(e.get("updated_at", "")),
            }

    def _append_note_event(self, event: dict[str, Any]) -> None:
        with self.note_log_file.open("a", encoding="utf-8") as f:
            f.write(json.dumps(event, ensure_ascii=False) + "\n")
        self.note_events.append(event)
        self._rebuild_note_index()

    def _load_ai_events(self) -> None:
        self.ai_events = []
        with self.ai_call_log_file.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(event, dict):
                    self.ai_events.append(event)

    def _append_ai_event(self, event: dict[str, Any]) -> None:
        with self.ai_call_log_file.open("a", encoding="utf-8") as f:
            f.write(json.dumps(event, ensure_ascii=False) + "\n")
        self.ai_events.append(event)

    def query_ai_stats(self, hours: int = 24) -> dict[str, Any]:
        safe_hours = min(max(hours, 1), 168)
        now = dt.datetime.now()
        start = now - dt.timedelta(hours=safe_hours)
        window_events = []
        for ev in self.ai_events:
            ts = parse_time(str(ev.get("timestamp", "")))
            if ts and ts >= start:
                window_events.append(ev)

        total = len(window_events)
        llm_calls = sum(1 for ev in window_events if str(ev.get("requested_provider", "")).lower() in {"llm", "auto"})
        fallback_calls = sum(1 for ev in window_events if bool(ev.get("fallback_used", False)))
        latencies = [int(ev.get("latency_ms", 0)) for ev in window_events if str(ev.get("latency_ms", "")).isdigit()]
        avg_latency = round(sum(latencies) / len(latencies), 2) if latencies else 0.0

        by_provider: dict[str, int] = {}
        error_types: dict[str, int] = {}
        for ev in window_events:
            provider = str(ev.get("provider", "unknown"))
            by_provider[provider] = by_provider.get(provider, 0) + 1
            err = str(ev.get("error_type", "")).strip()
            if err:
                error_types[err] = error_types.get(err, 0) + 1

        fallback_rate = round((fallback_calls / total) * 100, 2) if total else 0.0
        return {
            "window_hours": safe_hours,
            "total_calls": total,
            "llm_calls": llm_calls,
            "fallback_calls": fallback_calls,
            "fallback_rate_pct": fallback_rate,
            "avg_latency_ms": avg_latency,
            "by_provider": by_provider,
            "error_types": error_types,
            "updated_at": to_iso(now),
        }

    def _required_diag_fields_complete(self, diagnosis: dict[str, Any]) -> bool:
        required = ["conclusion", "causes", "steps", "prevention", "evidence", "confidence", "risk_level"]
        for key in required:
            if key not in diagnosis:
                return False
            value = diagnosis.get(key)
            if key in {"causes", "steps", "prevention", "evidence"}:
                if not isinstance(value, list):
                    return False
            elif value in (None, ""):
                return False
        return True

    def _required_analysis_fields_complete(self, analysis: dict[str, Any]) -> bool:
        required = [
            "summary",
            "findings",
            "possible_causes",
            "energy_saving_suggestions",
            "operations_suggestions",
            "evidence",
        ]
        for key in required:
            if key not in analysis:
                return False
            value = analysis.get(key)
            if key == "summary":
                if value in (None, ""):
                    return False
            elif not isinstance(value, list):
                return False
        return True

    def upsert_anomaly_note(self, payload: dict[str, Any]) -> dict[str, Any]:
        anomaly_id_raw = payload.get("anomaly_id")
        if anomaly_id_raw is None:
            raise ValueError("anomaly_id required")
        try:
            anomaly_id = int(anomaly_id_raw)
        except (TypeError, ValueError):
            raise ValueError("invalid anomaly_id")

        if not any(a["anomaly_id"] == anomaly_id for a in self.anomalies):
            raise LookupError("anomaly not found")

        cause_confirmed = str(payload.get("cause_confirmed", "")).strip()
        action_taken = str(payload.get("action_taken", "")).strip()
        result_summary = str(payload.get("result_summary", "")).strip()
        recurrence_risk = str(payload.get("recurrence_risk", "")).strip().lower()
        reviewer = str(payload.get("reviewer", "")).strip()

        if not cause_confirmed or not action_taken or not result_summary:
            raise ValueError("cause_confirmed/action_taken/result_summary required")
        if recurrence_risk not in {"low", "medium", "high"}:
            recurrence_risk = "medium"

        event = {
            "anomaly_id": anomaly_id,
            "cause_confirmed": cause_confirmed,
            "action_taken": action_taken,
            "result_summary": result_summary,
            "recurrence_risk": recurrence_risk,
            "reviewer": reviewer,
            "updated_at": to_iso(dt.datetime.now()),
        }
        self._append_note_event(event)
        return dict(self.note_index.get(anomaly_id, event))

    def query_anomaly_note(self, anomaly_id: int) -> dict[str, Any]:
        note = self.note_index.get(anomaly_id)
        if note:
            return dict(note)
        return {
            "anomaly_id": anomaly_id,
            "cause_confirmed": "",
            "action_taken": "",
            "result_summary": "",
            "recurrence_risk": "medium",
            "reviewer": "",
            "updated_at": "",
        }

    def _compute_processing_duration_hours(self, anomaly_id: int) -> float:
        state = self.action_index.get(anomaly_id)
        if not state or not state.get("history"):
            return 0.0
        history = state["history"]
        first_ts = parse_time(str(history[0].get("created_at", "")))
        resolved_ts = None
        for h in history:
            if str(h.get("status")) == STATUS_RESOLVED:
                resolved_ts = parse_time(str(h.get("created_at", "")))
                break
        end_ts = resolved_ts or parse_time(str(history[-1].get("created_at", "")))
        if not first_ts or not end_ts:
            return 0.0
        return round(max((end_ts - first_ts).total_seconds(), 0.0) / 3600.0, 2)

    def export_anomalies_csv(
        self,
        building_id: str | None,
        start_time: dt.datetime | None,
        end_time: dt.datetime | None,
        anomaly_type: str | None,
        severity: str | None,
        status: str | None,
        sort: str = "timestamp_desc",
    ) -> str:
        rows = self._sort_anomalies(self._filter_anomalies(building_id, start_time, end_time, anomaly_type, severity, status), sort)
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(
            [
                "anomaly_id",
                "building_id",
                "building_name",
                "anomaly_type",
                "severity",
                "status",
                "assignee",
                "timestamp",
                "deviation_pct",
                "estimated_loss_kwh",
                "processing_duration_hours",
                "final_conclusion",
            ]
        )
        for item in rows:
            aid = int(item["anomaly_id"])
            action_state = self._action_state(aid)
            estimated_loss = round(max(float(item.get("electricity_kwh", 0)) - float(item.get("mean_kwh", 0)), 0.0), 4)
            note = self.note_index.get(aid, {})
            conclusion = str(note.get("result_summary", "")).strip() or str(item.get("anomaly_type", ""))
            writer.writerow(
                [
                    aid,
                    item["building_id"],
                    item["building_name"],
                    item["anomaly_type"],
                    item["severity"],
                    action_state["status"],
                    action_state["assignee"],
                    to_iso(item["timestamp"]),
                    item["deviation_pct"],
                    estimated_loss,
                    self._compute_processing_duration_hours(aid),
                    conclusion,
                ]
            )
        return output.getvalue()

    def query_ai_evaluate(self, hours: int = 24) -> dict[str, Any]:
        safe_hours = min(max(hours, 1), 168)
        now = dt.datetime.now()
        start = now - dt.timedelta(hours=safe_hours)
        events = []
        for ev in self.ai_events:
            ts = parse_time(str(ev.get("timestamp", "")))
            if ts and ts >= start:
                events.append(ev)

        def provider_metrics(requested: str) -> dict[str, Any]:
            subset = [ev for ev in events if str(ev.get("requested_provider", "")).lower() == requested]
            total = len(subset)
            if total == 0:
                return {"total": 0, "success_rate_pct": 0.0, "avg_latency_ms": 0.0, "fallback_rate_pct": 0.0, "field_completeness_pct": 0.0}
            success = sum(1 for ev in subset if not str(ev.get("error_type", "")).strip())
            fallback = sum(1 for ev in subset if bool(ev.get("fallback_used", False)))
            complete = sum(1 for ev in subset if bool(ev.get("field_complete", False)))
            latencies = [int(ev.get("latency_ms", 0)) for ev in subset if str(ev.get("latency_ms", "")).isdigit()]
            avg_latency = round(sum(latencies) / len(latencies), 2) if latencies else 0.0
            return {
                "total": total,
                "success_rate_pct": round((success / total) * 100, 2),
                "avg_latency_ms": avg_latency,
                "fallback_rate_pct": round((fallback / total) * 100, 2),
                "field_completeness_pct": round((complete / total) * 100, 2),
            }

        template_metrics = provider_metrics("template")
        llm_metrics = provider_metrics("llm")
        auto_metrics = provider_metrics("auto")
        feedback_subset = [ev for ev in events if str(ev.get("feedback_label", "")).strip()]
        useful = sum(1 for ev in feedback_subset if str(ev.get("feedback_label")) == "useful")

        return {
            "window_hours": safe_hours,
            "template": template_metrics,
            "llm": llm_metrics,
            "auto": auto_metrics,
            "feedback": {
                "total_labeled": len(feedback_subset),
                "useful_rate_pct": round((useful / len(feedback_subset)) * 100, 2) if feedback_subset else 0.0,
            },
            "updated_at": to_iso(now),
        }

    def save_ai_feedback(self, payload: dict[str, Any]) -> dict[str, Any]:
        trace_id = str(payload.get("trace_id", "")).strip()
        label = str(payload.get("label", "")).strip().lower()
        if not trace_id:
            raise ValueError("trace_id required")
        if label not in {"useful", "not_useful"}:
            raise ValueError("label must be useful/not_useful")
        for ev in reversed(self.ai_events):
            if str(ev.get("trace_id", "")) == trace_id:
                ev["feedback_label"] = label
                with self.ai_call_log_file.open("w", encoding="utf-8") as f:
                    for row in self.ai_events:
                        f.write(json.dumps(row, ensure_ascii=False) + "\n")
                return {"trace_id": trace_id, "label": label}
        raise LookupError("trace_id not found")

    def query_system_health(self) -> dict[str, Any]:
        regression = {
            "status": "unknown",
            "updated_at": None,
            "all_ok": False,
            "steps": [],
        }
        if self.regression_summary_file.exists():
            try:
                regression = json.loads(self.regression_summary_file.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                pass

        data_source = {
            "normalized_energy": self.normalized_data_file.exists(),
            "dictionary": self.dict_file.exists(),
            "knowledge": self.knowledge_file.exists(),
        }
        ai_status = {
            "configured": bool(os.getenv("OPENAI_API_KEY", "").strip()),
            "base_url": bool(os.getenv("OPENAI_BASE_URL", "").strip()),
            "model": bool(os.getenv("OPENAI_MODEL", "").strip()),
        }
        return {
            "status": "ok" if all(data_source.values()) else "degraded",
            "data_source": data_source,
            "ai_provider": ai_status,
            "recent_regression": regression,
            "updated_at": to_iso(dt.datetime.now()),
        }

    def _action_state(self, anomaly_id: int) -> dict[str, Any]:
        state = self.action_index.get(anomaly_id)
        if not state:
            return {
                "status": STATUS_NEW,
                "assignee": "",
                "last_note": "",
                "last_action_at": "",
                "history_count": 0,
            }
        return {
            "status": state["status"],
            "assignee": state["assignee"],
            "last_note": state["note"],
            "last_action_at": state["last_action_at"],
            "history_count": len(state["history"]),
        }

    def query_anomaly_history(self, anomaly_id: int) -> dict[str, Any] | None:
        if not any(a["anomaly_id"] == anomaly_id for a in self.anomalies):
            return None
        state = self.action_index.get(anomaly_id)
        history = []
        if state:
            history = list(reversed(state["history"]))
        return {"anomaly_id": anomaly_id, "count": len(history), "items": history}

    def apply_anomaly_action(self, payload: dict[str, Any]) -> dict[str, Any]:
        anomaly_id_raw = payload.get("anomaly_id")
        action = str(payload.get("action", "")).strip().lower()
        assignee = str(payload.get("assignee", "")).strip()
        note = str(payload.get("note", "")).strip()

        if anomaly_id_raw is None:
            raise ValueError("anomaly_id required")
        try:
            anomaly_id = int(anomaly_id_raw)
        except (ValueError, TypeError):
            raise ValueError("invalid anomaly_id")

        if not any(a["anomaly_id"] == anomaly_id for a in self.anomalies):
            raise LookupError("anomaly not found")

        if action not in ACTION_TO_STATUS:
            raise ValueError("invalid action")

        current_status = self._action_state(anomaly_id)["status"]
        if action not in ALLOWED_TRANSITIONS.get(current_status, set()):
            raise ValueError(f"invalid transition: {current_status} -> {action}")

        target_status = ACTION_TO_STATUS[action]
        now = to_iso(dt.datetime.now())

        event = {
            "anomaly_id": anomaly_id,
            "action": action,
            "status": target_status,
            "assignee": assignee,
            "note": note,
            "created_at": now,
        }
        self._append_action_event(event)

        state = self._action_state(anomaly_id)
        return {
            "anomaly_id": anomaly_id,
            "status": state["status"],
            "assignee": state["assignee"],
            "last_note": state["last_note"],
            "last_action_at": state["last_action_at"],
            "history_count": state["history_count"],
        }

    def _filter_rows(self, building_id: str | None, start_time: dt.datetime | None, end_time: dt.datetime | None) -> list[dict[str, Any]]:
        rows = list(self.by_building.get(building_id, [])) if building_id and building_id in self.by_building else list(self.rows)
        if start_time:
            rows = [r for r in rows if r["timestamp"] >= start_time]
        if end_time:
            rows = [r for r in rows if r["timestamp"] <= end_time]
        return rows

    def _require_metric_type(self, metric_type: str | None) -> str:
        chosen = str(metric_type or "electricity").strip().lower() or "electricity"
        if chosen not in SUPPORTED_ANALYSIS_METRICS:
            raise ValueError("暂未接入该分析类型")
        return chosen

    def _summarize_values(self, values: list[float]) -> dict[str, float]:
        if not values:
            return {
                "total": 0.0,
                "avg": 0.0,
                "peak": 0.0,
                "min": 0.0,
                "std": 0.0,
                "volatility_pct": 0.0,
            }
        total = sum(values)
        avg = total / len(values)
        variance = sum((v - avg) ** 2 for v in values) / len(values)
        std = math.sqrt(variance)
        return {
            "total": round(total, 4),
            "avg": round(avg, 4),
            "peak": round(max(values), 4),
            "min": round(min(values), 4),
            "std": round(std, 4),
            "volatility_pct": round((std / avg * 100) if avg else 0.0, 2),
        }

    def _series_with_weather(self, rows: list[dict[str, Any]], building_id: str | None) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        ordered = sorted(rows, key=lambda x: x["timestamp"])
        if building_id:
            site_id = self.building_site_map.get(building_id)
        else:
            site_id = None

        weather_lookup = self.weather_by_site.get(site_id or "", {})
        series: list[dict[str, Any]] = []
        weather_series: list[dict[str, Any]] = []

        if not building_id:
            buckets: dict[dt.datetime, float] = {}
            for row in ordered:
                buckets.setdefault(row["timestamp"], 0.0)
                buckets[row["timestamp"]] += row["electricity_kwh"]
            for timestamp, value in sorted(buckets.items(), key=lambda x: x[0]):
                weather = weather_lookup.get(timestamp, {})
                point = {
                    "timestamp": to_iso(timestamp),
                    "value": round(value, 4),
                    "temperature_c": weather.get("temperature_c"),
                }
                series.append(point)
                if weather.get("temperature_c") is not None:
                    weather_series.append({"timestamp": to_iso(timestamp), "value": weather["temperature_c"]})
            return series, weather_series

        for row in ordered:
            weather = weather_lookup.get(row["timestamp"], {})
            point = {
                "timestamp": to_iso(row["timestamp"]),
                "value": round(row["electricity_kwh"], 4),
                "temperature_c": weather.get("temperature_c"),
            }
            series.append(point)
            if weather.get("temperature_c") is not None:
                weather_series.append({"timestamp": to_iso(row["timestamp"]), "value": weather["temperature_c"]})
        return series, weather_series

    def _temperature_correlation(self, series: list[dict[str, Any]]) -> float:
        pairs = [(float(item["value"]), float(item["temperature_c"])) for item in series if item.get("temperature_c") is not None]
        if len(pairs) < 2:
            return 0.0
        xs = [x for x, _ in pairs]
        ys = [y for _, y in pairs]
        x_avg = sum(xs) / len(xs)
        y_avg = sum(ys) / len(ys)
        numerator = sum((x - x_avg) * (y - y_avg) for x, y in pairs)
        denominator = math.sqrt(sum((x - x_avg) ** 2 for x in xs) * sum((y - y_avg) ** 2 for y in ys))
        if not denominator:
            return 0.0
        return round(numerator / denominator, 4)

    def _granularity_label(self, rows: list[dict[str, Any]]) -> str:
        ordered = sorted(rows, key=lambda item: item["timestamp"])
        if len(ordered) < 2:
            return "single-point"
        deltas = [
            max(1.0, round((ordered[index + 1]["timestamp"] - ordered[index]["timestamp"]).total_seconds() / 3600, 2))
            for index in range(min(len(ordered) - 1, 48))
        ]
        avg_delta = sum(deltas) / len(deltas)
        if avg_delta <= 1.2:
            return "hourly"
        if avg_delta <= 6.5:
            return "6-hour"
        if avg_delta <= 12.5:
            return "12-hour"
        return "daily"

    def _comparison_series(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        ordered = sorted(rows, key=lambda item: item["timestamp"])
        if not ordered:
            return []
        by_hour: dict[int, list[float]] = {hour: [] for hour in range(24)}
        for row in ordered:
            by_hour[int(row["hour"])].append(float(row["electricity_kwh"]))
        baseline_by_hour = {
            hour: (sum(values) / len(values)) if values else 0.0
            for hour, values in by_hour.items()
        }
        return [
            {
                "timestamp": to_iso(row["timestamp"]),
                "value": round(baseline_by_hour[int(row["hour"])], 4),
            }
            for row in ordered
        ]

    def _weather_relation_stats(self, series: list[dict[str, Any]]) -> dict[str, float]:
        pairs = [
            (float(item["temperature_c"]), float(item["value"]))
            for item in series
            if item.get("temperature_c") is not None
        ]
        if len(pairs) < 6:
            return {
                "hot_avg_load": 0.0,
                "cold_avg_load": 0.0,
                "hot_avg_temp": 0.0,
                "cold_avg_temp": 0.0,
                "hot_cold_gap_pct": 0.0,
            }

        sorted_pairs = sorted(pairs, key=lambda item: item[0])
        bucket_size = max(2, len(sorted_pairs) // 3)
        cold_bucket = sorted_pairs[:bucket_size]
        hot_bucket = sorted_pairs[-bucket_size:]
        cold_avg_load = sum(item[1] for item in cold_bucket) / len(cold_bucket)
        hot_avg_load = sum(item[1] for item in hot_bucket) / len(hot_bucket)
        cold_avg_temp = sum(item[0] for item in cold_bucket) / len(cold_bucket)
        hot_avg_temp = sum(item[0] for item in hot_bucket) / len(hot_bucket)
        gap_pct = ((hot_avg_load - cold_avg_load) / cold_avg_load * 100) if cold_avg_load else 0.0
        return {
            "hot_avg_load": round(hot_avg_load, 4),
            "cold_avg_load": round(cold_avg_load, 4),
            "hot_avg_temp": round(hot_avg_temp, 2),
            "cold_avg_temp": round(cold_avg_temp, 2),
            "hot_cold_gap_pct": round(gap_pct, 2),
        }

    def _trend_markers(self, anomalies: list[dict[str, Any]]) -> list[dict[str, Any]]:
        ranked = sorted(
            anomalies,
            key=lambda item: (SEVERITY_SCORE.get(str(item.get("severity")), 0), float(item.get("deviation_pct", 0.0))),
            reverse=True,
        )[:12]
        ranked.sort(key=lambda item: item["timestamp"])
        return [
            {
                "anomaly_id": int(item["anomaly_id"]),
                "timestamp": to_iso(item["timestamp"]),
                "anomaly_name": self.dict_data.get(str(item.get("anomaly_type", "")), {}).get("name", item.get("anomaly_type", "异常")),
                "severity": item["severity"],
                "deviation_pct": round(float(item["deviation_pct"]), 2),
                "value": round(float(item["electricity_kwh"]), 4),
                "estimated_loss_kwh": round(max(float(item["electricity_kwh"]) - float(item["mean_kwh"]), 0.0), 4),
            }
            for item in ranked
        ]

    def _insight_item(self, title: str, detail: str, severity: str = "info") -> dict[str, str]:
        return {"title": title, "detail": detail, "severity": severity}

    def _opportunity_item(self, title: str, detail: str, priority: str, estimated_kwh: float = 0.0) -> dict[str, Any]:
        return {
            "title": title,
            "detail": detail,
            "priority": priority,
            "estimated_kwh": round(float(estimated_kwh), 4),
        }

    def _analysis_target_building(self, building_id: str | None, rows: list[dict[str, Any]]) -> tuple[str | None, list[dict[str, Any]]]:
        if building_id and building_id in self.by_building:
            return building_id, self._filter_rows(building_id, None, None)
        if rows:
            fallback_id = rows[0]["building_id"]
            return fallback_id, list(self.by_building.get(fallback_id, []))
        if self.buildings_meta:
            fallback_id = sorted(self.buildings_meta.keys())[0]
            return fallback_id, list(self.by_building.get(fallback_id, []))
        return None, []

    def _sort_anomalies(self, rows: list[dict[str, Any]], sort: str) -> list[dict[str, Any]]:
        if sort == "timestamp_asc":
            return sorted(rows, key=lambda x: x["timestamp"])
        if sort == "deviation_desc":
            return sorted(rows, key=lambda x: abs(float(x.get("deviation_pct", 0))), reverse=True)
        if sort == "severity_desc":
            return sorted(rows, key=lambda x: (SEVERITY_SCORE.get(x.get("severity", "low"), 0), x["timestamp"]), reverse=True)
        return sorted(rows, key=lambda x: x["timestamp"], reverse=True)

    def _filter_anomalies(
        self,
        building_id: str | None,
        start_time: dt.datetime | None,
        end_time: dt.datetime | None,
        anomaly_type: str | None = None,
        severity: str | None = None,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        result = list(self.anomalies)
        if building_id:
            result = [x for x in result if x["building_id"] == building_id]
        if start_time:
            result = [x for x in result if x["timestamp"] >= start_time]
        if end_time:
            result = [x for x in result if x["timestamp"] <= end_time]
        if anomaly_type:
            result = [x for x in result if x["anomaly_type"] == anomaly_type]
        if severity:
            result = [x for x in result if x["severity"] == severity]
        if status:
            result = [x for x in result if self._action_state(int(x["anomaly_id"]))["status"] == status]
        return result

    def query_buildings(self) -> dict[str, Any]:
        items = []
        for _, meta in sorted(self.buildings_meta.items(), key=lambda x: x[0]):
            items.append(
                {
                    "building_id": meta["building_id"],
                    "building_name": meta["building_name"],
                    "building_type": meta["building_type"],
                    "record_count": meta["record_count"],
                    "start_time": to_iso(meta["start_time"]),
                    "end_time": to_iso(meta["end_time"]),
                }
            )
        global_start = min((m["start_time"] for m in self.buildings_meta.values()), default=None)
        global_end = max((m["end_time"] for m in self.buildings_meta.values()), default=None)
        return {
            "count": len(items),
            "items": items,
            "global_range": {
                "start_time": to_iso(global_start) if global_start else None,
                "end_time": to_iso(global_end) if global_end else None,
            },
        }

    def query_analysis_summary(
        self,
        building_id: str | None,
        start_time: dt.datetime | None,
        end_time: dt.datetime | None,
        metric_type: str | None = "electricity",
    ) -> dict[str, Any]:
        metric = self._require_metric_type(metric_type)
        rows = self._filter_rows(building_id, start_time, end_time)
        values = [r["electricity_kwh"] for r in rows]
        summary = self._summarize_values(values)
        anomalies = self._filter_anomalies(building_id, start_time, end_time)
        working_hours = [r["electricity_kwh"] for r in rows if 8 <= r["hour"] <= 20]
        off_hours = [r["electricity_kwh"] for r in rows if r["hour"] < 8 or r["hour"] > 20]

        return {
            "metric_type": metric,
            "metric_label": "电力",
            "unit": "kWh",
            "building_id": building_id or "ALL",
            "point_count": len(rows),
            "total_value": summary["total"],
            "avg_value": summary["avg"],
            "peak_value": summary["peak"],
            "min_value": summary["min"],
            "std_value": summary["std"],
            "volatility_pct": summary["volatility_pct"],
            "anomaly_count": len(anomalies),
            "working_hour_avg": round(sum(working_hours) / len(working_hours), 4) if working_hours else 0.0,
            "off_hour_avg": round(sum(off_hours) / len(off_hours), 4) if off_hours else 0.0,
            "supported_metric_types": sorted(SUPPORTED_ANALYSIS_METRICS),
            "method_note": "normalized electricity + anomaly context + weather ready",
        }

    def query_analysis_trend(
        self,
        building_id: str | None,
        start_time: dt.datetime | None,
        end_time: dt.datetime | None,
        metric_type: str | None = "electricity",
    ) -> dict[str, Any]:
        metric = self._require_metric_type(metric_type)
        rows = self._filter_rows(building_id, start_time, end_time)
        series, weather_series = self._series_with_weather(rows, building_id)
        anomalies = self._filter_anomalies(building_id, start_time, end_time)
        values = [item["value"] for item in series]
        summary = self._summarize_values(values)

        if len(values) >= 48:
            head = values[:24]
            tail = values[-24:]
            head_avg = sum(head) / len(head)
            tail_avg = sum(tail) / len(tail)
            change_pct = round(((tail_avg - head_avg) / head_avg * 100) if head_avg else 0.0, 2)
        else:
            change_pct = 0.0

        return {
            "metric_type": metric,
            "metric_label": "电力",
            "unit": "kWh",
            "building_id": building_id or "ALL",
            "series": series,
            "comparison_series": self._comparison_series(rows),
            "weather_series": weather_series,
            "markers": self._trend_markers(anomalies),
            "overlay_available": bool(weather_series),
            "summary": {
                "total_value": summary["total"],
                "avg_value": summary["avg"],
                "peak_value": summary["peak"],
                "volatility_pct": summary["volatility_pct"],
                "window_change_pct": change_pct,
                "temperature_correlation": self._temperature_correlation(series),
            },
        }

    def query_analysis_distribution(
        self,
        building_id: str | None,
        start_time: dt.datetime | None,
        end_time: dt.datetime | None,
        metric_type: str | None = "electricity",
    ) -> dict[str, Any]:
        metric = self._require_metric_type(metric_type)
        rows = self._filter_rows(building_id, start_time, end_time)
        hourly: dict[int, list[float]] = {hour: [] for hour in range(24)}
        weekday_hourly: dict[int, list[float]] = {hour: [] for hour in range(24)}
        weekday_total = 0.0
        weekend_total = 0.0
        day_total = 0.0
        night_total = 0.0
        night_base_values: list[float] = []

        for row in rows:
            value = float(row["electricity_kwh"])
            hourly[row["hour"]].append(value)
            if row["timestamp"].weekday() < 5:
                weekday_total += value
                weekday_hourly[row["hour"]].append(value)
            else:
                weekend_total += value
            if 8 <= row["hour"] <= 20:
                day_total += value
            else:
                night_total += value
            if row["hour"] < 6 or row["hour"] >= 22:
                night_base_values.append(value)

        hourly_profile = [
            {
                "hour": hour,
                "label": f"{hour:02d}:00",
                "avg_value": round(sum(values) / len(values), 4) if values else 0.0,
            }
            for hour, values in hourly.items()
        ]
        total = weekday_total + weekend_total
        active_total = day_total + night_total
        weekday_peak_hours = [
            {
                "hour": hour,
                "label": f"{hour:02d}:00",
                "avg_value": round(sum(values) / len(values), 4),
            }
            for hour, values in weekday_hourly.items()
            if values
        ]
        weekday_peak_hours.sort(key=lambda item: item["avg_value"], reverse=True)
        night_base_avg = round(sum(night_base_values) / len(night_base_values), 4) if night_base_values else 0.0
        summary_avg = round(sum(float(row["electricity_kwh"]) for row in rows) / len(rows), 4) if rows else 0.0

        return {
            "metric_type": metric,
            "metric_label": "电力",
            "unit": "kWh",
            "building_id": building_id or "ALL",
            "hourly_profile": hourly_profile,
            "weekday_weekend_split": [
                {"label": "工作日", "value": round(weekday_total, 4), "ratio_pct": round((weekday_total / total * 100) if total else 0.0, 2)},
                {"label": "周末", "value": round(weekend_total, 4), "ratio_pct": round((weekend_total / total * 100) if total else 0.0, 2)},
            ],
            "day_night_split": [
                {"label": "白天", "value": round(day_total, 4), "ratio_pct": round((day_total / active_total * 100) if active_total else 0.0, 2)},
                {"label": "夜间", "value": round(night_total, 4), "ratio_pct": round((night_total / active_total * 100) if active_total else 0.0, 2)},
            ],
            "weekday_peak_hours": weekday_peak_hours[:3],
            "night_base_load": {
                "avg_value": night_base_avg,
                "ratio_vs_avg_pct": round((night_base_avg / summary_avg * 100) if summary_avg else 0.0, 2),
            },
        }

    def query_analysis_compare(
        self,
        building_id: str | None,
        start_time: dt.datetime | None,
        end_time: dt.datetime | None,
        metric_type: str | None = "electricity",
    ) -> dict[str, Any]:
        metric = self._require_metric_type(metric_type)
        if not building_id:
            return {
                "metric_type": metric,
                "metric_label": "电力",
                "unit": "kWh",
                "building": None,
                "peer_group": None,
                "items": [],
                "peer_ranking": [],
                "message": "请选择单体建筑后查看同类对比。",
            }
        scoped_rows = self._filter_rows(building_id, start_time, end_time)
        target_building_id, _ = self._analysis_target_building(building_id, scoped_rows)
        if not target_building_id:
            return {
                "metric_type": metric,
                "metric_label": "电力",
                "unit": "kWh",
                "building": None,
                "peer_group": None,
                "items": [],
                "peer_ranking": [],
                "message": "当前筛选范围暂无可对比数据。",
            }

        target_meta = self.buildings_meta.get(target_building_id, {})
        target_type = target_meta.get("building_type", "unknown")
        peer_rows = self._filter_rows(None, start_time, end_time)
        grouped: dict[str, list[float]] = {}
        grouped_type: dict[str, str] = {}
        for row in peer_rows:
            grouped.setdefault(row["building_id"], []).append(float(row["electricity_kwh"]))
            grouped_type[row["building_id"]] = str(row["building_type"])

        building_avg = {bid: (sum(vals) / len(vals)) for bid, vals in grouped.items() if vals}
        peer_candidates = {bid: avg for bid, avg in building_avg.items() if grouped_type.get(bid) == target_type}
        peer_avg = round(sum(peer_candidates.values()) / len(peer_candidates), 4) if peer_candidates else 0.0
        target_avg = round(building_avg.get(target_building_id, 0.0), 4)
        vs_peer_pct = round(((target_avg - peer_avg) / peer_avg * 100) if peer_avg else 0.0, 2)
        percentile = round(
            (
                sum(1 for avg in peer_candidates.values() if avg <= target_avg) / len(peer_candidates) * 100
            ) if peer_candidates else 0.0,
            2,
        )
        ordered_peers = [bid for bid, _ in sorted(peer_candidates.items(), key=lambda item: item[1], reverse=True)]
        ranking_position = ordered_peers.index(target_building_id) + 1 if target_building_id in ordered_peers else None

        ranking = [
            {
                "building_id": bid,
                "building_name": self.buildings_meta.get(bid, {}).get("building_name", bid),
                "avg_value": round(avg, 4),
            }
            for bid, avg in sorted(peer_candidates.items(), key=lambda x: x[1], reverse=True)[:8]
        ]

        return {
            "metric_type": metric,
            "metric_label": "电力",
            "unit": "kWh",
            "building": {
                "building_id": target_building_id,
                "building_name": target_meta.get("building_name", target_building_id),
                "building_type": target_type,
                "avg_value": target_avg,
            },
            "peer_group": {
                "building_type": target_type,
                "peer_avg_value": peer_avg,
                "peer_count": len(peer_candidates),
                "vs_peer_pct": vs_peer_pct,
                "gap_pct": vs_peer_pct,
                "peer_percentile": percentile,
                "ranking_position": ranking_position,
            },
            "items": [
                {"label": "当前建筑", "value": target_avg},
                {"label": "同类均值", "value": peer_avg},
            ],
            "peer_ranking": ranking,
        }

    def query_analysis_insights(
        self,
        building_id: str | None,
        start_time: dt.datetime | None,
        end_time: dt.datetime | None,
        metric_type: str | None = "electricity",
    ) -> dict[str, Any]:
        metric = self._require_metric_type(metric_type)
        rows = self._filter_rows(building_id, start_time, end_time)
        ordered_rows = sorted(rows, key=lambda item: item["timestamp"])
        summary = self.query_analysis_summary(building_id, start_time, end_time, metric)
        trend = self.query_analysis_trend(building_id, start_time, end_time, metric)
        distribution = self.query_analysis_distribution(building_id, start_time, end_time, metric)
        compare = self.query_analysis_compare(building_id, start_time, end_time, metric)
        anomalies = self._filter_anomalies(building_id, start_time, end_time)
        building_meta = self.buildings_meta.get(building_id or "", {})
        weather_stats = self._weather_relation_stats(trend["series"])

        scope_summary = {
            "building_id": building_id or "ALL",
            "building_name": building_meta.get("building_name", "全部建筑"),
            "building_type": building_meta.get("building_type", "portfolio") if building_id else "portfolio",
            "selected_start_time": to_iso(start_time) if start_time else None,
            "selected_end_time": to_iso(end_time) if end_time else None,
            "data_start_time": to_iso(ordered_rows[0]["timestamp"]) if ordered_rows else None,
            "data_end_time": to_iso(ordered_rows[-1]["timestamp"]) if ordered_rows else None,
            "point_count": len(ordered_rows),
            "granularity": self._granularity_label(ordered_rows),
            "anomaly_count": len(anomalies),
            "metric_label": "电力",
            "unit": "kWh",
        }

        trend_findings: list[dict[str, str]] = []
        weather_findings: list[dict[str, str]] = []
        compare_findings: list[dict[str, str]] = []
        saving_opportunities: list[dict[str, Any]] = []

        change_pct = float(trend["summary"].get("window_change_pct", 0.0))
        volatility_pct = float(summary.get("volatility_pct", 0.0))
        temp_corr = float(trend["summary"].get("temperature_correlation", 0.0))
        working_hour_avg = float(summary.get("working_hour_avg", 0.0))
        off_hour_avg = float(summary.get("off_hour_avg", 0.0))
        working_off_gap_pct = ((working_hour_avg - off_hour_avg) / off_hour_avg * 100) if off_hour_avg else 0.0

        if change_pct >= 12:
            trend_findings.append(self._insight_item("近期负荷抬升", f"前后窗口均值上升 {round(change_pct, 2)}%，近期存在明显增载。", "warning"))
        elif change_pct <= -12:
            trend_findings.append(self._insight_item("近期负荷回落", f"前后窗口均值下降 {round(abs(change_pct), 2)}%，近期运行压力有所回落。", "success"))
        else:
            trend_findings.append(self._insight_item("趋势整体平稳", f"窗口变化 {round(change_pct, 2)}%，当前负荷没有明显阶跃变化。", "info"))

        if volatility_pct >= 35:
            trend_findings.append(self._insight_item("波动偏大", f"波动率 {round(volatility_pct, 2)}%，建议重点关注启停频繁时段。", "danger"))
        else:
            trend_findings.append(self._insight_item("稳定性可控", f"波动率 {round(volatility_pct, 2)}%，曲线整体处于可解释区间。", "success"))

        if working_off_gap_pct >= 60:
            trend_findings.append(self._insight_item("工作时段主导负荷", f"工作时段均值较非工作时段高 {round(working_off_gap_pct, 2)}%，节能重点应放在白天主业务时段。", "info"))
        elif off_hour_avg and (off_hour_avg / max(working_hour_avg, 1e-6)) >= 0.7:
            trend_findings.append(self._insight_item("非工作时段负荷偏高", "非工作时段负荷没有明显回落，可能存在待机或常开设备。", "warning"))

        if trend["overlay_available"]:
            if temp_corr >= 0.35:
                weather_findings.append(self._insight_item("温度正相关明显", f"温度相关系数 {round(temp_corr, 2)}，热天气会显著抬升负荷。", "warning"))
            elif temp_corr <= -0.35:
                weather_findings.append(self._insight_item("温度反向影响", f"温度相关系数 {round(temp_corr, 2)}，低温时段负荷抬升更明显。", "info"))
            else:
                weather_findings.append(self._insight_item("天气影响有限", f"温度相关系数 {round(temp_corr, 2)}，当前窗口内负荷更受内部运行策略影响。", "info"))

            if weather_stats["hot_avg_load"] and weather_stats["cold_avg_load"]:
                weather_findings.append(
                    self._insight_item(
                        "冷热天差异",
                        f"热天均值 {weather_stats['hot_avg_load']} kWh，冷天均值 {weather_stats['cold_avg_load']} kWh，差异 {round(weather_stats['hot_cold_gap_pct'], 2)}%。",
                        "info" if abs(weather_stats["hot_cold_gap_pct"]) < 12 else "warning",
                    )
                )

        peer_group = compare.get("peer_group") or {}
        if compare.get("message"):
            compare_findings.append(self._insight_item("同类对比待选定", compare["message"], "info"))
        else:
            compare_findings.append(
                self._insight_item(
                    "同类位置",
                    f"当前建筑在同类中位于前 {round(peer_group.get('peer_percentile', 0.0), 2)} 百分位，排名 {peer_group.get('ranking_position') or '-'} / {peer_group.get('peer_count') or '-'}。",
                    "warning" if float(peer_group.get("gap_pct", 0.0)) > 10 else "info",
                )
            )
            compare_findings.append(
                self._insight_item(
                    "与同类均值差距",
                    f"当前均值较同类均值 {'高' if float(peer_group.get('gap_pct', 0.0)) >= 0 else '低'} {round(abs(float(peer_group.get('gap_pct', 0.0))), 2)}%。",
                    "warning" if abs(float(peer_group.get("gap_pct", 0.0))) >= 10 else "success",
                )
            )

        if distribution["night_base_load"]["avg_value"] > 0:
            saving_opportunities.append(
                self._opportunity_item(
                    "夜间基线负荷优化",
                    f"夜间基线约 {distribution['night_base_load']['avg_value']} kWh，为整体均值的 {distribution['night_base_load']['ratio_vs_avg_pct']}%。可优先排查夜间常开设备。",
                    "high" if distribution["night_base_load"]["ratio_vs_avg_pct"] >= 55 else "medium",
                    distribution["night_base_load"]["avg_value"],
                )
            )

        anomaly_waste = sum(max(float(item["electricity_kwh"]) - float(item["mean_kwh"]), 0.0) for item in anomalies)
        if anomaly_waste > 0:
            saving_opportunities.append(
                self._opportunity_item(
                    "异常浪费回收",
                    f"当前窗口异常额外损失约 {round(anomaly_waste, 2)} kWh，可通过优先处理突增和持续高负荷事件回收损耗。",
                    "high" if len(anomalies) >= 3 else "medium",
                    anomaly_waste,
                )
            )

        if float(peer_group.get("gap_pct", 0.0)) > 8 and summary["point_count"] > 0:
            estimated_gap = max(float(summary["avg_value"]) - float(peer_group.get("peer_avg_value", 0.0)), 0.0) * summary["point_count"]
            saving_opportunities.append(
                self._opportunity_item(
                    "同类差距收敛",
                    f"若将均值收敛到同类水平，当前窗口理论可优化约 {round(estimated_gap, 2)} kWh。",
                    "medium",
                    estimated_gap,
                )
            )

        peak_hours = distribution.get("weekday_peak_hours") or []
        if peak_hours:
            top_hour = peak_hours[0]
            saving_opportunities.append(
                self._opportunity_item(
                    "高峰时段策略优化",
                    f"工作日高峰集中在 {top_hour['label']} 左右，平均负荷 {top_hour['avg_value']} kWh，适合做错峰和设定点优化。",
                    "medium",
                    top_hour["avg_value"],
                )
            )

        anomaly_windows = [
            {
                "anomaly_id": item["anomaly_id"],
                "timestamp": item["timestamp"],
                "anomaly_name": item["anomaly_name"],
                "severity": item["severity"],
                "deviation_pct": item["deviation_pct"],
                "estimated_loss_kwh": item["estimated_loss_kwh"],
            }
            for item in self._trend_markers(anomalies)[:4]
        ]

        return {
            "metric_type": metric,
            "scope_summary": scope_summary,
            "trend_findings": trend_findings[:4],
            "weather_findings": weather_findings[:3],
            "compare_findings": compare_findings[:3],
            "saving_opportunities": saving_opportunities[:4],
            "anomaly_windows": anomaly_windows,
        }

    def query_trend(self, building_id: str | None, start_time: dt.datetime | None, end_time: dt.datetime | None) -> dict[str, Any]:
        data = self.query_analysis_trend(building_id, start_time, end_time, "electricity")
        return {
            "unit": "kWh",
            "building_id": building_id or "ALL",
            "point_count": len(data["series"]),
            "series": data["series"],
            "summary": {
                "total_kwh": data["summary"]["total_value"],
                "avg_kwh": data["summary"]["avg_value"],
                "peak_kwh": data["summary"]["peak_value"],
            },
        }

    def query_rank(self, month: str | None) -> dict[str, Any]:
        target_month = month or max(r["timestamp"].strftime("%Y-%m") for r in self.rows)
        rank_items = []
        for building_id, rows in self.by_building.items():
            month_rows = [r for r in rows if r["timestamp"].strftime("%Y-%m") == target_month]
            if not month_rows:
                continue
            total = sum(r["electricity_kwh"] for r in month_rows)
            avg = total / len(month_rows)
            rank_items.append(
                {
                    "building_id": building_id,
                    "building_name": month_rows[0]["building_name"],
                    "building_type": month_rows[0]["building_type"],
                    "month": target_month,
                    "total_kwh": round(total, 4),
                    "avg_kwh": round(avg, 4),
                }
            )
        rank_items.sort(key=lambda x: x["avg_kwh"], reverse=True)
        for i, item in enumerate(rank_items, start=1):
            item["rank"] = i
        return {"month": target_month, "unit": "kWh", "items": rank_items}

    def query_anomalies(
        self,
        building_id: str | None,
        start_time: dt.datetime | None,
        end_time: dt.datetime | None,
        anomaly_type: str | None,
        severity: str | None,
        status: str | None,
        page: int = 1,
        page_size: int = 20,
        sort: str = "timestamp_desc",
    ) -> dict[str, Any]:
        filtered = self._filter_anomalies(building_id, start_time, end_time, anomaly_type, severity, status)
        sorted_rows = self._sort_anomalies(filtered, sort)

        by_type: dict[str, int] = {}
        by_status: dict[str, int] = {}
        for item in sorted_rows:
            by_type[item["anomaly_type"]] = by_type.get(item["anomaly_type"], 0) + 1
            st = self._action_state(int(item["anomaly_id"]))["status"]
            by_status[st] = by_status.get(st, 0) + 1

        safe_page = max(1, page)
        safe_page_size = min(max(1, page_size), 200)
        total_count = len(sorted_rows)
        total_pages = max(1, math.ceil(total_count / safe_page_size))
        if safe_page > total_pages:
            safe_page = total_pages

        start_idx = (safe_page - 1) * safe_page_size
        end_idx = start_idx + safe_page_size
        page_rows = sorted_rows[start_idx:end_idx]

        items = []
        for x in page_rows:
            action_state = self._action_state(int(x["anomaly_id"]))
            items.append(
                {
                    **x,
                    "timestamp": to_iso(x["timestamp"]),
                    "anomaly_name": self.dict_data.get(x["anomaly_type"], {}).get("name", x["anomaly_type"]),
                    "status": action_state["status"],
                    "assignee": action_state["assignee"],
                    "last_note": action_state["last_note"],
                    "last_action_at": action_state["last_action_at"],
                }
            )

        return {
            "count": len(items),
            "total_count": total_count,
            "page": safe_page,
            "page_size": safe_page_size,
            "total_pages": total_pages,
            "sort": sort,
            "severity": severity,
            "status": status,
            "by_type": by_type,
            "by_status": by_status,
            "items": items,
        }

    def query_metrics_overview(self, building_id: str | None, start_time: dt.datetime | None, end_time: dt.datetime | None) -> dict[str, Any]:
        rows = self._filter_rows(building_id, start_time, end_time)
        anomalies = self._filter_anomalies(building_id, start_time, end_time)
        values = [r["electricity_kwh"] for r in rows]
        total = sum(values)
        avg = total / len(values) if values else 0
        peak = max(values) if values else 0
        peak_excess_pct = ((peak - avg) / avg * 100) if avg else 0
        wasted_kwh = sum(max(a["electricity_kwh"] - a["mean_kwh"], 0) for a in anomalies)
        carbon_reduction_kg = wasted_kwh * CARBON_FACTOR
        return {
            "building_id": building_id or "ALL",
            "unit": "kWh",
            "carbon_factor": CARBON_FACTOR,
            "total_kwh": round(total, 4),
            "avg_kwh": round(avg, 4),
            "peak_kwh": round(peak, 4),
            "anomaly_count": len(anomalies),
            "saving_potential_pct": round(max(peak_excess_pct, 0), 2),
            "wasted_kwh": round(wasted_kwh, 4),
            "carbon_reduction_kg": round(carbon_reduction_kg, 4),
            "method_note": "peak_excess + anomaly_waste",
        }

    def query_saving_potential(self, building_id: str | None, start_time: dt.datetime | None, end_time: dt.datetime | None) -> dict[str, Any]:
        rows = self._filter_rows(building_id, start_time, end_time)
        anomalies = self._filter_anomalies(building_id, start_time, end_time)
        values = [r["electricity_kwh"] for r in rows]
        avg = sum(values) / len(values) if values else 0
        peak = max(values) if values else 0
        peak_excess_pct = ((peak - avg) / avg * 100) if avg else 0
        wasted_kwh = sum(max(a["electricity_kwh"] - a["mean_kwh"], 0) for a in anomalies)
        carbon_reduction_kg = wasted_kwh * CARBON_FACTOR

        by_building: dict[str, list[float]] = {}
        btype: dict[str, str] = {}
        for r in rows:
            by_building.setdefault(r["building_id"], []).append(r["electricity_kwh"])
            btype[r["building_id"]] = r["building_type"]

        peer_info = {"building_id": None, "building_type": None, "vs_type_avg_pct": 0.0, "building_avg_kwh": 0.0, "type_avg_kwh": 0.0}
        if by_building:
            building_avg = {k: sum(v) / len(v) for k, v in by_building.items() if v}
            type_groups: dict[str, list[float]] = {}
            for bid, bav in building_avg.items():
                type_groups.setdefault(btype.get(bid, "unknown"), []).append(bav)
            type_avg = {k: sum(v) / len(v) for k, v in type_groups.items() if v}
            target_bid = building_id if building_id in building_avg else max(building_avg, key=building_avg.get)
            target_type = btype.get(target_bid, "unknown")
            t_avg = type_avg.get(target_type, 0)
            b_avg = building_avg.get(target_bid, 0)
            vs_pct = ((b_avg - t_avg) / t_avg * 100) if t_avg else 0
            peer_info = {
                "building_id": target_bid,
                "building_type": target_type,
                "vs_type_avg_pct": round(vs_pct, 2),
                "building_avg_kwh": round(b_avg, 4),
                "type_avg_kwh": round(t_avg, 4),
            }

        return {
            "building_id": building_id or "ALL",
            "method_peak_excess_pct": round(max(peak_excess_pct, 0), 2),
            "method_anomaly_waste_kwh": round(wasted_kwh, 4),
            "method_anomaly_carbon_kg": round(carbon_reduction_kg, 4),
            "method_peer_compare": peer_info,
        }

    def query_anomaly_detail(self, anomaly_id: int) -> dict[str, Any] | None:
        context = next((item for item in self.anomalies if item["anomaly_id"] == anomaly_id), None)
        if not context:
            return None

        building_rows = self.by_building.get(context["building_id"], [])
        local_start = context["timestamp"] - dt.timedelta(hours=6)
        local_end = context["timestamp"] + dt.timedelta(hours=6)
        neighborhood = [r for r in building_rows if local_start <= r["timestamp"] <= local_end]
        timeline = [{"timestamp": to_iso(r["timestamp"]), "value": round(r["electricity_kwh"], 4)} for r in neighborhood]

        baseline_start = context["timestamp"] - dt.timedelta(hours=24)
        baseline_rows = [r for r in building_rows if baseline_start <= r["timestamp"] < context["timestamp"]]
        baseline_values = [r["electricity_kwh"] for r in baseline_rows]
        baseline_avg = sum(baseline_values) / len(baseline_values) if baseline_values else context["mean_kwh"]
        baseline_max = max(baseline_values) if baseline_values else context["mean_kwh"]
        baseline_min = min(baseline_values) if baseline_values else context["mean_kwh"]

        estimated_loss = max(context["electricity_kwh"] - baseline_avg, 0)
        peer_rows = [r for r in self.rows if r["timestamp"] == context["timestamp"] and r["building_type"] == context["building_type"]]
        peer_avg = (sum(r["electricity_kwh"] for r in peer_rows) / len(peer_rows)) if peer_rows else context["mean_kwh"]
        vs_peer_pct = ((context["electricity_kwh"] - peer_avg) / peer_avg * 100) if peer_avg else 0

        knowledge = self.dict_data.get(context["anomaly_type"], {})
        action_state = self._action_state(anomaly_id)
        note = self.query_anomaly_note(anomaly_id)

        return {
            "anomaly": {
                **context,
                "timestamp": to_iso(context["timestamp"]),
                "anomaly_name": knowledge.get("name", context["anomaly_type"]),
                "status": action_state["status"],
            },
            "impact": {
                "estimated_loss_kwh": round(estimated_loss, 4),
                "wasted_kwh": round(estimated_loss, 4),
                "carbon_kg": round(estimated_loss * CARBON_FACTOR, 4),
            },
            "baseline_window": {
                "hours": 24,
                "start_time": to_iso(baseline_start),
                "end_time": to_iso(context["timestamp"]),
                "avg_kwh": round(baseline_avg, 4),
                "max_kwh": round(baseline_max, 4),
                "min_kwh": round(baseline_min, 4),
                "sample_count": len(baseline_rows),
            },
            "peer_compare": {
                "building_type": context["building_type"],
                "peer_avg_kwh": round(peer_avg, 4),
                "vs_peer_pct": round(vs_peer_pct, 2),
                "peer_sample_count": len(peer_rows),
            },
            "timeline": timeline,
            "recommended_actions": knowledge.get("steps", [])[:5],
            "prevention": knowledge.get("prevention", [])[:5],
            "processing_summary": {
                "latest_status": action_state["status"],
                "history_count": action_state["history_count"],
                "latest_note": action_state["last_note"],
                "assignee": action_state["assignee"],
                "last_action_at": action_state["last_action_at"],
            },
            "postmortem_note": note,
        }

    def _search_knowledge(self, anomaly_type: str, message: str, limit: int = 3) -> list[dict[str, str]]:
        if not self.knowledge_chunks:
            return []
        keywords = [k.lower() for k in self.dict_data.get(anomaly_type, {}).get("keywords", [])]
        extra_tokens = [t.lower() for t in message.replace("，", " ").replace(",", " ").split() if len(t) >= 2]
        tokens = [t for t in (keywords + extra_tokens) if t]
        if not tokens:
            return []

        scored: list[tuple[int, str, dict[str, Any]]] = []
        for chunk in self.knowledge_chunks:
            text = str(chunk.get("text", "")).lower()
            if not text:
                continue
            score = sum(1 for tk in tokens if tk in text)
            if score > 0:
                scored.append((score, str(chunk.get("chunk_id", "")), chunk))

        scored.sort(key=lambda x: (-x[0], x[1]))
        picked = []
        for _, _, chunk in scored[:limit]:
            text = str(chunk.get("text", "")).replace("\n", " ")
            picked.append(
                {
                    "chunk_id": str(chunk.get("chunk_id", "")),
                    "title": str(chunk.get("title", "")),
                    "section": str(chunk.get("chunk_id", "")),
                    "excerpt": text[:220],
                }
            )
        return picked

    def _resolve_context(self, payload: dict[str, Any]) -> tuple[dict[str, Any] | None, str | None]:
        anomaly_id = payload.get("anomaly_id")
        building_id = str(payload.get("building_id", "")).strip() or None
        timestamp = parse_time(str(payload.get("timestamp", "")).strip() or None)

        context = None
        if anomaly_id:
            try:
                aid = int(anomaly_id)
            except (TypeError, ValueError):
                aid = None
            if aid is not None:
                context = next((item for item in self.anomalies if item["anomaly_id"] == aid), None)

        if context is None and building_id and timestamp:
            context = next(
                (
                    item
                    for item in self.anomalies
                    if item["building_id"] == building_id and abs((item["timestamp"] - timestamp).total_seconds()) <= 3600
                ),
                None,
            )

        if context is None and building_id:
            context = next((item for item in self.anomalies if item["building_id"] == building_id), None)

        return context, building_id

    def _diagnose_by_template(self, payload: dict[str, Any]) -> dict[str, Any]:
        message = str(payload.get("message", "")).strip()
        anomaly_type = str(payload.get("anomaly_type", "")).strip() or None
        context, fallback_building_id = self._resolve_context(payload)

        if anomaly_type:
            chosen_type = anomaly_type
        elif context:
            chosen_type = context["anomaly_type"]
        else:
            chosen_type = self._type_from_keywords(message)

        knowledge = self.dict_data.get(chosen_type, self.dict_data.get("anomaly_spike", {}))
        anomaly_name = knowledge.get("name", chosen_type)
        causes = knowledge.get("possible_causes", [])
        steps = knowledge.get("steps", [])
        prevention = knowledge.get("prevention", [])
        evidence = self._search_knowledge(chosen_type, message)

        if context:
            conclusion = (
                f"{context['building_name']} 在 {to_iso(context['timestamp'])} 出现{anomaly_name}，"
                f"实测 {context['electricity_kwh']} kWh，历史均值 {context['mean_kwh']} kWh，"
                f"偏差 {context['deviation_pct']}%。"
            )
        else:
            conclusion = (
                f"根据你提供的问题，系统判定为 {anomaly_name} 场景。"
                "建议先执行基础排查步骤，再结合实时曲线确认是否恢复。"
            )

        if context and evidence:
            confidence = 0.85
        elif context:
            confidence = 0.72
        elif evidence:
            confidence = 0.58
        else:
            confidence = 0.45

        risk_level = "high" if confidence >= 0.8 else "medium" if confidence >= 0.6 else "low"
        recommended_actions = steps[:3] if steps else prevention[:3]

        return {
            "diagnosis": {
                "anomaly_type": chosen_type,
                "anomaly_name": anomaly_name,
                "conclusion": conclusion,
                "causes": causes,
                "possible_causes": causes,
                "steps": steps,
                "prevention": prevention,
                "recommended_actions": recommended_actions,
                "evidence": evidence,
                "confidence": round(confidence, 2),
                "risk_level": risk_level,
            },
            "context": {
                "anomaly_id": context["anomaly_id"] if context else None,
                "building_id": context["building_id"] if context else fallback_building_id,
                "timestamp": to_iso(context["timestamp"]) if context else None,
            },
        }

    def _build_analysis_context(
        self,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        metric_type = str(payload.get("metric_type", "electricity")).strip().lower() or "electricity"
        building_id = str(payload.get("building_id", "")).strip() or None
        start_time = parse_time(str(payload.get("start_time", "")).strip() or None)
        end_time = parse_time(str(payload.get("end_time", "")).strip() or None)

        summary = self.query_analysis_summary(building_id, start_time, end_time, metric_type)
        trend = self.query_analysis_trend(building_id, start_time, end_time, metric_type)
        distribution = self.query_analysis_distribution(building_id, start_time, end_time, metric_type)
        compare = self.query_analysis_compare(building_id, start_time, end_time, metric_type)
        payload_insights = payload.get("insights")
        insights = payload_insights if isinstance(payload_insights, dict) else self.query_analysis_insights(building_id, start_time, end_time, metric_type)
        building = compare.get("building") or {}

        return {
            "building_id": building_id or building.get("building_id"),
            "building_name": building.get("building_name") or (self.buildings_meta.get(building_id or "", {}) or {}).get("building_name"),
            "building_type": building.get("building_type") or (self.buildings_meta.get(building_id or "", {}) or {}).get("building_type"),
            "metric_type": metric_type,
            "start_time": to_iso(start_time) if start_time else None,
            "end_time": to_iso(end_time) if end_time else None,
            "summary": summary,
            "trend": trend,
            "distribution": distribution,
            "compare": compare,
            "insights": insights,
        }

    def _truncate_text(self, value: Any, limit: int = 160) -> str:
        text = str(value or "").strip()
        if len(text) <= limit:
            return text
        return f"{text[:limit].rstrip()}..."

    def _build_analysis_prompt_context(
        self,
        payload: dict[str, Any],
        context: dict[str, Any],
        analysis_seed: dict[str, Any],
    ) -> dict[str, Any]:
        summary = context.get("summary") or {}
        insights = context.get("insights") or {}
        compare = context.get("compare") or {}
        trend_snapshot = payload.get("trend_snapshot") if isinstance(payload.get("trend_snapshot"), dict) else {}
        distribution_snapshot = payload.get("distribution_snapshot") if isinstance(payload.get("distribution_snapshot"), dict) else {}
        compare_snapshot = payload.get("compare_snapshot") if isinstance(payload.get("compare_snapshot"), dict) else {}

        def compact_findings(items: list[Any], limit: int = 4) -> list[dict[str, str]]:
            compacted = []
            for item in (items or [])[:limit]:
                if not isinstance(item, dict):
                    continue
                compacted.append(
                    {
                        "title": self._truncate_text(item.get("title", ""), 80),
                        "detail": self._truncate_text(item.get("detail", ""), 180),
                        "severity": str(item.get("severity", "")).strip() or "info",
                    }
                )
            return compacted

        def compact_opportunities(items: list[Any], limit: int = 4) -> list[dict[str, Any]]:
            compacted = []
            for item in (items or [])[:limit]:
                if not isinstance(item, dict):
                    continue
                compacted.append(
                    {
                        "title": self._truncate_text(item.get("title", ""), 80),
                        "detail": self._truncate_text(item.get("detail", ""), 180),
                        "priority": str(item.get("priority", "")).strip() or "info",
                        "estimated_loss_kwh": item.get("estimated_loss_kwh", 0),
                    }
                )
            return compacted

        def compact_windows(items: list[Any], limit: int = 5) -> list[dict[str, Any]]:
            compacted = []
            for item in (items or [])[:limit]:
                if not isinstance(item, dict):
                    continue
                compacted.append(
                    {
                        "timestamp": item.get("timestamp"),
                        "anomaly_name": self._truncate_text(item.get("anomaly_name", ""), 60),
                        "severity": item.get("severity"),
                        "deviation_pct": item.get("deviation_pct"),
                        "estimated_loss_kwh": item.get("estimated_loss_kwh"),
                    }
                )
            return compacted

        compact_evidence = []
        for idx, item in enumerate((analysis_seed.get("evidence") or [])[:3], start=1):
            if isinstance(item, dict):
                compact_evidence.append(
                    {
                        "chunk_id": item.get("chunk_id", f"seed-{idx}"),
                        "title": self._truncate_text(item.get("title", ""), 80),
                        "section": self._truncate_text(item.get("section", ""), 80),
                        "excerpt": self._truncate_text(item.get("excerpt", ""), 180),
                    }
                )
            else:
                compact_evidence.append(
                    {
                        "chunk_id": f"seed-{idx}",
                        "title": "seed",
                        "section": "",
                        "excerpt": self._truncate_text(item, 180),
                    }
                )

        return {
            "building": {
                "building_id": context.get("building_id"),
                "building_name": context.get("building_name"),
                "building_type": context.get("building_type"),
                "metric_type": context.get("metric_type"),
                "start_time": context.get("start_time"),
                "end_time": context.get("end_time"),
            },
            "summary": {
                "metric_label": summary.get("metric_label"),
                "unit": summary.get("unit"),
                "point_count": summary.get("point_count"),
                "total_value": summary.get("total_value"),
                "avg_value": summary.get("avg_value"),
                "peak_value": summary.get("peak_value"),
                "volatility_pct": summary.get("volatility_pct"),
                "anomaly_count": summary.get("anomaly_count"),
                "working_hour_avg": summary.get("working_hour_avg"),
                "off_hour_avg": summary.get("off_hour_avg"),
            },
            "trend_snapshot": {
                "window_change_pct": trend_snapshot.get("window_change_pct", context.get("trend", {}).get("summary", {}).get("window_change_pct")),
                "temperature_correlation": trend_snapshot.get("temperature_correlation", context.get("trend", {}).get("summary", {}).get("temperature_correlation")),
            },
            "distribution_snapshot": {
                "weekday_peak_hours": distribution_snapshot.get("weekday_peak_hours") or (context.get("distribution", {}).get("weekday_peak_hours") or [])[:4],
                "night_base_load": distribution_snapshot.get("night_base_load") or context.get("distribution", {}).get("night_base_load") or {},
            },
            "compare_snapshot": compare_snapshot or compare.get("peer_group") or {},
            "insight_snapshot": {
                "scope_summary": insights.get("scope_summary") or {},
                "trend_findings": compact_findings(insights.get("trend_findings") or []),
                "weather_findings": compact_findings(insights.get("weather_findings") or []),
                "compare_findings": compact_findings(insights.get("compare_findings") or []),
                "saving_opportunities": compact_opportunities(insights.get("saving_opportunities") or []),
                "anomaly_windows": compact_windows(insights.get("anomaly_windows") or []),
            },
            "analysis_seed": {
                "summary": self._truncate_text(analysis_seed.get("summary", ""), 220),
                "findings": [self._truncate_text(item, 160) for item in (analysis_seed.get("findings") or [])[:5]],
                "possible_causes": [self._truncate_text(item, 160) for item in (analysis_seed.get("possible_causes") or [])[:4]],
                "energy_saving_suggestions": [self._truncate_text(item, 160) for item in (analysis_seed.get("energy_saving_suggestions") or [])[:4]],
                "operations_suggestions": [self._truncate_text(item, 160) for item in (analysis_seed.get("operations_suggestions") or [])[:4]],
                "evidence": compact_evidence,
            },
        }

    def _analyze_by_template(self, payload: dict[str, Any]) -> dict[str, Any]:
        context = self._build_analysis_context(payload)
        summary = context["summary"]
        insights = context["insights"]
        building_name = context.get("building_name") or "当前建筑"
        metric_label = "电力"
        findings = [
            f"{item['title']}：{item['detail']}"
            for item in (
                (insights.get("trend_findings") or []) +
                (insights.get("weather_findings") or []) +
                (insights.get("compare_findings") or [])
            )[:6]
        ]

        possible_causes: list[str] = []
        for item in insights.get("weather_findings") or []:
            text = str(item.get("detail", ""))
            if "热天气" in text or "温度正相关" in text:
                possible_causes.append("负荷受外部气温影响明显，空调或通风策略可能抬高用电。")
            elif "低温" in text or "反向影响" in text:
                possible_causes.append("低温时段相关负荷抬升，需排查采暖或伴热运行策略。")
        for item in insights.get("trend_findings") or []:
            text = str(item.get("detail", ""))
            if "启停" in text or "波动" in text:
                possible_causes.append("设备启停节奏可能偏密，导致曲线稳定性下降。")
            elif "非工作时段负荷偏高" in f"{item.get('title', '')}{text}":
                possible_causes.append("存在夜间待机、常开或联动策略未收敛的问题。")
        if not possible_causes:
            possible_causes.append("建议结合设备台账、运行日程和异常时间窗进一步定位根因。")

        energy_saving = [
            f"{item['title']}：{item['detail']}"
            for item in (insights.get("saving_opportunities") or [])[:4]
        ]
        if not energy_saving:
            energy_saving.append("当前窗口未发现明确的高优先节能机会，可继续跟踪高峰和夜间负荷。")

        operations = [
            f"优先复核 {item['timestamp']} 的 {item['anomaly_name']}，偏差 {item['deviation_pct']}%，影响约 {item['estimated_loss_kwh']} kWh。"
            for item in (insights.get("anomaly_windows") or [])[:3]
        ]
        if not operations:
            operations.append("继续跟踪高峰时段与夜间基线，形成设备运行排班对照。")

        evidence = []
        message = str(payload.get("message", "")).strip() or f"{building_name} {metric_label} 分析"
        for item in self._search_knowledge("anomaly_sustained_high_load", message, limit=3):
            evidence.append(item)

        scope = insights.get("scope_summary") or {}
        summary_text = (
            f"{building_name} 当前{metric_label}分析覆盖 {scope.get('data_start_time') or '-'} 至 {scope.get('data_end_time') or '-'}，"
            f"共 {scope.get('point_count', 0)} 个数据点；均值 {summary['avg_value']} kWh，"
            f"峰值 {summary['peak_value']} kWh，波动率 {summary['volatility_pct']}%。"
        )

        return {
            "analysis": {
                "summary": summary_text,
                "findings": findings,
                "possible_causes": possible_causes,
                "energy_saving_suggestions": energy_saving,
                "operations_suggestions": operations,
                "evidence": evidence,
            },
            "context": context,
        }

    def analyze(self, payload: dict[str, Any]) -> dict[str, Any]:
        preferred = str(payload.get("provider", "auto")).strip().lower()
        if preferred not in {"template", "llm", "auto"}:
            preferred = "auto"

        start = time.perf_counter()
        fallback_used = False
        error_message = None
        trace_id = uuid.uuid4().hex
        template_result = self._analyze_by_template(payload)
        result = template_result
        provider_name = "template_provider"

        try:
            if preferred in {"llm", "auto"}:
                llm_provider = self.providers["llm"]
                if payload.get("simulate_llm_failure"):
                    raise RuntimeError("Simulated llm failure")
                api_key = os.getenv("OPENAI_API_KEY", "").strip()
                if not api_key:
                    raise RuntimeError("LLM provider not configured")

                base_url = os.getenv("OPENAI_BASE_URL", "https://api.deepseek.com").strip() or "https://api.deepseek.com"
                model = os.getenv("OPENAI_MODEL", "deepseek-chat").strip() or "deepseek-chat"
                base_timeout_sec = float(os.getenv("OPENAI_TIMEOUT_SEC", "20"))
                timeout_sec = float(os.getenv("OPENAI_ANALYZE_TIMEOUT_SEC", str(max(base_timeout_sec, 45.0))))
                context = template_result["context"]
                analysis_seed = template_result["analysis"]
                user_question = str(payload.get("message", "")).strip() or "请围绕当前筛选范围输出一份可直接用于汇报的分析结论。"
                system_prompt = (
                    "你是建筑能源分析助手，面向楼宇能源管理和运维答辩场景。"
                    "你必须只输出一个JSON对象，不要输出任何解释、前后缀或Markdown。"
                    "JSON字段必须包含：summary, findings, possible_causes, energy_saving_suggestions, operations_suggestions, evidence。"
                    "findings/possible_causes/energy_saving_suggestions/operations_suggestions/evidence 必须是数组。"
                    "请使用中文；不要输出空泛套话；结论必须引用给定的时间范围、趋势、同类对比、天气联动或异常窗口。"
                    "建议动作尽量落到具体时段、运行策略、夜间基线或异常事件。"
                )
                prompt_context = self._build_analysis_prompt_context(payload, context, analysis_seed)
                user_prompt = (
                    f"当前分析上下文: {json.dumps(prompt_context, ensure_ascii=False)}\n"
                    f"用户补充问题: {user_question}\n"
                    "请基于当前建筑、筛选时间、KPI 摘要、趋势变化、温度关系、同类差距、节能机会和异常窗口生成结果。\n"
                    "如果证据不足，请明确说明证据不足，但仍要给出最稳妥的建议。\n"
                    "请输出严格JSON。"
                )
                response = llm_provider._call_chat_completion(
                    base_url=base_url,
                    api_key=api_key,
                    model=model,
                    timeout_sec=timeout_sec,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                )
                content = response.get("choices", [{}])[0].get("message", {}).get("content", "")
                if isinstance(content, list):
                    content = "".join(
                        str(part.get("text", "")) if isinstance(part, dict) else str(part)
                        for part in content
                    )
                llm_obj = llm_provider._extract_json_object(str(content))
                result = {
                    "analysis": {
                        "summary": str(llm_obj.get("summary", "")).strip() or template_result["analysis"]["summary"],
                        "findings": llm_provider._coerce_list_of_str(llm_obj.get("findings")) or template_result["analysis"]["findings"],
                        "possible_causes": llm_provider._coerce_list_of_str(llm_obj.get("possible_causes")) or template_result["analysis"]["possible_causes"],
                        "energy_saving_suggestions": llm_provider._coerce_list_of_str(llm_obj.get("energy_saving_suggestions")) or template_result["analysis"]["energy_saving_suggestions"],
                        "operations_suggestions": llm_provider._coerce_list_of_str(llm_obj.get("operations_suggestions")) or template_result["analysis"]["operations_suggestions"],
                        "evidence": llm_obj.get("evidence", template_result["analysis"]["evidence"])
                        if isinstance(llm_obj.get("evidence", template_result["analysis"]["evidence"]), list)
                        else template_result["analysis"]["evidence"],
                    },
                    "context": context,
                }
                provider_name = llm_provider.name
        except Exception as exc:
            fallback_used = True
            error_message = str(exc)
            result = template_result

        latency_ms = int((time.perf_counter() - start) * 1000)
        result["analysis"]["provider"] = provider_name
        result["analysis"]["requested_provider"] = preferred
        result["analysis"]["latency_ms"] = latency_ms
        result["analysis"]["fallback_used"] = fallback_used
        result["analysis"]["trace_id"] = trace_id
        if error_message:
            result["analysis"]["degrade_message"] = self._friendly_degrade_message(error_message)

        error_type = ""
        if fallback_used:
            msg = (error_message or "").lower()
            if "http status 429" in msg:
                error_type = "rate_limit"
            elif "maximum context length" in msg or "reduce the length of the messages" in msg:
                error_type = "context_too_long"
            elif "timed out" in msg:
                error_type = "timeout"
            elif "http status" in msg:
                error_type = "http_error"
            elif "network error" in msg:
                error_type = "network_error"
            elif "parse error" in msg:
                error_type = "parse_error"
            elif "not configured" in msg:
                error_type = "not_configured"
            else:
                error_type = "unknown_error"

        field_complete = self._required_analysis_fields_complete(result.get("analysis", {}))
        self._append_ai_event(
            {
                "timestamp": to_iso(dt.datetime.now()),
                "trace_id": trace_id,
                "requested_provider": preferred,
                "provider": provider_name,
                "fallback_used": fallback_used,
                "latency_ms": latency_ms,
                "error_type": error_type,
                "building_id": result.get("context", {}).get("building_id"),
                "event_type": "analysis",
                "field_complete": field_complete,
                "result_risk_level": "",
            }
        )
        return result

    def diagnose(self, payload: dict[str, Any]) -> dict[str, Any]:
        preferred = str(payload.get("provider", "auto")).strip().lower()
        if preferred not in {"template", "llm", "auto"}:
            preferred = "auto"

        start = time.perf_counter()
        fallback_used = False
        error_message = None
        trace_id = uuid.uuid4().hex

        try:
            if preferred in {"llm", "auto"}:
                result = self.providers["llm"].diagnose(self, payload)
                provider_name = self.providers["llm"].name
            else:
                result = self.providers["template"].diagnose(self, payload)
                provider_name = self.providers["template"].name
        except Exception as exc:
            fallback_used = True
            error_message = str(exc)
            result = self.providers["template"].diagnose(self, payload)
            provider_name = self.providers["template"].name

        latency_ms = int((time.perf_counter() - start) * 1000)
        result["diagnosis"]["provider"] = provider_name
        result["diagnosis"]["requested_provider"] = preferred
        result["diagnosis"]["latency_ms"] = latency_ms
        result["diagnosis"]["fallback_used"] = fallback_used
        result["diagnosis"]["trace_id"] = trace_id
        if error_message:
            result["diagnosis"]["degrade_message"] = self._friendly_degrade_message(error_message)

        error_type = ""
        if fallback_used:
            msg = (error_message or "").lower()
            if "http status 429" in msg:
                error_type = "rate_limit"
            elif "http status" in msg:
                error_type = "http_error"
            elif "network error" in msg:
                error_type = "network_error"
            elif "parse error" in msg:
                error_type = "parse_error"
            elif "not configured" in msg:
                error_type = "not_configured"
            else:
                error_type = "unknown_error"
        field_complete = self._required_diag_fields_complete(result.get("diagnosis", {}))
        self._append_ai_event(
            {
                "timestamp": to_iso(dt.datetime.now()),
                "trace_id": trace_id,
                "requested_provider": preferred,
                "provider": provider_name,
                "fallback_used": fallback_used,
                "latency_ms": latency_ms,
                "error_type": error_type,
                "anomaly_id": result.get("context", {}).get("anomaly_id"),
                "has_message": bool(str(payload.get("message", "")).strip()),
                "event_type": "diagnose",
                "field_complete": field_complete,
                "result_risk_level": str(result.get("diagnosis", {}).get("risk_level", "")),
            }
        )
        return result

    def _type_from_keywords(self, text: str) -> str:
        lowered = text.lower()
        for anomaly_type, details in self.dict_data.items():
            keywords = details.get("keywords", [])
            if any(k.lower() in lowered for k in keywords):
                return anomaly_type
        return "anomaly_spike"

    def _friendly_degrade_message(self, error_message: str | None) -> str:
        raw = str(error_message or "").strip()
        lowered = raw.lower()
        if "not configured" in lowered:
            return "当前环境未配置 DeepSeek API Key，本次使用模板兜底。"
        if "maximum context length" in lowered or "reduce the length of the messages" in lowered:
            return "发送给 DeepSeek 的分析上下文过长，系统已切换到模板兜底。"
        if "timed out" in lowered:
            return "DeepSeek 分析响应超时，系统已切换到模板兜底。"
        if "network error" in lowered:
            return "DeepSeek 网络请求失败，本次使用模板兜底。"
        if "http status 429" in lowered:
            return "DeepSeek 当前触发限流，本次使用模板兜底。"
        if "http status" in lowered:
            return "DeepSeek 服务响应异常，本次使用模板兜底。"
        if "parse error" in lowered:
            return "DeepSeek 返回格式异常，本次使用模板兜底。"
        if "simulated llm failure" in lowered:
            return "当前为模拟失败场景，本次使用模板兜底。"
        return "在线模型暂时不可用，系统已切换到模板兜底。"


REPO = EnergyRepository(
    DEMO_DATA_FILE,
    NORMALIZED_DATA_FILE,
    METADATA_FILE,
    WEATHER_FILE,
    DICT_FILE,
    KNOWLEDGE_FILE,
    ACTION_LOG_FILE,
    AI_CALL_LOG_FILE,
    NOTE_LOG_FILE,
    REGRESSION_SUMMARY_FILE,
)


class Handler(BaseHTTPRequestHandler):
    server_version = "A8EnergyServer/0.4"

    def _set_json_headers(self, status: int = HTTPStatus.OK) -> None:
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def _set_file_headers(self, status: int, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()

    def _json(self, data: Any, status: int = HTTPStatus.OK) -> None:
        self._set_json_headers(status)
        payload = json.dumps(data, ensure_ascii=False).encode("utf-8")
        try:
            self.wfile.write(payload)
        except (BrokenPipeError, ConnectionAbortedError, ConnectionResetError):
            return

    def _csv(self, text: str, filename: str) -> None:
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/csv; charset=utf-8")
        self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(text.encode("utf-8-sig"))

    def do_OPTIONS(self) -> None:
        self._set_json_headers(HTTPStatus.NO_CONTENT)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path.startswith("/api/"):
            self._handle_api_get(parsed)
            return
        self._serve_static(parsed.path)

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length) if length else b"{}"
        try:
            payload = json.loads(body.decode("utf-8"))
        except json.JSONDecodeError:
            self._json({"code": 400, "message": "invalid json", "data": None}, HTTPStatus.BAD_REQUEST)
            return

        if parsed.path == "/api/ai/diagnose":
            result = REPO.diagnose(payload)
            self._json({"code": 0, "message": "ok", "data": result})
            return

        if parsed.path == "/api/ai/analyze":
            try:
                result = REPO.analyze(payload)
            except ValueError as exc:
                self._json({"code": 400, "message": str(exc), "data": None}, HTTPStatus.BAD_REQUEST)
                return
            self._json({"code": 0, "message": "ok", "data": result})
            return

        if parsed.path == "/api/anomaly/action":
            try:
                result = REPO.apply_anomaly_action(payload)
            except ValueError as exc:
                self._json({"code": 400, "message": str(exc), "data": None}, HTTPStatus.BAD_REQUEST)
                return
            except LookupError as exc:
                self._json({"code": 404, "message": str(exc), "data": None}, HTTPStatus.NOT_FOUND)
                return
            self._json({"code": 0, "message": "ok", "data": result})
            return

        if parsed.path == "/api/anomaly/note":
            try:
                result = REPO.upsert_anomaly_note(payload)
            except ValueError as exc:
                self._json({"code": 400, "message": str(exc), "data": None}, HTTPStatus.BAD_REQUEST)
                return
            except LookupError as exc:
                self._json({"code": 404, "message": str(exc), "data": None}, HTTPStatus.NOT_FOUND)
                return
            self._json({"code": 0, "message": "ok", "data": result})
            return

        if parsed.path == "/api/ai/evaluate":
            hours_raw = payload.get("hours", 24)
            try:
                hours = int(hours_raw)
            except (TypeError, ValueError):
                hours = 24
            self._json({"code": 0, "message": "ok", "data": REPO.query_ai_evaluate(hours)})
            return

        if parsed.path == "/api/ai/feedback":
            try:
                result = REPO.save_ai_feedback(payload)
            except ValueError as exc:
                self._json({"code": 400, "message": str(exc), "data": None}, HTTPStatus.BAD_REQUEST)
                return
            except LookupError as exc:
                self._json({"code": 404, "message": str(exc), "data": None}, HTTPStatus.NOT_FOUND)
                return
            self._json({"code": 0, "message": "ok", "data": result})
            return

        self._json({"code": 404, "message": "not found", "data": None}, HTTPStatus.NOT_FOUND)

    def _handle_api_get(self, parsed: Any) -> None:
        params = parse_qs(parsed.query)
        building_id = params.get("building_id", [None])[0]
        start_time = parse_time(params.get("start_time", [None])[0])
        end_time = parse_time(params.get("end_time", [None])[0])
        metric_type = params.get("metric_type", ["electricity"])[0]

        if parsed.path == "/api/buildings":
            self._json({"code": 0, "message": "ok", "data": REPO.query_buildings()})
            return

        if parsed.path == "/api/energy/trend":
            self._json({"code": 0, "message": "ok", "data": REPO.query_trend(building_id, start_time, end_time)})
            return

        if parsed.path == "/api/energy/rank":
            month = params.get("month", [None])[0]
            self._json({"code": 0, "message": "ok", "data": REPO.query_rank(month)})
            return

        if parsed.path == "/api/analysis/summary":
            try:
                data = REPO.query_analysis_summary(building_id, start_time, end_time, metric_type)
            except ValueError as exc:
                self._json({"code": 400, "message": str(exc), "data": None}, HTTPStatus.BAD_REQUEST)
                return
            self._json({"code": 0, "message": "ok", "data": data})
            return

        if parsed.path == "/api/analysis/trend":
            try:
                data = REPO.query_analysis_trend(building_id, start_time, end_time, metric_type)
            except ValueError as exc:
                self._json({"code": 400, "message": str(exc), "data": None}, HTTPStatus.BAD_REQUEST)
                return
            self._json({"code": 0, "message": "ok", "data": data})
            return

        if parsed.path == "/api/analysis/distribution":
            try:
                data = REPO.query_analysis_distribution(building_id, start_time, end_time, metric_type)
            except ValueError as exc:
                self._json({"code": 400, "message": str(exc), "data": None}, HTTPStatus.BAD_REQUEST)
                return
            self._json({"code": 0, "message": "ok", "data": data})
            return

        if parsed.path == "/api/analysis/insights":
            try:
                data = REPO.query_analysis_insights(building_id, start_time, end_time, metric_type)
            except ValueError as exc:
                self._json({"code": 400, "message": str(exc), "data": None}, HTTPStatus.BAD_REQUEST)
                return
            self._json({"code": 0, "message": "ok", "data": data})
            return

        if parsed.path == "/api/analysis/compare":
            try:
                data = REPO.query_analysis_compare(building_id, start_time, end_time, metric_type)
            except ValueError as exc:
                self._json({"code": 400, "message": str(exc), "data": None}, HTTPStatus.BAD_REQUEST)
                return
            self._json({"code": 0, "message": "ok", "data": data})
            return

        if parsed.path == "/api/anomaly/list":
            anomaly_type = params.get("anomaly_type", [None])[0]
            severity = params.get("severity", [None])[0]
            status = params.get("status", [None])[0]
            if status and status not in STATUS_VALUES:
                self._json({"code": 400, "message": "invalid status", "data": None}, HTTPStatus.BAD_REQUEST)
                return
            sort = params.get("sort", ["timestamp_desc"])[0]
            try:
                page = int(params.get("page", ["1"])[0])
            except ValueError:
                page = 1
            try:
                page_size = int(params.get("page_size", ["20"])[0])
            except ValueError:
                page_size = 20

            data = REPO.query_anomalies(building_id, start_time, end_time, anomaly_type, severity, status, page, page_size, sort)
            self._json({"code": 0, "message": "ok", "data": data})
            return

        if parsed.path == "/api/anomaly/history":
            anomaly_id = params.get("anomaly_id", [None])[0]
            if not anomaly_id:
                self._json({"code": 400, "message": "anomaly_id required", "data": None}, HTTPStatus.BAD_REQUEST)
                return
            try:
                data = REPO.query_anomaly_history(int(anomaly_id))
            except ValueError:
                self._json({"code": 400, "message": "invalid anomaly_id", "data": None}, HTTPStatus.BAD_REQUEST)
                return
            if not data:
                self._json({"code": 404, "message": "anomaly not found", "data": None}, HTTPStatus.NOT_FOUND)
                return
            self._json({"code": 0, "message": "ok", "data": data})
            return

        if parsed.path == "/api/anomaly/export":
            anomaly_type = params.get("anomaly_type", [None])[0]
            severity = params.get("severity", [None])[0]
            status = params.get("status", [None])[0]
            sort = params.get("sort", ["timestamp_desc"])[0]
            content = REPO.export_anomalies_csv(building_id, start_time, end_time, anomaly_type, severity, status, sort)
            ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
            self._csv(content, f"a8_anomaly_export_{ts}.csv")
            return

        if parsed.path == "/api/ai/stats":
            try:
                hours = int(params.get("hours", ["24"])[0])
            except ValueError:
                hours = 24
            self._json({"code": 0, "message": "ok", "data": REPO.query_ai_stats(hours)})
            return

        if parsed.path == "/api/system/health":
            self._json({"code": 0, "message": "ok", "data": REPO.query_system_health()})
            return

        if parsed.path == "/api/metrics/overview":
            self._json({"code": 0, "message": "ok", "data": REPO.query_metrics_overview(building_id, start_time, end_time)})
            return

        if parsed.path == "/api/metrics/saving-potential":
            self._json({"code": 0, "message": "ok", "data": REPO.query_saving_potential(building_id, start_time, end_time)})
            return

        if parsed.path == "/api/anomaly/detail":
            anomaly_id = params.get("anomaly_id", [None])[0]
            if not anomaly_id:
                self._json({"code": 400, "message": "anomaly_id required", "data": None}, HTTPStatus.BAD_REQUEST)
                return
            try:
                data = REPO.query_anomaly_detail(int(anomaly_id))
            except ValueError:
                self._json({"code": 400, "message": "invalid anomaly_id", "data": None}, HTTPStatus.BAD_REQUEST)
                return
            if not data:
                self._json({"code": 404, "message": "anomaly not found", "data": None}, HTTPStatus.NOT_FOUND)
                return
            self._json({"code": 0, "message": "ok", "data": data})
            return

        self._json({"code": 404, "message": "not found", "data": None}, HTTPStatus.NOT_FOUND)

    def _serve_static(self, path: str) -> None:
        clean_path = path or "/"
        target = FRONTEND_DIR / "index.html" if clean_path in ("/", "/index.html") else FRONTEND_DIR / clean_path.lstrip("/")
        if not target.exists() or not target.is_file():
            self._json({"code": 404, "message": "not found", "data": None}, HTTPStatus.NOT_FOUND)
            return

        content_type = "text/plain; charset=utf-8"
        if target.suffix == ".html":
            content_type = "text/html; charset=utf-8"
        elif target.suffix == ".css":
            content_type = "text/css; charset=utf-8"
        elif target.suffix == ".js":
            content_type = "application/javascript; charset=utf-8"

        self._set_file_headers(HTTPStatus.OK, content_type)
        self.wfile.write(target.read_bytes())


def run(host: str = "127.0.0.1", port: int = 8000) -> None:
    server = ThreadingHTTPServer((host, port), Handler)
    print(f"A8 server running at http://{host}:{port}")
    print("Press Ctrl+C to stop.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("Shutting down...")
    finally:
        server.server_close()


if __name__ == "__main__":
    run()
