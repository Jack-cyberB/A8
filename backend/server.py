from __future__ import annotations

import csv
import datetime as dt
import json
import math
import os
import re
import socket
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlparse
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]
DEMO_DATA_FILE = ROOT / "data" / "energy_dataset.csv"
NORMALIZED_DATA_FILE = ROOT / "data" / "normalized" / "energy_normalized.csv"
DICT_FILE = ROOT / "data" / "ai_dictionary.json"
KNOWLEDGE_FILE = ROOT / "data" / "normalized" / "knowledge_chunks.jsonl"
RUNTIME_DIR = ROOT / "data" / "runtime"
ACTION_LOG_FILE = RUNTIME_DIR / "anomaly_actions.jsonl"
AI_CALL_LOG_FILE = RUNTIME_DIR / "ai_calls.jsonl"
FRONTEND_DIR = ROOT / "frontend"

TIME_FMT = "%Y-%m-%d %H:%M:%S"
CARBON_FACTOR = 0.785
SEVERITY_SCORE = {"low": 1, "medium": 2, "high": 3}

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
            "你是建筑能源运维诊断助手。必须只输出一个JSON对象，不要输出其他文本。"
            "JSON字段必须包含：conclusion, causes, steps, prevention, recommended_actions, evidence, confidence, risk_level。"
            "其中causes/steps/prevention/recommended_actions为字符串数组，evidence为数组。risk_level只能是low/medium/high。"
        )
        user_prompt = (
            f"异常类型: {anomaly_name}\n"
            f"异常上下文: {json.dumps(context, ensure_ascii=False)}\n"
            f"模板诊断摘要: {diag_template.get('conclusion', '')}\n"
            f"知识证据:\n{evidence_text}\n"
            f"用户问题: {prompt_message}\n"
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
        dict_file: Path,
        knowledge_file: Path,
        action_log_file: Path,
        ai_call_log_file: Path,
    ) -> None:
        self.demo_data_file = demo_data_file
        self.normalized_data_file = normalized_data_file
        self.dict_file = dict_file
        self.knowledge_file = knowledge_file
        self.action_log_file = action_log_file
        self.ai_call_log_file = ai_call_log_file

        self.rows = self._load_rows()
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

        self._prepare_indexes()
        self._ensure_runtime_storage()
        self._load_actions()
        self._load_ai_events()

    def _ensure_runtime_storage(self) -> None:
        self.action_log_file.parent.mkdir(parents=True, exist_ok=True)
        if not self.action_log_file.exists():
            self.action_log_file.write_text("", encoding="utf-8")
        if not self.ai_call_log_file.exists():
            self.ai_call_log_file.write_text("", encoding="utf-8")

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

    def query_trend(self, building_id: str | None, start_time: dt.datetime | None, end_time: dt.datetime | None) -> dict[str, Any]:
        rows = self._filter_rows(building_id, start_time, end_time)
        if not building_id:
            buckets: dict[dt.datetime, float] = {}
            for r in rows:
                buckets.setdefault(r["timestamp"], 0.0)
                buckets[r["timestamp"]] += r["electricity_kwh"]
            series = [{"timestamp": to_iso(ts), "value": round(v, 4)} for ts, v in sorted(buckets.items(), key=lambda x: x[0])]
        else:
            series = [{"timestamp": to_iso(r["timestamp"]), "value": round(r["electricity_kwh"], 4)} for r in sorted(rows, key=lambda x: x["timestamp"])]

        values = [item["value"] for item in series]
        total = sum(values)
        avg = total / len(values) if values else 0
        peak = max(values) if values else 0
        return {
            "unit": "kWh",
            "building_id": building_id or "ALL",
            "point_count": len(series),
            "series": series,
            "summary": {"total_kwh": round(total, 4), "avg_kwh": round(avg, 4), "peak_kwh": round(peak, 4)},
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

    def diagnose(self, payload: dict[str, Any]) -> dict[str, Any]:
        preferred = str(payload.get("provider", "template")).strip().lower()
        if preferred not in {"template", "llm", "auto"}:
            preferred = "template"

        start = time.perf_counter()
        fallback_used = False
        error_message = None

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
        if error_message:
            result["diagnosis"]["degrade_message"] = f"LLM unavailable, fallback to template: {error_message}"

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
        self._append_ai_event(
            {
                "timestamp": to_iso(dt.datetime.now()),
                "requested_provider": preferred,
                "provider": provider_name,
                "fallback_used": fallback_used,
                "latency_ms": latency_ms,
                "error_type": error_type,
                "anomaly_id": result.get("context", {}).get("anomaly_id"),
                "has_message": bool(str(payload.get("message", "")).strip()),
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


REPO = EnergyRepository(
    DEMO_DATA_FILE,
    NORMALIZED_DATA_FILE,
    DICT_FILE,
    KNOWLEDGE_FILE,
    ACTION_LOG_FILE,
    AI_CALL_LOG_FILE,
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

        self._json({"code": 404, "message": "not found", "data": None}, HTTPStatus.NOT_FOUND)

    def _handle_api_get(self, parsed: Any) -> None:
        params = parse_qs(parsed.query)
        building_id = params.get("building_id", [None])[0]
        start_time = parse_time(params.get("start_time", [None])[0])
        end_time = parse_time(params.get("end_time", [None])[0])

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

        if parsed.path == "/api/ai/stats":
            try:
                hours = int(params.get("hours", ["24"])[0])
            except ValueError:
                hours = 24
            self._json({"code": 0, "message": "ok", "data": REPO.query_ai_stats(hours)})
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
