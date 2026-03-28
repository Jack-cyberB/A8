from __future__ import annotations

import csv
import datetime as dt
import json
import os
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

os.environ["STORAGE_BACKEND"] = "file"

from backend.mysql_support import MySQLClient, sql_literal
from backend.server import (
    ACTION_LOG_FILE,
    AI_CALL_LOG_FILE,
    DICT_FILE,
    FileImportSource,
    KNOWLEDGE_FILE,
    METADATA_FILE,
    NORMALIZED_DATA_FILE,
    NOTE_LOG_FILE,
    REGRESSION_SUMMARY_FILE,
    WEATHER_FILE,
    DEMO_DATA_FILE,
    to_iso,
)


def chunked(items, size):
    bucket = []
    for item in items:
        bucket.append(item)
        if len(bucket) >= size:
            yield bucket
            bucket = []
    if bucket:
        yield bucket


def now_text() -> str:
    return to_iso(dt.datetime.now())


def load_file_repository() -> FileImportSource:
    return FileImportSource(
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


def import_buildings(client: MySQLClient, repo: FileImportSource) -> int:
    rows = []
    now = now_text()
    for meta in repo.bdq2_metadata.values():
        rows.append(
            "("
            + ", ".join(
                [
                    sql_literal(meta.get("building_id")),
                    sql_literal(meta.get("site_id")),
                    sql_literal(meta.get("primaryspaceusage")),
                    sql_literal(meta.get("sub_primaryspaceusage")),
                    sql_literal(meta.get("peer_category")),
                    sql_literal(meta.get("display_category")),
                    sql_literal(meta.get("display_name")),
                    sql_literal(now),
                    sql_literal(now),
                ]
            )
            + ")"
        )
    for batch in chunked(rows, 500):
        sql = f"""
        INSERT INTO buildings (
            building_id, site_id, primaryspaceusage, sub_primaryspaceusage, peer_category,
            display_category, display_name, created_at, updated_at
        ) VALUES {", ".join(batch)}
        ON DUPLICATE KEY UPDATE
            site_id = VALUES(site_id),
            primaryspaceusage = VALUES(primaryspaceusage),
            sub_primaryspaceusage = VALUES(sub_primaryspaceusage),
            peer_category = VALUES(peer_category),
            display_category = VALUES(display_category),
            display_name = VALUES(display_name),
            updated_at = VALUES(updated_at)
        """
        client.execute(sql, timeout_sec=120)
    return len(rows)


def import_energy(client: MySQLClient, repo: FileImportSource) -> int:
    rows = []
    now = now_text()
    for item in repo.rows:
        rows.append(
            "("
            + ", ".join(
                [
                    sql_literal(item.get("record_id")),
                    sql_literal(item.get("building_id")),
                    sql_literal(item.get("building_name")),
                    sql_literal(item.get("building_type")),
                    sql_literal("electricity"),
                    sql_literal(to_iso(item["timestamp"])),
                    sql_literal(item.get("hour")),
                    sql_literal(item.get("electricity_kwh")),
                    sql_literal("kWh"),
                    sql_literal(item.get("source", "normalized")),
                    sql_literal(now),
                    sql_literal(now),
                ]
            )
            + ")"
        )
    for batch in chunked(rows, 500):
        sql = f"""
        INSERT INTO energy_timeseries (
            record_id, building_id, building_name, building_type, metric_type, ts, hour_of_day,
            value, unit, source, created_at, updated_at
        ) VALUES {", ".join(batch)}
        ON DUPLICATE KEY UPDATE
            record_id = VALUES(record_id),
            building_name = VALUES(building_name),
            building_type = VALUES(building_type),
            hour_of_day = VALUES(hour_of_day),
            value = VALUES(value),
            unit = VALUES(unit),
            source = VALUES(source),
            updated_at = VALUES(updated_at)
        """
        client.execute(sql, timeout_sec=120)
    return len(rows)


def import_weather(client: MySQLClient, repo: FileImportSource) -> int:
    rows = []
    now = now_text()
    for site_id, by_time in repo.weather_by_site.items():
        for timestamp, values in by_time.items():
            rows.append(
                "("
                + ", ".join(
                    [
                        sql_literal(site_id),
                        sql_literal(to_iso(timestamp)),
                        sql_literal(values.get("temperature_c", 0.0)),
                        sql_literal(values.get("wind_speed", 0.0)),
                        sql_literal(now),
                        sql_literal(now),
                    ]
                )
                + ")"
            )
    for batch in chunked(rows, 500):
        sql = f"""
        INSERT INTO weather_timeseries (
            site_id, ts, temperature_c, wind_speed, created_at, updated_at
        ) VALUES {", ".join(batch)}
        ON DUPLICATE KEY UPDATE
            temperature_c = VALUES(temperature_c),
            wind_speed = VALUES(wind_speed),
            updated_at = VALUES(updated_at)
        """
        client.execute(sql, timeout_sec=120)
    return len(rows)


def import_peer_energy_daily(client: MySQLClient, repo: FileImportSource) -> int:
    raw_file = repo.raw_electricity_file
    if not raw_file.exists():
        client.execute("TRUNCATE TABLE peer_energy_daily")
        return 0

    candidate_ids = [
        building_id
        for building_id, meta in repo.bdq2_metadata.items()
        if str(meta.get("peer_category", "")).strip()
    ]
    if not candidate_ids:
        client.execute("TRUNCATE TABLE peer_energy_daily")
        return 0

    daily_totals = {}
    with raw_file.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        header = next(reader, [])
        indexed_candidates = [
            (idx, building_id)
            for idx, building_id in enumerate(header[1:], start=1)
            if building_id in candidate_ids
        ]
        for row in reader:
            if not row:
                continue
            day = str(row[0]).strip()[:10]
            if not day:
                continue
            for idx, building_id in indexed_candidates:
                if idx >= len(row):
                    continue
                raw_val = str(row[idx]).strip()
                if not raw_val:
                    continue
                try:
                    value = float(raw_val)
                except ValueError:
                    continue
                if value < 0:
                    continue
                key = (building_id, day)
                total, count = daily_totals.get(key, (0.0, 0))
                daily_totals[key] = (total + value, count + 1)

    client.execute("TRUNCATE TABLE peer_energy_daily")
    now = now_text()
    rows = []
    for (building_id, day), (total_value, sample_count) in daily_totals.items():
        if sample_count <= 0:
            continue
        avg_value = total_value / sample_count
        rows.append(
            "("
            + ", ".join(
                [
                    sql_literal("electricity"),
                    sql_literal(building_id),
                    sql_literal(day),
                    sql_literal(round(total_value, 6)),
                    sql_literal(sample_count),
                    sql_literal(round(avg_value, 6)),
                    sql_literal(now),
                    sql_literal(now),
                ]
            )
            + ")"
        )

    for batch in chunked(rows, 500):
        sql = f"""
        INSERT INTO peer_energy_daily (
            metric_type, building_id, day, total_value, sample_count, avg_value, created_at, updated_at
        ) VALUES {", ".join(batch)}
        """
        client.execute(sql, timeout_sec=120)
    return len(rows)


def import_anomaly_actions(client: MySQLClient, repo: FileImportSource) -> int:
    rows = []
    for event in repo.action_events:
        rows.append(
            "("
            + ", ".join(
                [
                    sql_literal(event.get("anomaly_id")),
                    sql_literal(event.get("action")),
                    sql_literal(event.get("status_before")),
                    sql_literal(event.get("status")),
                    sql_literal(event.get("assignee")),
                    sql_literal(event.get("note")),
                    sql_literal(event.get("created_at")),
                ]
            )
            + ")"
        )
    if rows:
        client.execute("TRUNCATE TABLE anomaly_actions")
    for batch in chunked(rows, 500):
        sql = f"""
        INSERT INTO anomaly_actions (
            anomaly_id, action_name, status_before, status_after, assignee, note, created_at
        ) VALUES {", ".join(batch)}
        """
        client.execute(sql, timeout_sec=120)
    return len(rows)


def import_anomaly_notes(client: MySQLClient, repo: FileImportSource) -> int:
    rows = []
    for event in repo.note_events:
        rows.append(
            "("
            + ", ".join(
                [
                    sql_literal(event.get("anomaly_id")),
                    sql_literal(event.get("cause_confirmed")),
                    sql_literal(event.get("action_taken")),
                    sql_literal(event.get("result_summary")),
                    sql_literal(event.get("recurrence_risk")),
                    sql_literal(event.get("reviewer")),
                    sql_literal(event.get("updated_at")),
                ]
            )
            + ")"
        )
    if rows:
        client.execute("TRUNCATE TABLE anomaly_notes")
    for batch in chunked(rows, 500):
        sql = f"""
        INSERT INTO anomaly_notes (
            anomaly_id, cause_confirmed, action_taken, result_summary, recurrence_risk, reviewer, updated_at
        ) VALUES {", ".join(batch)}
        """
        client.execute(sql, timeout_sec=120)
    return len(rows)


def import_ai_calls(client: MySQLClient, repo: FileImportSource) -> int:
    rows = []
    now = now_text()
    for event in repo.ai_events:
        timestamp = str(event.get("timestamp", "")).strip() or now
        rows.append(
            "("
            + ", ".join(
                [
                    sql_literal(event.get("trace_id")),
                    sql_literal(timestamp),
                    sql_literal(event.get("requested_provider")),
                    sql_literal(event.get("provider")),
                    sql_literal(event.get("event_type")),
                    sql_literal(event.get("building_id")),
                    sql_literal(event.get("anomaly_id")),
                    "1" if bool(event.get("has_message", False)) else "0",
                    sql_literal(event.get("result_risk_level")),
                    sql_literal(event.get("knowledge_source")),
                    sql_literal(int(event.get("retrieval_hit_count", 0) or 0)),
                    sql_literal(event.get("retrieval_error_type")),
                    "1" if bool(event.get("fallback_used", False)) else "0",
                    "1" if bool(event.get("field_complete", False)) else "0",
                    sql_literal(int(event.get("latency_ms", 0) or 0)),
                    "0" if str(event.get("error_type", "")).strip() else "1",
                    sql_literal(event.get("error_type")),
                    sql_literal(event.get("feedback_label")),
                    sql_literal(timestamp),
                    sql_literal(timestamp),
                ]
            )
            + ")"
        )
    if rows:
        client.execute("TRUNCATE TABLE ai_calls")
    for batch in chunked(rows, 300):
        sql = f"""
        INSERT INTO ai_calls (
            trace_id, event_time, requested_provider, provider, scene, building_id, anomaly_id,
            has_message, result_risk_level, knowledge_source, retrieval_hit_count, retrieval_error_type,
            fallback_used, field_complete, latency_ms, success, error_type, feedback_label, created_at, updated_at
        ) VALUES {", ".join(batch)}
        """
        client.execute(sql, timeout_sec=120)
    return len(rows)


def import_regression_summary(client: MySQLClient) -> bool:
    if not REGRESSION_SUMMARY_FILE.exists():
        return False
    payload = json.loads(REGRESSION_SUMMARY_FILE.read_text(encoding="utf-8"))
    now = now_text()
    sql = f"""
    INSERT INTO system_snapshots (snapshot_key, payload_json, updated_at)
    VALUES ('regression_summary', {sql_literal(json.dumps(payload, ensure_ascii=False))}, {sql_literal(now)})
    ON DUPLICATE KEY UPDATE
        payload_json = VALUES(payload_json),
        updated_at = VALUES(updated_at)
    """
    client.execute(sql)
    return True


def main() -> None:
    client = MySQLClient.from_env()
    client.ensure_schema()
    repo = load_file_repository()

    stats = {
        "buildings": import_buildings(client, repo),
        "energy_timeseries": import_energy(client, repo),
        "weather_timeseries": import_weather(client, repo),
        "peer_energy_daily": import_peer_energy_daily(client, repo),
        "anomaly_actions": import_anomaly_actions(client, repo),
        "anomaly_notes": import_anomaly_notes(client, repo),
        "ai_calls": import_ai_calls(client, repo),
        "regression_summary": import_regression_summary(client),
    }

    print(json.dumps(stats, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
