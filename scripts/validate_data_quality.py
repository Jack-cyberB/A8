from __future__ import annotations

import csv
import json
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
NORMALIZED = ROOT / "data" / "normalized"

BDG2_FILES = {
    "metadata": RAW / "bdg2" / "data" / "metadata" / "metadata.csv",
    "weather": RAW / "bdg2" / "data" / "weather" / "weather.csv",
    "electricity": RAW / "bdg2" / "data" / "meters" / "cleaned" / "electricity_cleaned.csv",
}
SIKONG_DIR = RAW / "sikong"
ENERGY_NORMALIZED = NORMALIZED / "energy_normalized.csv"
REPORT = NORMALIZED / "data_quality_report.json"

TS_FMT = "%Y-%m-%d %H:%M:%S"
REQUIRED_COLUMNS = [
    "building_id",
    "building_name",
    "building_type",
    "timestamp",
    "electricity_kwh",
    "source",
]


def check_file_exists(path: Path, min_size: int = 1_000) -> dict:
    exists = path.exists() and path.is_file()
    size = path.stat().st_size if exists else 0
    return {
        "path": str(path),
        "exists": exists,
        "size_bytes": size,
        "is_real_data": size >= min_size,
    }


def check_sikong_jsons(path: Path) -> dict:
    json_files = sorted(path.glob("*.json")) if path.exists() else []
    return {
        "path": str(path),
        "json_count": len(json_files),
        "sample_files": [p.name for p in json_files[:6]],
        "meets_minimum": len(json_files) >= 20,
    }


def check_energy_normalized(path: Path) -> tuple[dict, bool]:
    summary = {
        "path": str(path),
        "exists": path.exists(),
        "rows": 0,
        "columns": [],
        "missing_required_columns": [],
        "invalid_rows": 0,
        "duplicate_building_timestamp": 0,
        "unique_buildings": 0,
        "sources": [],
        "continuity": {
            "checked_buildings": 0,
            "max_gap_hours": 0,
            "gap_gt_3h_count": 0,
        },
    }
    if not path.exists():
        return summary, False

    duplicates = 0
    invalid_rows = 0
    seen_keys: set[tuple[str, str]] = set()
    by_building_ts: dict[str, list[datetime]] = defaultdict(list)
    sources: set[str] = set()

    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        summary["columns"] = list(reader.fieldnames or [])
        summary["missing_required_columns"] = [c for c in REQUIRED_COLUMNS if c not in summary["columns"]]
        if summary["missing_required_columns"]:
            return summary, False

        for row in reader:
            summary["rows"] += 1
            bid = str(row.get("building_id", "")).strip()
            bname = str(row.get("building_name", "")).strip()
            btype = str(row.get("building_type", "")).strip()
            ts_raw = str(row.get("timestamp", "")).strip()
            source = str(row.get("source", "")).strip()
            val = str(row.get("electricity_kwh", "")).strip()

            if not (bid and bname and btype and ts_raw and source and val):
                invalid_rows += 1
                continue
            try:
                float(val)
                ts = datetime.strptime(ts_raw, TS_FMT)
            except ValueError:
                invalid_rows += 1
                continue

            key = (bid, ts_raw)
            if key in seen_keys:
                duplicates += 1
            else:
                seen_keys.add(key)

            by_building_ts[bid].append(ts)
            sources.add(source)

    max_gap = 0.0
    gap_gt_3h = 0
    for tss in by_building_ts.values():
        tss.sort()
        for i in range(1, len(tss)):
            gap_hours = (tss[i] - tss[i - 1]).total_seconds() / 3600
            if gap_hours > max_gap:
                max_gap = gap_hours
            if gap_hours > 3:
                gap_gt_3h += 1

    summary["invalid_rows"] = invalid_rows
    summary["duplicate_building_timestamp"] = duplicates
    summary["unique_buildings"] = len(by_building_ts)
    summary["sources"] = sorted(sources)
    summary["continuity"] = {
        "checked_buildings": len(by_building_ts),
        "max_gap_hours": round(max_gap, 4),
        "gap_gt_3h_count": gap_gt_3h,
    }

    critical_ok = (
        summary["rows"] > 0
        and invalid_rows == 0
        and duplicates == 0
        and summary["unique_buildings"] > 0
        and len(sources) > 0
    )
    return summary, critical_ok


def main() -> int:
    NORMALIZED.mkdir(parents=True, exist_ok=True)

    bdg2_checks = {name: check_file_exists(path, min_size=50_000) for name, path in BDG2_FILES.items()}
    sikong_check = check_sikong_jsons(SIKONG_DIR)
    energy_check, energy_ok = check_energy_normalized(ENERGY_NORMALIZED)

    hard_failures: list[str] = []

    for name, item in bdg2_checks.items():
        if not item["exists"]:
            hard_failures.append(f"missing_bdg2_{name}")
        elif not item["is_real_data"]:
            hard_failures.append(f"bdg2_{name}_not_real_data")

    if not sikong_check["meets_minimum"]:
        hard_failures.append("sikong_json_count_below_20")

    if not energy_ok:
        hard_failures.append("energy_normalized_traceability_check_failed")

    report = {
        "status": "pass" if not hard_failures else "fail",
        "checked_at": datetime.now().strftime(TS_FMT),
        "bdg2": bdg2_checks,
        "sikong": sikong_check,
        "energy_normalized": energy_check,
        "hard_failures": hard_failures,
    }

    REPORT.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))

    return 0 if not hard_failures else 1


if __name__ == "__main__":
    sys.exit(main())
