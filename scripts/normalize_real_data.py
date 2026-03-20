from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BDG2_ELEC = ROOT / "data" / "raw" / "bdg2" / "data" / "meters" / "cleaned" / "electricity_cleaned.csv"
BDG2_META = ROOT / "data" / "raw" / "bdg2" / "data" / "metadata" / "metadata.csv"
SIKONG_DIR = ROOT / "data" / "raw" / "sikong"
OUT_DIR = ROOT / "data" / "normalized"
OUT_ENERGY = OUT_DIR / "energy_normalized.csv"
OUT_KNOWLEDGE = OUT_DIR / "knowledge_chunks.jsonl"


def load_bdg2_meta(path: Path) -> dict[str, dict[str, str]]:
    meta: dict[str, dict[str, str]] = {}
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            bid = row.get("building_id", "").strip()
            if not bid:
                continue
            meta[bid] = {
                "building_name": bid,
                "building_type": row.get("primaryspaceusage", "unknown").strip() or "unknown",
            }
    return meta


def choose_buildings(electricity_file: Path, meta: dict[str, dict[str, str]], max_buildings: int) -> list[str]:
    with electricity_file.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        headers = next(reader)
    cols = [c for c in headers[1:] if c in meta]
    return cols[:max_buildings]


def normalize_energy(
    electricity_file: Path,
    meta: dict[str, dict[str, str]],
    building_ids: list[str],
    out_file: Path,
) -> tuple[int, int]:
    out_file.parent.mkdir(parents=True, exist_ok=True)
    output_rows = 0
    input_rows = 0

    with electricity_file.open("r", encoding="utf-8", newline="") as src, out_file.open(
        "w", encoding="utf-8", newline=""
    ) as dst:
        reader = csv.DictReader(src)
        writer = csv.DictWriter(
            dst,
            fieldnames=[
                "building_id",
                "building_name",
                "building_type",
                "timestamp",
                "electricity_kwh",
                "source",
            ],
        )
        writer.writeheader()

        for row in reader:
            input_rows += 1
            ts = row.get("timestamp", "").strip()
            if not ts:
                continue
            for bid in building_ids:
                raw_val = row.get(bid, "")
                if raw_val is None:
                    continue
                raw_val = raw_val.strip()
                if not raw_val:
                    continue
                try:
                    value = float(raw_val)
                except ValueError:
                    continue
                if value < 0:
                    continue
                writer.writerow(
                    {
                        "building_id": bid,
                        "building_name": meta[bid]["building_name"],
                        "building_type": meta[bid]["building_type"],
                        "timestamp": ts,
                        "electricity_kwh": f"{value:.4f}",
                        "source": "bdg2_cleaned_electricity",
                    }
                )
                output_rows += 1
    return input_rows, output_rows


def normalize_sikong_knowledge(sikong_dir: Path, out_file: Path) -> int:
    out_file.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    chunk_id = 1
    json_files = sorted(sikong_dir.glob("*.json"))

    with out_file.open("w", encoding="utf-8") as f:
        for file_path in json_files:
            try:
                data = json.loads(file_path.read_text(encoding="utf-8"))
            except Exception:
                continue

            title = file_path.stem
            instances = []
            if isinstance(data, dict):
                if isinstance(data.get("instance"), list):
                    instances = data.get("instance", [])
                elif isinstance(data.get("instances"), list):
                    instances = data.get("instances", [])
            if not isinstance(instances, list):
                continue

            for item in instances:
                if not isinstance(item, dict):
                    continue
                question = str(item.get("input", "")).strip()
                answer = str(item.get("output", "")).strip()
                if not question and not answer:
                    continue
                text = f"Q: {question}\nA: {answer}".strip()
                record = {
                    "chunk_id": f"sikong-{chunk_id}",
                    "source": "sikong",
                    "title": title,
                    "question": question,
                    "answer": answer,
                    "text": text,
                }
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
                chunk_id += 1
                count += 1
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="Normalize BDG2 and Sikong data for A8 project.")
    parser.add_argument("--max-buildings", type=int, default=30, help="Number of BDG2 building columns to include.")
    args = parser.parse_args()

    if not BDG2_ELEC.exists():
        raise FileNotFoundError(f"Missing file: {BDG2_ELEC}")
    if not BDG2_META.exists():
        raise FileNotFoundError(f"Missing file: {BDG2_META}")
    if not SIKONG_DIR.exists():
        raise FileNotFoundError(f"Missing dir: {SIKONG_DIR}")

    meta = load_bdg2_meta(BDG2_META)
    selected_buildings = choose_buildings(BDG2_ELEC, meta, max_buildings=args.max_buildings)
    if not selected_buildings:
        raise RuntimeError("No matching building columns found in electricity file.")

    input_rows, output_rows = normalize_energy(BDG2_ELEC, meta, selected_buildings, OUT_ENERGY)
    chunk_count = normalize_sikong_knowledge(SIKONG_DIR, OUT_KNOWLEDGE)

    summary = {
        "selected_buildings": len(selected_buildings),
        "input_timeseries_rows": input_rows,
        "normalized_energy_rows": output_rows,
        "knowledge_chunks": chunk_count,
        "energy_file": str(OUT_ENERGY),
        "knowledge_file": str(OUT_KNOWLEDGE),
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
