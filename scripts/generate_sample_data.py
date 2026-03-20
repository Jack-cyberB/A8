from __future__ import annotations

import csv
import datetime as dt
import math
import random
from pathlib import Path


OUTPUT_PATH = Path(__file__).resolve().parents[1] / "data" / "energy_dataset.csv"


BUILDINGS = [
    {"building_id": "B001", "building_name": "教学楼", "building_type": "Teaching"},
    {"building_id": "B002", "building_name": "办公楼", "building_type": "Office"},
    {"building_id": "B003", "building_name": "实验楼", "building_type": "Lab"},
]


def base_load(building_type: str, hour: int) -> float:
    daytime = 8 <= hour <= 20
    if building_type == "Office":
        return 95 if daytime else 35
    if building_type == "Lab":
        return 120 if daytime else 55
    return 78 if daytime else 28


def generate_rows() -> list[dict[str, str]]:
    random.seed(42)
    start = dt.datetime(2025, 12, 1, 0, 0, 0)
    hours = 90 * 24
    rows: list[dict[str, str]] = []
    idx = 1

    for b in BUILDINGS:
        for i in range(hours):
            current = start + dt.timedelta(hours=i)
            hour = current.hour
            weekday = current.weekday()
            weekend_factor = 0.88 if weekday >= 5 else 1.0
            seasonal_factor = 1.0 + 0.08 * math.sin((i / 24) * 2 * math.pi / 30)
            peak_factor = 1.2 if 18 <= hour <= 20 else 1.0
            noise = random.uniform(-6.0, 6.0)

            kwh = (
                base_load(b["building_type"], hour)
                * weekend_factor
                * seasonal_factor
                * peak_factor
                + noise
            )

            if b["building_id"] == "B002" and current.day in (5, 18, 25) and hour in (10, 11):
                kwh *= 1.9
            if b["building_id"] == "B001" and current.day in (8, 22) and 9 <= hour <= 12:
                kwh = random.uniform(0.05, 0.4)
            if b["building_id"] == "B003" and current.day in (12, 26) and 14 <= hour <= 18:
                kwh *= 1.65

            if kwh < 0:
                kwh = 0.05

            rows.append(
                {
                    "record_id": str(idx),
                    "building_id": b["building_id"],
                    "building_name": b["building_name"],
                    "building_type": b["building_type"],
                    "timestamp": current.strftime("%Y-%m-%d %H:%M:%S"),
                    "hour": str(hour),
                    "electricity_kwh": f"{kwh:.2f}",
                }
            )
            idx += 1
    return rows


def main() -> None:
    rows = generate_rows()
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "record_id",
                "building_id",
                "building_name",
                "building_type",
                "timestamp",
                "hour",
                "electricity_kwh",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(rows)} rows -> {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
