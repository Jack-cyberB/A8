from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

TESTS = [
    "backend.tests.test_repository.RepositoryTests.test_buildings",
    "backend.tests.test_repository.RepositoryTests.test_anomaly_action_flow",
    "backend.tests.test_repository.RepositoryTests.test_anomaly_list_has_status_fields",
    "backend.tests.test_repository.RepositoryTests.test_anomaly_detail_has_processing_summary",
    "backend.tests.test_repository.RepositoryTests.test_diagnose_template_shape",
    "backend.tests.test_repository.RepositoryTests.test_diagnose_llm_fallback",
    "backend.tests.test_repository.RepositoryTests.test_diagnose_llm_success_mock",
    "backend.tests.test_repository.RepositoryTests.test_ai_stats_shape",
    "backend.tests.test_repository.RepositoryTests.test_anomaly_note_upsert_and_detail",
    "backend.tests.test_repository.RepositoryTests.test_export_csv_and_health",
    "backend.tests.test_repository.RepositoryTests.test_ai_evaluate_and_feedback",
]


def main() -> int:
    py = sys.executable
    all_ok = True
    for test in TESTS:
        print(f"[RUN] {test}")
        proc = subprocess.run([py, "-m", "unittest", test, "-v"], cwd=str(ROOT), capture_output=True, text=True, encoding="utf-8", errors="replace")
        if proc.stdout:
            print(proc.stdout.strip())
        if proc.stderr:
            print(proc.stderr.strip())
        ok = proc.returncode == 0
        print(f"[{'PASS' if ok else 'FAIL'}] {test}\n")
        if not ok:
            all_ok = False
            break
    return 0 if all_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
