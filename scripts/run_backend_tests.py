from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

TESTS = [
    "backend.tests.test_repository.RepositoryTests.test_buildings",
    "backend.tests.test_repository.RepositoryTests.test_create_repository_defaults_to_mysql",
    "backend.tests.test_repository.RepositoryTests.test_create_repository_uses_file_backend_when_requested",
    "backend.tests.test_repository.RepositoryTests.test_anomaly_action_flow",
    "backend.tests.test_repository.RepositoryTests.test_anomaly_list_has_status_fields",
    "backend.tests.test_repository.RepositoryTests.test_anomaly_detail_has_processing_summary",
    "backend.tests.test_repository.RepositoryTests.test_diagnose_template_shape",
    "backend.tests.test_repository.RepositoryTests.test_diagnose_llm_failure_raises",
    "backend.tests.test_repository.RepositoryTests.test_diagnose_llm_success_mock",
    "backend.tests.test_repository.RepositoryTests.test_ai_stats_shape",
    "backend.tests.test_repository.RepositoryTests.test_anomaly_note_upsert_and_detail",
    "backend.tests.test_repository.RepositoryTests.test_export_csv_and_health",
    "backend.tests.test_repository.RepositoryTests.test_ai_evaluate_and_feedback",
    "backend.tests.test_repository.RepositoryTests.test_clean_ragflow_answer_text_removes_inline_citations",
    "backend.tests.test_repository.RepositoryTests.test_knowledge_route_for_question_distinguishes_standard_and_scene",
    "backend.tests.test_repository.RepositoryTests.test_merge_ragflow_stream_text_supports_incremental_and_cumulative_chunks",
    "backend.tests.test_repository.RepositoryTests.test_ragflow_chat_proxy_uses_native_chat_completion",
    "backend.tests.test_repository.RepositoryTests.test_ragflow_chat_stream_events_use_native_stream_and_preserve_full_answer",
    "backend.tests.test_repository.MySQLDriverIntegrationTests.test_mysql_client_health_reports_connected",
    "backend.tests.test_repository.MySQLDriverIntegrationTests.test_mysql_repository_roundtrip_for_storage_tables",
]

def _safe_print(text: str) -> None:
    if not text:
        return
    stream = sys.stdout
    encoding = getattr(stream, "encoding", None) or "utf-8"
    safe = text.encode(encoding, errors="replace").decode(encoding, errors="replace")
    print(safe)


def main() -> int:
    py = sys.executable
    all_ok = True
    for test in TESTS:
        print(f"[RUN] {test}")
        proc = subprocess.run([py, "-m", "unittest", test, "-v"], cwd=str(ROOT), capture_output=True, text=True, encoding="utf-8", errors="replace")
        if proc.stdout:
            _safe_print(proc.stdout.strip())
        if proc.stderr:
            _safe_print(proc.stderr.strip())
        ok = proc.returncode == 0
        print(f"[{'PASS' if ok else 'FAIL'}] {test}\n")
        if not ok:
            all_ok = False
            break
    return 0 if all_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
