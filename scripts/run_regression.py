from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def npm_cmd() -> str:
    return "npm.cmd" if sys.platform.startswith("win") else "npm"


def safe_console_text(text: str) -> str:
    enc = sys.stdout.encoding or "utf-8"
    return text.encode(enc, errors="replace").decode(enc, errors="replace")


def run_step(name: str, command: list[str]) -> tuple[bool, str]:
    print(f"[RUN] {name}: {' '.join(command)}")
    proc = subprocess.run(command, cwd=str(ROOT), capture_output=True, text=True, encoding="utf-8", errors="replace")
    if proc.stdout:
        print(safe_console_text(proc.stdout.strip()))
    if proc.stderr:
        print(safe_console_text(proc.stderr.strip()))
    ok = proc.returncode == 0
    print(f"[{'PASS' if ok else 'FAIL'}] {name}\\n")
    return ok, name


def main() -> int:
    py = sys.executable
    steps = [
        ("data quality", [py, "scripts/validate_data_quality.py"]),
        ("backend unit tests", [py, "-m", "unittest", "backend.tests.test_repository", "-v"]),
        ("api smoke", [py, "scripts/smoke_test_api.py"]),
        ("playwright e2e", [npm_cmd(), "run", "test:e2e"]),
    ]

    results = [run_step(name, cmd) for name, cmd in steps]
    all_ok = all(ok for ok, _ in results)
    print("=== Regression Summary ===")
    for ok, name in results:
        print(f"- {name}: {'PASS' if ok else 'FAIL'}")

    return 0 if all_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
