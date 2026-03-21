import json
import os
import subprocess
import sys
import time
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
SERVER_URL = "http://127.0.0.1:8012"
server_cmd = [sys.executable, "-c", "from backend.server import run; run(port=8012)"]
proc = subprocess.Popen(server_cmd, cwd=str(ROOT), stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def get_json(url: str, method: str = "GET", payload: dict | None = None, timeout: int = 60):
    data = None
    headers = {}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = Request(url, data=data, method=method, headers=headers)
    with urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


try:
    from backend.server import load_local_env

    load_local_env()
    if not os.getenv("OPENAI_API_KEY"):
        raise RuntimeError("OPENAI_API_KEY is required for live LLM smoke test")

    ready = False
    for _ in range(30):
        try:
            _ = get_json(f"{SERVER_URL}/api/buildings", timeout=10)
            ready = True
            break
        except URLError:
            time.sleep(1)

    if not ready:
        raise RuntimeError("Server did not become ready within 30 seconds")

    anomaly = get_json(f"{SERVER_URL}/api/anomaly/list?page=1&page_size=1&sort=timestamp_desc", timeout=10)
    aid = anomaly["data"]["items"][0]["anomaly_id"]

    result = get_json(
        f"{SERVER_URL}/api/ai/diagnose",
        method="POST",
        timeout=90,
        payload={"message": "请给出可执行诊断建议", "anomaly_id": aid, "provider": "llm"},
    )
    diag = result["data"]["diagnosis"]

    assert result["code"] == 0
    assert "conclusion" in diag and diag["conclusion"]
    assert "fallback_used" in diag
    print(
        json.dumps(
            {
                "status": "pass",
                "provider": diag.get("provider"),
                "fallback_used": diag.get("fallback_used"),
                "latency_ms": diag.get("latency_ms"),
                "risk_level": diag.get("risk_level"),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
finally:
    proc.terminate()
    try:
        proc.wait(timeout=3)
    except subprocess.TimeoutExpired:
        proc.kill()
