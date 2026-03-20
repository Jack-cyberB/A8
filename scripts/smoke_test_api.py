import json
import subprocess
import sys
import time
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
SERVER_URL = "http://127.0.0.1:8011"
server_cmd = [sys.executable, "-c", "from backend.server import run; run(port=8011)"]
proc = subprocess.Popen(server_cmd, cwd=str(ROOT), stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def get_json(url: str, method: str = "GET", payload: dict | None = None, timeout: int = 15):
    data = None
    headers = {}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = Request(url, data=data, method=method, headers=headers)
    with urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


try:
    ready = False
    for _ in range(30):
        try:
            _ = get_json(f"{SERVER_URL}/api/buildings")
            ready = True
            break
        except URLError:
            time.sleep(1.0)

    if not ready:
        raise RuntimeError("Server did not become ready within 30 seconds")

    buildings = get_json(f"{SERVER_URL}/api/buildings")
    anomaly = get_json(f"{SERVER_URL}/api/anomaly/list?page=1&page_size=20&sort=severity_desc")
    aid = anomaly["data"]["items"][0]["anomaly_id"]

    detail = get_json(f"{SERVER_URL}/api/anomaly/detail?anomaly_id={aid}")

    new_list = get_json(f"{SERVER_URL}/api/anomaly/list?status=new&page=1&page_size=5")
    if new_list["data"]["count"] > 0:
        target_id = new_list["data"]["items"][0]["anomaly_id"]
        _ = get_json(
            f"{SERVER_URL}/api/anomaly/action",
            method="POST",
            payload={"anomaly_id": target_id, "action": "ack", "assignee": "smoke", "note": "smoke ack"},
        )
    else:
        ack_list = get_json(f"{SERVER_URL}/api/anomaly/list?status=acknowledged&page=1&page_size=5")
        target_id = ack_list["data"]["items"][0]["anomaly_id"]

    history = get_json(f"{SERVER_URL}/api/anomaly/history?anomaly_id={target_id}")
    ai_stats = get_json(f"{SERVER_URL}/api/ai/stats?hours=24")

    diagnose_template = get_json(
        f"{SERVER_URL}/api/ai/diagnose",
        method="POST",
        payload={"message": "请分析突增异常", "anomaly_id": aid, "provider": "template"},
    )
    diagnose_fallback = get_json(
        f"{SERVER_URL}/api/ai/diagnose",
        method="POST",
        payload={
            "message": "请分析突增异常",
            "anomaly_id": aid,
            "provider": "llm",
            "simulate_llm_failure": True,
        },
    )

    assert buildings["code"] == 0 and buildings["data"]["count"] > 0
    assert anomaly["code"] == 0 and anomaly["data"]["count"] > 0
    assert detail["code"] == 0 and "processing_summary" in detail["data"]
    assert history["code"] == 0 and history["data"]["count"] >= 1
    assert ai_stats["code"] == 0 and "total_calls" in ai_stats["data"]
    assert diagnose_template["code"] == 0 and not diagnose_template["data"]["diagnosis"]["fallback_used"]
    assert diagnose_fallback["code"] == 0 and diagnose_fallback["data"]["diagnosis"]["fallback_used"]
    print("API smoke test passed")
finally:
    proc.terminate()
    try:
        proc.wait(timeout=3)
    except subprocess.TimeoutExpired:
        proc.kill()
