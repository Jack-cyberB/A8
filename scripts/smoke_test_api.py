import json
import subprocess
import sys
import time
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
server_cmd = [sys.executable, str(ROOT / "backend" / "server.py")]
proc = subprocess.Popen(server_cmd, cwd=str(ROOT), stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def get_json(url: str, method: str = "GET", payload: dict | None = None):
    data = None
    headers = {}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = Request(url, data=data, method=method, headers=headers)
    with urlopen(req, timeout=10) as r:
        return json.loads(r.read().decode("utf-8"))


try:
    ready = False
    for _ in range(30):
        try:
            _ = get_json("http://127.0.0.1:8000/api/buildings")
            ready = True
            break
        except URLError:
            time.sleep(1.0)

    if not ready:
        raise RuntimeError("Server did not become ready within 30 seconds")

    buildings = get_json("http://127.0.0.1:8000/api/buildings")
    trend = get_json("http://127.0.0.1:8000/api/energy/trend")
    rank = get_json("http://127.0.0.1:8000/api/energy/rank")
    anomaly = get_json("http://127.0.0.1:8000/api/anomaly/list?page=1&page_size=20&sort=severity_desc")
    detail = get_json(f"http://127.0.0.1:8000/api/anomaly/detail?anomaly_id={anomaly['data']['items'][0]['anomaly_id']}")
    overview = get_json("http://127.0.0.1:8000/api/metrics/overview")
    diagnose = get_json(
        "http://127.0.0.1:8000/api/ai/diagnose",
        method="POST",
        payload={"message": "请分析突增异常", "anomaly_id": anomaly["data"]["items"][0]["anomaly_id"]},
    )

    assert buildings["code"] == 0 and buildings["data"]["count"] > 0
    assert trend["code"] == 0 and trend["data"]["point_count"] > 0
    assert rank["code"] == 0 and len(rank["data"]["items"]) >= 1
    assert anomaly["code"] == 0 and anomaly["data"]["count"] > 0
    assert detail["code"] == 0 and "baseline_window" in detail["data"]
    assert overview["code"] == 0 and overview["data"]["total_kwh"] > 0
    assert diagnose["code"] == 0 and "risk_level" in diagnose["data"]["diagnosis"]
    print("API smoke test passed")
finally:
    proc.terminate()
    try:
        proc.wait(timeout=3)
    except subprocess.TimeoutExpired:
        proc.kill()
