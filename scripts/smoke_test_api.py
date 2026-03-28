import json
import subprocess
import sys
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
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


def get_text(url: str, timeout: int = 20) -> str:
    req = Request(url, method="GET")
    with urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8-sig")


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
    building_id = buildings["data"]["items"][0]["building_id"]

    detail = get_json(f"{SERVER_URL}/api/anomaly/detail?anomaly_id={aid}")
    analysis_summary = get_json(f"{SERVER_URL}/api/analysis/summary?building_id={building_id}&metric_type=electricity")
    analysis_trend = get_json(f"{SERVER_URL}/api/analysis/trend?building_id={building_id}&metric_type=electricity")
    analysis_distribution = get_json(f"{SERVER_URL}/api/analysis/distribution?building_id={building_id}&metric_type=electricity")
    analysis_compare = get_json(f"{SERVER_URL}/api/analysis/compare?building_id={building_id}&metric_type=electricity")

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
    system_health = get_json(f"{SERVER_URL}/api/system/health")
    exported = get_text(f"{SERVER_URL}/api/anomaly/export?status=acknowledged")

    diagnose_template = get_json(
        f"{SERVER_URL}/api/ai/diagnose",
        method="POST",
        payload={"message": "请分析突增异常", "anomaly_id": aid, "provider": "template"},
    )
    diagnose_failure_status = None
    diagnose_failure_body = ""
    try:
        _ = get_json(
            f"{SERVER_URL}/api/ai/diagnose",
            method="POST",
            payload={
                "message": "请分析突增异常",
                "anomaly_id": aid,
                "provider": "llm",
                "simulate_llm_failure": True,
            },
        )
    except HTTPError as exc:
        diagnose_failure_status = exc.code
        diagnose_failure_body = exc.read().decode("utf-8", errors="ignore")
    trace_id = diagnose_template["data"]["diagnosis"]["trace_id"]
    feedback = get_json(
        f"{SERVER_URL}/api/ai/feedback",
        method="POST",
        payload={"trace_id": trace_id, "label": "useful"},
    )
    evaluate = get_json(
        f"{SERVER_URL}/api/ai/evaluate",
        method="POST",
        payload={"hours": 24},
    )
    analysis_report = get_json(
        f"{SERVER_URL}/api/ai/analyze",
        method="POST",
        payload={"provider": "template", "building_id": building_id, "metric_type": "electricity"},
    )
    note = get_json(
        f"{SERVER_URL}/api/anomaly/note",
        method="POST",
        payload={
            "anomaly_id": aid,
            "cause_confirmed": "烟测",
            "action_taken": "巡检",
            "result_summary": "恢复",
            "recurrence_risk": "low",
            "reviewer": "smoke",
        },
    )

    assert buildings["code"] == 0 and buildings["data"]["count"] > 0
    assert anomaly["code"] == 0 and anomaly["data"]["count"] > 0
    assert detail["code"] == 0 and "processing_summary" in detail["data"]
    assert analysis_summary["code"] == 0 and analysis_summary["data"]["metric_type"] == "electricity"
    assert analysis_trend["code"] == 0 and len(analysis_trend["data"]["series"]) > 0
    assert analysis_distribution["code"] == 0 and len(analysis_distribution["data"]["hourly_profile"]) == 24
    assert analysis_compare["code"] == 0 and len(analysis_compare["data"]["items"]) == 2
    assert history["code"] == 0 and history["data"]["count"] >= 1
    assert ai_stats["code"] == 0 and "total_calls" in ai_stats["data"]
    assert system_health["code"] == 0 and "recent_regression" in system_health["data"]
    assert system_health["data"]["storage"]["backend"] == "mysql"
    assert system_health["data"]["storage"]["mysql"]["connected"] is True
    assert system_health["data"]["storage"]["mysql"]["database"] == "a8"
    assert "ragflow" in system_health["data"]
    assert "anomaly_id,building_id,building_name" in exported
    assert diagnose_template["code"] == 0 and not diagnose_template["data"]["diagnosis"]["fallback_used"]
    assert diagnose_template["data"]["diagnosis"]["knowledge_source"] in {"ragflow", "local", "none"}
    assert all("source_type" in item for item in diagnose_template["data"]["diagnosis"]["evidence"])
    assert diagnose_failure_status == 502
    assert "Simulated llm failure" in diagnose_failure_body
    assert feedback["code"] == 0 and feedback["data"]["label"] == "useful"
    assert evaluate["code"] == 0 and "llm" in evaluate["data"]
    assert analysis_report["code"] == 0 and "findings" in analysis_report["data"]["analysis"]
    assert analysis_report["data"]["analysis"]["knowledge_source"] in {"ragflow", "local", "none"}
    assert note["code"] == 0 and note["data"]["recurrence_risk"] == "low"
    print("API smoke test passed")
finally:
    proc.terminate()
    try:
        proc.wait(timeout=3)
    except subprocess.TimeoutExpired:
        proc.kill()
