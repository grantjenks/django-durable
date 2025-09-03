import argparse
import json
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parents[1]
MANAGE = str(ROOT / "manage.py")
DB_PATH = str(ROOT / "db.sqlite3")


def run_manage(*args: str) -> str:
    cmd = [sys.executable, MANAGE, *args]
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        raise RuntimeError(
            f"Command failed: {' '.join(cmd)}\nSTDOUT:\n{res.stdout}\nSTDERR:\n{res.stderr}"
        )
    return res.stdout.strip()


def read_workflow(exec_id: str) -> Dict[str, Any]:
    con = sqlite3.connect(DB_PATH)
    try:
        cur = con.cursor()
        norm_id = exec_id.replace('-', '')
        cur.execute(
            "SELECT status, result FROM django_durable_workflowexecution WHERE id=?",
            (norm_id,),
        )
        row = cur.fetchone()
        assert row, f"Workflow not found: {exec_id}"
        status, result = row
        result_obj = json.loads(result) if result else None
        return {"status": status, "result": result_obj}
    finally:
        con.close()


def read_activity_statuses(exec_id: str) -> List[str]:
    con = sqlite3.connect(DB_PATH)
    try:
        cur = con.cursor()
        norm_id = exec_id.replace('-', '')
        cur.execute(
            "SELECT status FROM django_durable_activitytask WHERE execution_id=?",
            (norm_id,),
        )
        return [r[0] for r in cur.fetchall()]
    finally:
        con.close()


def run_worker(iterations: int = 50) -> None:
    run_manage(
        "durable_worker",
        "--batch",
        "100",
        "--tick",
        "0.01",
        "--iterations",
        str(iterations),
    )


WORKFLOWS: List[Dict[str, Any]] = [
    {
        "name": "add_flow",
        "input": {"a": 1, "b": 2},
        "expect_status": "COMPLETED",
        "expect_result": {"value": 3},
    },
    {
        "name": "retry_flow",
        "input": {"key": "k", "fail_times": 1},
        "expect_status": "COMPLETED",
        "expect_result": {"attempts": 2},
    },
    {
        "name": "retry_linear_flow",
        "input": {"key": "k", "fail_times": 1},
        "expect_status": "COMPLETED",
        "expect_result": {"attempts": 2},
    },
    {
        "name": "heartbeat_flow",
        "input": {},
        "expect_status": "COMPLETED",
        "expect_result": {"ok": True},
    },
    {
        "name": "activity_timeout_flow",
        "input": {},
        "expect_status": "FAILED",
        "expect_result": None,
    },
    {
        "name": "sleep_work_loop",
        "input": {"loops": 3, "sleep": 0},
        "expect_status": "COMPLETED",
        "expect_result": {"done": 3},
    },
    {
        "name": "parent_child_workflow",
        "input": {"x": 3},
        "expect_status": "COMPLETED",
        "expect_result": {"child": {"y": 4}},
    },
    {
        "name": "long_running_step_flow",
        "input": {"loops": 2, "delay": 0.01},
        "expect_status": "COMPLETED",
        "expect_result": {"done": 2},
    },
    {
        "name": "long_activity_flow",
        "input": {"loops": 2, "delay": 0.01},
        "expect_status": "COMPLETED",
        "expect_result": {"done": 2},
    },
    {
        "name": "e2e_flow",
        "input": {"value": 5},
        "signal": {"name": "go", "input": {"ok": True}},
        "expect_status": "COMPLETED",
        "expect_result": {"res": 5, "sig": {"ok": True}},
    },
    {
        "name": "complex_flow",
        "input": {"value": 2},
        "signal": {"name": "finish", "input": {"add": 3}},
        "expect_status": "COMPLETED",
        "expect_result": {"result": 17, "sig": {"add": 3}},
    },
]


def run_workflow(spec: Dict[str, Any]) -> None:
    out = run_manage(
        "durable_start",
        spec["name"],
        "--input",
        json.dumps(spec["input"]),
    )
    exec_id = out.splitlines()[-1].strip()
    run_worker()
    if "signal" in spec:
        sig = spec["signal"]
        run_manage(
            "durable_signal",
            exec_id,
            sig["name"],
            "--input",
            json.dumps(sig["input"]),
        )
        run_worker()
    # Poll until workflow leaves RUNNING state so retries/timers can settle
    deadline = time.time() + 5
    wf = read_workflow(exec_id)
    while wf["status"] == "RUNNING" and time.time() < deadline:
        run_worker()
        wf = read_workflow(exec_id)
    if wf["status"] != spec["expect_status"]:
        raise AssertionError(
            f"Workflow {spec['name']} status {wf['status']} != {spec['expect_status']}"
        )
    if wf["result"] != spec["expect_result"]:
        raise AssertionError(
            f"Workflow {spec['name']} result {wf['result']} != {spec['expect_result']}"
        )
    statuses = read_activity_statuses(exec_id)
    if spec["expect_status"] == "COMPLETED":
        if statuses and not all(s == "COMPLETED" for s in statuses):
            raise AssertionError(
                f"Activities not all completed for {spec['name']}: {statuses}"
            )
    else:
        if statuses and all(s == "COMPLETED" for s in statuses):
            raise AssertionError(
                f"Activities unexpectedly all completed for {spec['name']}"
            )


def main() -> None:
    parser = argparse.ArgumentParser(description="Stress test harness")
    parser.add_argument(
        "--seconds", type=int, default=10, help="Duration to run in seconds"
    )
    args = parser.parse_args()
    deadline = time.time() + args.seconds
    count = 0
    while time.time() < deadline:
        spec = WORKFLOWS[count % len(WORKFLOWS)]
        run_workflow(spec)
        count += 1
    print(f"Completed {count} workflow executions")


if __name__ == "__main__":
    main()


