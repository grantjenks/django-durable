import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
import sqlite3

ROOT = Path(__file__).resolve().parents[2]
MANAGE = str(ROOT / "manage.py")
DB_PATH = str(ROOT / "db.sqlite3")


def run_manage(*args, check=True):
    cmd = [sys.executable, MANAGE, *args]
    res = subprocess.run(cmd, capture_output=True, text=True)
    if check and res.returncode != 0:
        raise AssertionError(
            f"Command failed: {' '.join(cmd)}\nSTDOUT:\n{res.stdout}\nSTDERR:\n{res.stderr}"
        )
    return res.stdout.strip()


@pytest.fixture(scope="session", autouse=True)
def migrate_db():
    run_manage("migrate", "--noinput")


def read_workflow(exec_id):
    con = sqlite3.connect(DB_PATH)
    try:
        cur = con.cursor()
        cur.execute(
            "SELECT status, result FROM django_durable_workflowexecution WHERE id=?",
            (int(exec_id),),
        )
        row = cur.fetchone()
        assert row, f"Workflow not found: {exec_id}"
        status, result = row
        try:
            result_obj = json.loads(result) if result is not None else None
        except Exception:
            result_obj = None
        return status, result_obj
    finally:
        con.close()


def read_activity_statuses(exec_id):
    con = sqlite3.connect(DB_PATH)
    try:
        cur = con.cursor()
        cur.execute(
            "SELECT status FROM django_durable_activitytask WHERE execution_id=?",
            (int(exec_id),),
        )
        return [r[0] for r in cur.fetchall()]
    finally:
        con.close()


def test_activity_timeout(tmp_path):
    run_manage("flush", "--noinput")
    out = run_manage("durable_start", "testproj.durable_workflows.activity_timeout_flow")
    exec_id = out.splitlines()[-1].strip()
    run_manage("durable_worker", "--batch", "50", "--tick", "0.01", "--iterations", "5")
    status, _ = read_workflow(exec_id)
    assert status == "FAILED"
    statuses = read_activity_statuses(exec_id)
    assert statuses[0] == "TIMED_OUT"


def test_workflow_timeout(tmp_path):
    out = run_manage(
        "durable_start",
        "testproj.durable_workflows.sleep_work_loop",
        "--input",
        json.dumps({"loops": 1, "sleep": 1}),
        "--timeout",
        "0.1",
    )
    exec_id = out.splitlines()[-1].strip()
    run_manage(
        "durable_worker",
        "--batch",
        "50",
        "--tick",
        "0.01",
        "--iterations",
        "20",
    )
    status, _ = read_workflow(exec_id)
    assert status == "TIMED_OUT"


def test_retry_policy(tmp_path):
    run_manage("flush", "--noinput")
    out = run_manage(
        "durable_start",
        "testproj.durable_workflows.retry_flow",
        "--input",
        json.dumps({"key": "a", "fail_times": 2}),
    )
    exec_id = out.splitlines()[-1].strip()
    run_manage(
        "durable_worker",
        "--batch",
        "50",
        "--tick",
        "0.01",
        "--iterations",
        "1000",
    )
    status, result = read_workflow(exec_id)
    assert status == "COMPLETED"
    assert result == {"attempts": 3}


def test_retry_policy_linear(tmp_path):
    run_manage("flush", "--noinput")
    out = run_manage(
        "durable_start",
        "testproj.durable_workflows.retry_linear_flow",
        "--input",
        json.dumps({"key": "a", "fail_times": 2}),
    )
    exec_id = out.splitlines()[-1].strip()
    run_manage(
        "durable_worker",
        "--batch",
        "50",
        "--tick",
        "0.01",
        "--iterations",
        "1000",
    )
    status, result = read_workflow(exec_id)
    assert status == "COMPLETED"
    assert result == {"attempts": 3}
