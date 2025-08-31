import json
import os
import subprocess
import sys
import time
from pathlib import Path
import pytest


ROOT = Path(__file__).resolve().parents[2]
MANAGE = str(ROOT / "manage.py")
DB_PATH = str(ROOT / "db.sqlite3")


def run_manage(*args, check=True):
    cmd = [sys.executable, MANAGE, *args]
    res = subprocess.run(cmd, capture_output=True, text=True)
    if check and res.returncode != 0:
        raise AssertionError(f"Command failed: {' '.join(cmd)}\nSTDOUT:\n{res.stdout}\nSTDERR:\n{res.stderr}")
    return res.stdout.strip()


@pytest.fixture(scope="session", autouse=True)
def migrate_db():
    # Ensure DB schema is up-to-date for tests
    run_manage("migrate", "--noinput")


def read_workflow(exec_id):
    import sqlite3

    con = sqlite3.connect(DB_PATH)
    try:
        cur = con.cursor()
        # SQLite stores UUIDs as 32-char hex without dashes by default
        norm_id = exec_id.replace('-', '')
        cur.execute(
            "SELECT status, result FROM django_durable_workflowexecution WHERE id=?",
            (norm_id,),
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
    import sqlite3

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


def test_signal_flow_completes(tmp_path):
    # Start workflow
    out = run_manage(
        "durable_start",
        "e2e_flow",
        "--input",
        json.dumps({"value": 7}),
    )
    exec_id = out.splitlines()[-1].strip()
    assert len(exec_id) > 0

    # Run worker a few iterations to schedule/execute activity and reach signal wait
    run_manage("durable_worker", "--batch", "50", "--tick", "0.01", "--iterations", "10")

    status, result = read_workflow(exec_id)
    assert status == "RUNNING"
    assert result is None

    # Send signal and resume to completion
    run_manage(
        "durable_signal",
        exec_id,
        "go",
        "--input",
        json.dumps({"ok": True}),
    )
    run_manage("durable_worker", "--batch", "50", "--tick", "0.01", "--iterations", "10")

    status, result = read_workflow(exec_id)
    assert status == "COMPLETED"
    assert result == {"res": 7, "sig": {"ok": True}}


def test_cancel_marks_workflow_and_tasks(tmp_path):
    # Start workflow
    out = run_manage(
        "durable_start",
        "e2e_flow",
        "--input",
        json.dumps({"value": 1}),
    )
    exec_id = out.splitlines()[-1].strip()

    # Run one iteration to schedule first activity but not execute it
    run_manage("durable_worker", "--batch", "50", "--tick", "0.01", "--iterations", "1")

    # Cancel
    run_manage("durable_cancel", exec_id, "--reason", "test")

    # Verify workflow canceled and queued tasks failed
    status, result = read_workflow(exec_id)
    assert status == "CANCELED"
    statuses = read_activity_statuses(exec_id)
    # Either 0 (if no task yet) or all failed
    assert all(s == "FAILED" for s in statuses)


def test_complex_flow_runs_end_to_end(tmp_path):
    # Start complex workflow
    out = run_manage(
        "durable_start",
        "complex_flow",
        "--input",
        json.dumps({"value": 2}),
    )
    exec_id = out.splitlines()[-1].strip()
    assert exec_id

    # Run worker to progress through first activities and reach signal wait
    run_manage("durable_worker", "--batch", "50", "--tick", "0.01", "--iterations", "20")

    status, result = read_workflow(exec_id)
    assert status == "RUNNING"
    assert result is None

    # Send signal and resume workflow to completion
    run_manage(
        "durable_signal",
        exec_id,
        "finish",
        "--input",
        json.dumps({"add": 3}),
    )
    run_manage("durable_worker", "--batch", "50", "--tick", "0.01", "--iterations", "20")

    status, result = read_workflow(exec_id)
    assert status == "COMPLETED"
    assert result == {"result": 17, "sig": {"add": 3}}

    statuses = read_activity_statuses(exec_id)
    assert len(statuses) == 6
    assert all(s == "COMPLETED" for s in statuses)
