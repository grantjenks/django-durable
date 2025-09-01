import json
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
        norm_id = exec_id.replace('-', '')
        cur.execute(
            "SELECT status, result FROM django_durable_workflowexecution WHERE id=?",
            (norm_id,),
        )
        row = cur.fetchone()
        assert row, f"Workflow not found: {exec_id}"
        status, result = row
        result_obj = json.loads(result) if result is not None else None
        return status, result_obj
    finally:
        con.close()


def read_activity(exec_id):
    con = sqlite3.connect(DB_PATH)
    try:
        cur = con.cursor()
        norm_id = exec_id.replace('-', '')
        cur.execute(
            "SELECT status, heartbeat_details FROM django_durable_activitytask WHERE execution_id=?",
            (norm_id,),
        )
        row = cur.fetchone()
        if not row:
            return None, None
        status, hb = row
        hb_obj = json.loads(hb) if hb else None
        return status, hb_obj
    finally:
        con.close()


def test_activity_heartbeat_success(tmp_path):
    out = run_manage("durable_start", "heartbeat_flow")
    exec_id = out.splitlines()[-1].strip()
    run_manage("durable_worker", "--batch", "50", "--tick", "0.01", "--iterations", "5")
    status, result = read_workflow(exec_id)
    assert status == "COMPLETED"
    assert result == {"ok": True}
    a_status, hb = read_activity(exec_id)
    assert a_status == "COMPLETED"
    assert hb == {"beat": 2}


def test_activity_heartbeat_timeout(tmp_path):
    out = run_manage("durable_start", "heartbeat_timeout_flow")
    exec_id = out.splitlines()[-1].strip()
    # Step workflow once to enqueue the activity
    run_manage("durable_worker", "--batch", "50", "--tick", "0.01", "--iterations", "1")

    # Mark the activity as running with a stale heartbeat
    code = (
        "from django_durable.models import ActivityTask\n"
        "from django.utils import timezone\n"
        "from datetime import timedelta\n"
        f"t = ActivityTask.objects.get(execution_id='{exec_id}')\n"
        "t.status = 'RUNNING'\n"
        "t.started_at = timezone.now() - timedelta(seconds=5)\n"
        "t.heartbeat_at = t.started_at\n"
        "t.heartbeat_timeout = 0.1\n"
        "t.save()\n"
    )
    run_manage("shell", "-c", code)

    # Run worker to detect heartbeat timeout
    run_manage("durable_worker", "--batch", "50", "--tick", "0.01", "--iterations", "5")
    status, _ = read_workflow(exec_id)
    assert status == "FAILED"
    a_status, _ = read_activity(exec_id)
    assert a_status == "TIMED_OUT"

