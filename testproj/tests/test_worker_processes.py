import json
import sqlite3
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
        raise AssertionError(
            f"Command failed: {' '.join(cmd)}\nSTDOUT:\n{res.stdout}\nSTDERR:\n{res.stderr}"
        )
    return res


def read_workflow(exec_id):
    con = sqlite3.connect(DB_PATH)
    try:
        cur = con.cursor()
        norm_id = exec_id.replace("-", "")
        cur.execute(
            "SELECT status FROM django_durable_workflowexecution WHERE id=?",
            (norm_id,),
        )
        row = cur.fetchone()
        assert row, f"Workflow not found: {exec_id}"
        return row[0]
    finally:
        con.close()


def read_activity_status(exec_id):
    con = sqlite3.connect(DB_PATH)
    try:
        cur = con.cursor()
        norm_id = exec_id.replace("-", "")
        cur.execute(
            "SELECT status FROM django_durable_activitytask WHERE execution_id=?",
            (norm_id,),
        )
        row = cur.fetchone()
        assert row, "Activity not found"
        return row[0]
    finally:
        con.close()


@pytest.fixture(scope="session", autouse=True)
def migrate_db():
    run_manage("migrate", "--noinput")


def test_activity_timeout_kills_process():
    run_manage("flush", "--noinput")
    out = run_manage(
        "durable_start",
        "long_activity_flow",
        "--input",
        json.dumps({"loops": 1, "delay": 5.0}),
    )
    exec_id = out.stdout.strip().splitlines()[-1]

    start = time.time()
    run_manage("durable_worker", "--tick", "0", "--batch", "10", "--iterations", "5")
    elapsed = time.time() - start
    assert elapsed < 4, f"worker took too long: {elapsed}s"

    assert read_activity_status(exec_id) in {"TIMED_OUT", "QUEUED"}
    assert read_workflow(exec_id) in {"RUNNING", "PENDING"}
