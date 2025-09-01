import json
import os
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


def read_activity_results(exec_id):
    con = sqlite3.connect(DB_PATH)
    try:
        cur = con.cursor()
        norm_id = exec_id.replace("-", "")
        cur.execute(
            "SELECT result FROM django_durable_activitytask WHERE execution_id=? AND activity_name=?",
            (norm_id, "do_work"),
        )
        rows = cur.fetchall()
        return [json.loads(r[0])["i"] for r in rows if r[0]]
    finally:
        con.close()


@pytest.fixture(scope="session", autouse=True)
def migrate_db():
    run_manage("migrate", "--noinput")


def test_negative_threads_invalid():
    res = run_manage(
        "durable_worker", "--threads", "-1", "--iterations", "1", check=False
    )
    assert res.returncode != 0
    assert "threads" in res.stderr.lower()


def test_threads_zero_and_positive():
    run_manage("durable_worker", "--threads", "0", "--iterations", "1")
    run_manage("durable_worker", "--threads", "2", "--iterations", "1")


def test_thread_safety_stress():
    steps = int(os.environ.get("DURABLE_STRESS_STEPS", "20"))
    run_manage("flush", "--noinput")
    out = run_manage(
        "durable_start",
        "sleep_work_loop",
        "--input",
        json.dumps({"loops": steps, "sleep": 0}),
    )
    exec_id = out.stdout.strip().splitlines()[-1]

    worker = subprocess.Popen(
        [
            sys.executable,
            MANAGE,
            "durable_worker",
            "--threads",
            "4",
            "--tick",
            "0",
            "--batch",
            "100",
        ]
    )
    try:
        deadline = time.time() + max(30, steps)
        while time.time() < deadline:
            if read_workflow(exec_id) == "COMPLETED":
                break
            time.sleep(0.1)
        assert read_workflow(exec_id) == "COMPLETED"
        results = read_activity_results(exec_id)
        assert len(results) == steps
        assert sorted(results) == list(range(steps))
    finally:
        worker.terminate()
        try:
            worker.wait(timeout=5)
        except Exception:
            worker.kill()

