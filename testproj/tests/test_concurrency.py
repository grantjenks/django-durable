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
    return res.stdout.strip()


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


@pytest.fixture(scope="session", autouse=True)
def migrate_db():
    run_manage("migrate", "--noinput")


def test_multiple_workers_process_flows_concurrently(tmp_path):
    run_manage("flush", "--noinput")
    # Launch 10 workflows that repeatedly sleep and perform an activity.
    exec_ids = []
    for _ in range(10):
        out = run_manage(
            "durable_start",
            "sleep_work_loop",
            "--input",
            json.dumps({"loops": 3, "sleep": 0.3}),
        )
        exec_ids.append(out.splitlines()[-1].strip())

    # Start three worker processes in parallel.
    workers = [
        subprocess.Popen(
            [sys.executable, MANAGE, "durable_worker", "--batch", "5", "--tick", "0.05"]
        )
        for _ in range(3)
    ]

    start = time.time()
    try:
        deadline = start + 20  # allow extra time for workers to start
        while time.time() < deadline:
            statuses = [read_workflow(eid) for eid in exec_ids]
            if all(s == "COMPLETED" for s in statuses):
                break
            time.sleep(0.1)

        total = time.time() - start
        assert all(read_workflow(eid) == "COMPLETED" for eid in exec_ids)
        # Sequential would take ~9s (3 loops * 0.3s * 10 workflows)
        assert total < 15, f"workflows took too long: {total}"  # ensure concurrency
    finally:
        for p in workers:
            p.terminate()
            try:
                p.wait(timeout=5)
            except Exception:
                p.kill()
