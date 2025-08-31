import json
import random
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
        norm = exec_id.replace('-', '')
        cur.execute(
            "SELECT status FROM django_durable_workflowexecution WHERE id=?",
            (norm,),
        )
        row = cur.fetchone()
        assert row, f"Workflow not found: {exec_id}"
        return row[0]
    finally:
        con.close()


def running_counts():
    con = sqlite3.connect(DB_PATH)
    try:
        cur = con.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM django_durable_workflowexecution WHERE status='RUNNING'"
        )
        wf = cur.fetchone()[0]
        cur.execute(
            "SELECT COUNT(*) FROM django_durable_activitytask WHERE status='RUNNING'"
        )
        act = cur.fetchone()[0]
        return wf, act
    finally:
        con.close()


def start_worker():
    return subprocess.Popen(
        [sys.executable, MANAGE, "durable_worker", "--batch", "5", "--tick", "0.05"]
    )


@pytest.fixture(scope="session", autouse=True)
def migrate_db():
    run_manage("migrate", "--noinput")


@pytest.mark.slow
def test_worker_killed_during_workflow(tmp_path):
    run_manage("flush", "--noinput")
    exec_ids = []
    for _ in range(5):
        out = run_manage(
            "durable_start",
            "long_running_step_flow",
            "--input",
            json.dumps({"loops": 3, "delay": 0.2}),
        )
        exec_ids.append(out.splitlines()[-1].strip())

    workers = [start_worker() for _ in range(3)]
    rnd = random.Random(0)
    start = time.time()
    deadline = start + 60
    try:
        while time.time() < deadline:
            statuses = [read_workflow(eid) for eid in exec_ids]
            if all(s == "COMPLETED" for s in statuses):
                break
            _, act_running = running_counts()
            if act_running == 0 and rnd.random() < 0.3:
                victim = rnd.choice(workers)
                victim.kill()
                victim.wait(timeout=5)
                workers.remove(victim)
                workers.append(start_worker())
            time.sleep(0.1)
        assert all(read_workflow(eid) == "COMPLETED" for eid in exec_ids)
    finally:
        for p in workers:
            p.terminate()
            try:
                p.wait(timeout=5)
            except Exception:
                p.kill()


@pytest.mark.slow
def test_worker_killed_during_activity(tmp_path):
    run_manage("flush", "--noinput")
    exec_ids = []
    for _ in range(5):
        out = run_manage(
            "durable_start",
            "long_activity_flow",
            "--input",
            json.dumps({"loops": 3, "delay": 0.2}),
        )
        exec_ids.append(out.splitlines()[-1].strip())

    workers = [start_worker() for _ in range(3)]
    rnd = random.Random(1)
    start = time.time()
    deadline = start + 60
    try:
        while time.time() < deadline:
            statuses = [read_workflow(eid) for eid in exec_ids]
            if all(s == "COMPLETED" for s in statuses):
                break
            _, act_running = running_counts()
            if act_running > 0 and rnd.random() < 0.1:
                victim = rnd.choice(workers)
                victim.kill()
                victim.wait(timeout=5)
                workers.remove(victim)
                workers.append(start_worker())
            time.sleep(0.1)
        assert all(read_workflow(eid) == "COMPLETED" for eid in exec_ids)
    finally:
        for p in workers:
            p.terminate()
            try:
                p.wait(timeout=5)
            except Exception:
                p.kill()
