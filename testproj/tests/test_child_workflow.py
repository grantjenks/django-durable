import json
import sqlite3
import subprocess
import sys
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
        try:
            result_obj = json.loads(result) if result is not None else None
        except Exception:
            result_obj = None
        return status, result_obj
    finally:
        con.close()


def read_child(parent_id):
    con = sqlite3.connect(DB_PATH)
    try:
        cur = con.cursor()
        norm_parent = parent_id.replace('-', '')
        cur.execute(
            "SELECT id FROM django_durable_workflowexecution WHERE parent_id=?",
            (norm_parent,),
        )
        row = cur.fetchone()
        assert row, "Child workflow not found"
        child_id_hex = row[0]
        import uuid

        child_id = str(uuid.UUID(child_id_hex))
        return read_workflow(child_id)
    finally:
        con.close()


def test_parent_waits_for_child(tmp_path):
    run_manage("flush", "--noinput")
    out = run_manage(
        "durable_start",
        "parent_child_workflow",
        "--input",
        json.dumps({"x": 2}),
    )
    parent_id = out.splitlines()[-1].strip()

    run_manage("durable_worker", "--batch", "50", "--tick", "0.01", "--iterations", "20")

    status, result = read_workflow(parent_id)
    assert status == "COMPLETED"
    assert result == {"child": {"y": 3}}

    c_status, c_result = read_child(parent_id)
    assert c_status == "COMPLETED"
    assert c_result == {"y": 3}
