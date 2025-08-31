import json
import subprocess
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[2]
MANAGE = str(ROOT / "manage.py")


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


def test_status_and_custom_query(tmp_path):
    run_manage("flush", "--noinput")
    # Start workflow and progress to signal wait
    out = run_manage(
        "durable_start", "e2e_flow", "--input", json.dumps({"value": 5})
    )
    exec_id = out.splitlines()[-1].strip()

    run_manage(
        "durable_worker", "--batch", "50", "--tick", "0.01", "--iterations", "10"
    )

    # Default status query
    res = run_manage("durable_status", exec_id)
    data = json.loads(res)
    assert data["status"] == "WAITING"
    assert data["workflow_name"] == "e2e_flow"

    # Custom history query registered for e2e_flow
    res = run_manage("durable_status", exec_id, "--query", "history")
    data = json.loads(res)
    assert data["events"] > 0

