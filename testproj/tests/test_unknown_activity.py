import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
MANAGE = str(ROOT / "manage.py")
DB_PATH = str(ROOT / "db.sqlite3")

def run_manage(*args: str, check: bool = True):
    cmd = [sys.executable, MANAGE, *args]
    res = subprocess.run(cmd, capture_output=True, text=True)
    if check and res.returncode != 0:
        raise AssertionError(
            f"Command failed: {' '.join(cmd)}\nSTDOUT:\n{res.stdout}\nSTDERR:\n{res.stderr}"
        )
    return res


@pytest.fixture(scope="session", autouse=True)
def migrate_db() -> None:
    run_manage("migrate", "--noinput")


def read_task(task_id: str):
    con = sqlite3.connect(DB_PATH)
    try:
        cur = con.cursor()
        norm_id = task_id.replace("-", "")
        cur.execute(
            "SELECT status, error FROM django_durable_activitytask WHERE id=?",
            (norm_id,),
        )
        return cur.fetchone()
    finally:
        con.close()


def test_unknown_activity_fails_without_crashing() -> None:
    res = run_manage(
        "shell",
        "-c",
        "from django_durable.models import WorkflowExecution, ActivityTask;\n"
        "wf=WorkflowExecution.objects.create(workflow_name='wf');\n"
        "t=ActivityTask.objects.create(execution=wf, activity_name='missing');\n"
        "print(t.id)",
    )
    task_id = res.stdout.strip().splitlines()[-1]

    run_manage("durable_worker", "--batch", "10", "--tick", "0", "--iterations", "1")

    status, error = read_task(task_id)
    assert status == "FAILED"
    assert "Unknown activity" in error or error == "workflow_not_runnable"
