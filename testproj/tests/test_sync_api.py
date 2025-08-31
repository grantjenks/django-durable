import os
import sys
from pathlib import Path

import django
import pytest


ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "testproj.settings")
django.setup()

from django.core.management import call_command

from django_durable.engine import (
    run_activity,
    run_workflow,
    start_activity,
    start_workflow,
    wait_activity,
    wait_workflow,
)


@pytest.fixture(scope="session", autouse=True)
def migrate_db():
    call_command("migrate", "--noinput")


@pytest.fixture(autouse=True)
def flush_db():
    call_command("flush", "--noinput")


def test_run_workflow():
    res = run_workflow("retry_flow", key="k1", fail_times=2)
    assert res == {"attempts": 3}


def test_start_and_wait_workflow():
    handle = start_workflow("retry_flow", key="k2", fail_times=1)
    res = wait_workflow(handle)
    assert res == {"attempts": 2}


def test_run_activity():
    res = run_activity("add", 3, 4)
    assert res == {"value": 7}


def test_start_and_wait_activity():
    handle = start_activity("add", 5, 6)
    res = wait_activity(handle)
    assert res == {"value": 11}

