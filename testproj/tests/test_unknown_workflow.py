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
from django_durable import start_workflow
from django_durable.exceptions import UnknownWorkflowError


@pytest.fixture(scope="session", autouse=True)
def migrate_db():
    call_command("migrate", "--noinput")


@pytest.fixture(autouse=True)
def flush_db():
    call_command("flush", "--noinput")


def test_start_unknown_workflow_raises():
    with pytest.raises(UnknownWorkflowError):
        start_workflow("testproj.durable_workflows.missing_flow")
