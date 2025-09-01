import os
from pathlib import Path
import sys
import django
import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT))

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "testproj.settings")
django.setup()

from django.core.management import call_command
from django_durable.registry import register
from django_durable.models import WorkflowExecution, ActivityTask
from django_durable import engine
import testproj.durable_activities  # ensure activities registered


@pytest.fixture(autouse=True, scope="module")
def migrate_db():
    call_command("migrate", interactive=False, verbosity=0)


def _run_activity(execution):
    task = ActivityTask.objects.filter(execution=execution).first()
    assert task is not None
    engine.execute_activity(task)


def _step_to_waiting(execution):
    engine.step_workflow(execution)
    _run_activity(execution)
    engine.step_workflow(execution)


def test_get_version_survives_code_change():
    register.workflows.pop("version_flow", None)

    @register.workflow(name="version_flow")
    def version_flow(ctx):
        v = ctx.get_version("change", 1)
        if v == 1:
            res = ctx.run_activity("echo", "v1")
        else:
            res = ctx.run_activity("echo", "v2")
        ctx.wait_signal("go")
        return res["value"]

    exec1 = WorkflowExecution.objects.create(workflow_name="version_flow", input={})
    _step_to_waiting(exec1)

    register.workflows.pop("version_flow", None)

    @register.workflow(name="version_flow")
    def version_flow(ctx):
        v = ctx.get_version("change", 2)
        if v == 1:
            res = ctx.run_activity("echo", "v1")
        else:
            res = ctx.run_activity("echo", "v2")
        sig = ctx.wait_signal("go")
        return res["value"]

    engine.send_signal(exec1, "go")
    engine.step_workflow(exec1)
    exec1.refresh_from_db()
    assert exec1.result == "v1"

    exec2 = WorkflowExecution.objects.create(workflow_name="version_flow", input={})
    _step_to_waiting(exec2)
    engine.send_signal(exec2, "go")
    engine.step_workflow(exec2)
    exec2.refresh_from_db()
    assert exec2.result == "v2"


def test_patch_deprecation_allows_removal():
    register.workflows.pop("patch_flow", None)

    @register.workflow(name="patch_flow")
    def patch_flow(ctx):
        if ctx.patched("feat"):
            res = ctx.run_activity("echo", "new")
        else:
            res = ctx.run_activity("echo", "old")
        ctx.wait_signal("go")
        return res["value"]

    exec1 = WorkflowExecution.objects.create(workflow_name="patch_flow", input={})
    _step_to_waiting(exec1)

    register.workflows.pop("patch_flow", None)

    @register.workflow(name="patch_flow")
    def patch_flow(ctx):
        ctx.deprecate_patch("feat")
        res = ctx.run_activity("echo", "new")
        ctx.wait_signal("go")
        return res["value"]

    engine.send_signal(exec1, "go")
    engine.step_workflow(exec1)
    exec1.refresh_from_db()
    assert exec1.result == "new"

    exec2 = WorkflowExecution.objects.create(workflow_name="patch_flow", input={})
    _step_to_waiting(exec2)
    engine.send_signal(exec2, "go")
    engine.step_workflow(exec2)
    exec2.refresh_from_db()
    assert exec2.result == "new"
