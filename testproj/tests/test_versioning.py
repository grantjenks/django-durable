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
from django_durable import register, signal_workflow
from django_durable.models import WorkflowExecution, ActivityTask
from testproj.durable_activities import echo


@pytest.fixture(autouse=True, scope="module")
def migrate_db():
    call_command("migrate", interactive=False, verbosity=0)


def _run_activity(execution):
    task = ActivityTask.objects.filter(execution=execution).first()
    assert task is not None
    call_command("durable_internal_run_activity", str(task.id))


def _step_to_waiting(execution):
    call_command("durable_internal_step_workflow", str(execution.id))
    _run_activity(execution)
    call_command("durable_internal_step_workflow", str(execution.id))


def test_get_version_survives_code_change():
    register.workflows.pop(f"{__name__}.version_flow", None)

    @register.workflow()
    def version_flow(ctx):
        v = ctx.get_version("change", 1)
        if v == 1:
            res = ctx.run_activity(echo, "v1")
        else:
            res = ctx.run_activity(echo, "v2")
        ctx.wait_signal("go")
        return res["value"]

    exec1 = WorkflowExecution.objects.create(workflow_name=version_flow._durable_name, input={})
    _step_to_waiting(exec1)

    register.workflows.pop(f"{__name__}.version_flow", None)

    @register.workflow()
    def version_flow(ctx):
        v = ctx.get_version("change", 2)
        if v == 1:
            res = ctx.run_activity(echo, "v1")
        else:
            res = ctx.run_activity(echo, "v2")
        sig = ctx.wait_signal("go")
        return res["value"]

    signal_workflow(exec1, "go")
    call_command("durable_internal_step_workflow", str(exec1.id))
    exec1.refresh_from_db()
    assert exec1.result == "v1"

    exec2 = WorkflowExecution.objects.create(workflow_name=version_flow._durable_name, input={})
    _step_to_waiting(exec2)
    signal_workflow(exec2, "go")
    call_command("durable_internal_step_workflow", str(exec2.id))
    exec2.refresh_from_db()
    assert exec2.result == "v2"


def test_patch_deprecation_allows_removal():
    register.workflows.pop(f"{__name__}.patch_flow", None)

    @register.workflow()
    def patch_flow(ctx):
        if ctx.patched("feat"):
            res = ctx.run_activity(echo, "new")
        else:
            res = ctx.run_activity(echo, "old")
        ctx.wait_signal("go")
        return res["value"]

    exec1 = WorkflowExecution.objects.create(workflow_name=patch_flow._durable_name, input={})
    _step_to_waiting(exec1)

    register.workflows.pop(f"{__name__}.patch_flow", None)

    @register.workflow()
    def patch_flow(ctx):
        ctx.deprecate_patch("feat")
        res = ctx.run_activity(echo, "new")
        ctx.wait_signal("go")
        return res["value"]

    signal_workflow(exec1, "go")
    call_command("durable_internal_step_workflow", str(exec1.id))
    exec1.refresh_from_db()
    assert exec1.result == "new"

    exec2 = WorkflowExecution.objects.create(workflow_name=patch_flow._durable_name, input={})
    _step_to_waiting(exec2)
    signal_workflow(exec2, "go")
    call_command("durable_internal_step_workflow", str(exec2.id))
    exec2.refresh_from_db()
    assert exec2.result == "new"
