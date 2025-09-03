import json
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

from django_durable import (
    cancel_workflow,
    run_workflow,
    signal_workflow,
    start_workflow,
    wait_workflow,
    register,
)
from django_durable.exceptions import (
    ActivityTimeout,
    NondeterminismError,
    WorkflowException,
    WorkflowTimeout,
)
from django_durable.engine import Context, step_workflow
from django_durable.models import ActivityTask, WorkflowExecution, HistoryEvent
from django_durable.constants import HistoryEventType, ErrorCode


@pytest.fixture(scope="session", autouse=True)
def migrate_db():
    call_command("migrate", "--noinput")


@pytest.fixture(autouse=True)
def flush_db():
    call_command("flush", "--noinput")


def test_run_workflow():
    res = run_workflow("testproj.retry_flow", key="k1", fail_times=2)
    assert res == {"attempts": 3}


def test_start_and_wait_workflow():
    handle = start_workflow("testproj.retry_flow", key="k2", fail_times=1)
    res = wait_workflow(handle)
    assert res == {"attempts": 2}


def test_activity_within_workflow():
    @register.workflow()
    def add_flow(ctx, a, b):
        return ctx.run_activity("testproj.add", a, b)

    res = run_workflow("testproj.add_flow", a=3, b=4)
    assert res == {"value": 7}


def test_parallel_activities():
    @register.workflow()
    def parent(ctx):
        handles = [ctx.start_activity("testproj.add", i, i + 1) for i in range(3)]
        results = [ctx.wait_activity(h) for h in handles]
        return {"results": results}

    res = run_workflow("testproj.parent")
    assert res == {"results": [{"value": 1}, {"value": 3}, {"value": 5}]}


def test_run_workflow_with_child_workflow():
    @register.workflow()
    def child(ctx, x):
        return {"res": x + 1}

    @register.workflow()
    def parent(ctx, x):
        return {"child": ctx.run_workflow("testproj.child", x=x)}

    res = run_workflow("testproj.parent", x=3)
    assert res == {"child": {"res": 4}}


def test_child_workflow_failure_propagates():
    @register.workflow()
    def failing_child(ctx):
        raise RuntimeError("boom")

    @register.workflow()
    def parent(ctx):
        ctx.run_workflow("testproj.failing_child")

    with pytest.raises(WorkflowException):
        run_workflow("testproj.parent")


def test_signal_queue_consumed_in_order():
    @register.workflow()
    def sig_flow(ctx):
        first = ctx.wait_signal("go")
        second = ctx.wait_signal("go")
        return {"signals": [first, second]}

    handle = start_workflow("testproj.sig_flow")
    signal_workflow(handle, "go", {"n": 1})
    signal_workflow(handle, "go", {"n": 2})
    res = wait_workflow(handle)
    assert res == {"signals": [{"n": 1}, {"n": 2}]}


def test_activity_timeout_can_be_caught():
    wf = WorkflowExecution.objects.create(workflow_name="wf")
    HistoryEvent.objects.create(
        execution=wf,
        type=HistoryEventType.ACTIVITY_TIMED_OUT.value,
        pos=1,
        details={"error": ErrorCode.ACTIVITY_TIMEOUT.value},
    )
    ctx = Context(execution=wf)
    with pytest.raises(ActivityTimeout):
        ctx.wait_activity(1)


def test_wait_workflow_raises_workflowtimeout():
    wf = WorkflowExecution.objects.create(
        workflow_name="wf",
        status=WorkflowExecution.Status.TIMED_OUT,
        error=ErrorCode.WORKFLOW_TIMEOUT.value,
    )
    with pytest.raises(WorkflowTimeout):
        wait_workflow(wf)


def test_activity_input_mismatch_raises_nondeterminism():
    wf = WorkflowExecution.objects.create(workflow_name="wf")
    ctx = Context(execution=wf)
    ctx.start_activity("testproj.add", 1, b=2)
    ev = HistoryEvent.objects.get(
        execution=wf,
        pos=0,
        type=HistoryEventType.ACTIVITY_SCHEDULED.value,
    )
    assert ev.details["input"] == json.dumps({"args": [1], "kwargs": {"b": 2}})
    ctx_replay = Context(execution=wf)
    ctx_replay.start_activity("testproj.add", 1, b=2)
    ctx_mismatch = Context(execution=wf)
    with pytest.raises(NondeterminismError):
        ctx_mismatch.start_activity("testproj.add", 1, b=3)


def test_cancel_workflow_programmatically():
    @register.workflow()
    def cancel_flow(ctx):
        ctx.run_activity("testproj.add", 1, 2)

    handle = start_workflow("testproj.cancel_flow")
    wf = WorkflowExecution.objects.get(pk=handle)
    step_workflow(wf)

    cancel_workflow(handle, reason="test")

    wf.refresh_from_db()
    assert wf.status == WorkflowExecution.Status.CANCELED
    tasks = ActivityTask.objects.filter(execution=wf)
    assert tasks and all(t.status == ActivityTask.Status.FAILED for t in tasks)

