"""Run real competing transactions on PostgreSQL (SQLite has no row locks)."""
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from threading import Barrier

import pytest
from django.core.management import call_command
from django.db import connection, connections
from django.utils import timezone

from django_durable.engine import step_workflow
from django_durable.models import ActivityTask, WorkflowExecution
from testproj.durable_workflows import e2e_flow

pytestmark = pytest.mark.skipif(connection.vendor != 'postgresql', reason='requires PostgreSQL row locks')


@pytest.fixture(autouse=True)
def database():
    call_command('migrate', verbosity=0)
    call_command('flush', interactive=False, verbosity=0)


def race(*operations):
    barrier = Barrier(len(operations))
    def run(operation):
        try:
            barrier.wait(timeout=10)
            return operation()
        finally:
            connections.close_all()
    with ThreadPoolExecutor(max_workers=len(operations)) as pool:
        futures = [pool.submit(run, op) for op in operations]
        return [future.result(timeout=15) for future in futures]


def test_competing_claims_start_only_one_attempt():
    wf = WorkflowExecution.objects.create(workflow_name='wf', status='RUNNING')
    task = ActivityTask.objects.create(execution=wf, activity_name='activity')
    def claim():
        return ActivityTask.objects.get(pk=task.pk).start()
    assert sorted(race(claim, claim)) == [False, True]
    task.refresh_from_db()
    assert task.attempt == 1
    assert task.lease_token is not None


def test_competing_workflow_replays_schedule_one_activity():
    wf = WorkflowExecution.objects.create(workflow_name=e2e_flow._durable_name, input={'value': 42})
    race(lambda: step_workflow(wf), lambda: step_workflow(wf))
    assert wf.activities.count() == 1
    assert wf.history.filter(type='activity_scheduled').count() == 1


def test_competing_completions_commit_one_history_event():
    wf = WorkflowExecution.objects.create(workflow_name='wf', status='RUNNING')
    task = ActivityTask.objects.create(execution=wf, activity_name='activity')
    assert task.start()
    # Separate snapshots of the same attempt, as seen by competing processes.
    first = ActivityTask.objects.get(pk=task.pk)
    second = ActivityTask.objects.get(pk=task.pk)
    assert sorted(race(lambda: first.mark_completed('first'), lambda: second.mark_completed('second'))) == [False, True]
    task.refresh_from_db()
    wf.refresh_from_db()
    assert task.result in ('first', 'second')
    assert wf.status == 'PENDING'
    assert wf.history.filter(type='activity_completed').count() == 1


def test_expiry_and_late_completion_cannot_both_commit():
    wf = WorkflowExecution.objects.create(workflow_name='wf', status='RUNNING')
    task = ActivityTask.objects.create(execution=wf, activity_name='activity')
    assert task.start()
    ActivityTask.objects.filter(pk=task.pk).update(expires_at=timezone.now()-timedelta(seconds=1))
    first = ActivityTask.objects.get(pk=task.pk)
    second = ActivityTask.objects.get(pk=task.pk)
    race(lambda: first.mark_completed('late'), second.mark_timed_out)
    task.refresh_from_db()
    assert task.status == 'TIMED_OUT'
    assert wf.history.filter(type='activity_timed_out').count() == 1
    assert not wf.history.filter(type='activity_completed').exists()


def test_signal_and_cancellation_cannot_resurrect_workflow():
    wf = WorkflowExecution.objects.create(workflow_name='wf', status='RUNNING')
    first = WorkflowExecution.objects.get(pk=wf.pk)
    second = WorkflowExecution.objects.get(pk=wf.pk)
    race(lambda: first.enqueue_signal('go'), second.cancel)
    wf.refresh_from_db()
    assert wf.status == 'CANCELED'
