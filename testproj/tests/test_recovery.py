"""Regression tests for durable state transitions, recovery, and deadlines."""
import os
from datetime import timedelta
from unittest.mock import patch

import django
import pytest

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'testproj.settings')
django.setup()

from django.core.management import call_command
from django.utils import timezone

from django_durable import register, run_workflow, signal_workflow, start_workflow
from django_durable.engine import Context, _current_activity, activity_heartbeat, execute_activity, step_workflow
from django_durable.exceptions import ActivityCanceled, ActivityError, ActivityTimeout, WaitActivityTimeout, WaitWorkflowTimeout, WorkflowTimeout
from django_durable.management.commands.durable_worker import Command
from django_durable.models import ActivityTask, HistoryEvent, WorkflowExecution
from django_durable.retry import RetryPolicy


@pytest.fixture(autouse=True)
def database():
    call_command('migrate', verbosity=0)
    call_command('flush', interactive=False, verbosity=0)


def start(fn, **kwargs):
    wf = WorkflowExecution.objects.get(pk=start_workflow(fn, **kwargs))
    step_workflow(wf)
    wf.refresh_from_db()
    return wf


@register.activity()
def answer():
    return 42


@register.workflow()
def answer_flow(ctx):
    return ctx.run_activity(answer)


def test_none_return_completes_and_notifies_parent():
    @register.workflow()
    def child(ctx):
        pass

    @register.workflow()
    def parent(ctx):
        assert ctx.run_workflow(child) is None
        return 'done'

    assert run_workflow(child) is None
    assert run_workflow(parent) == 'done'


def test_exhausted_activity_wakes_workflow_and_can_be_caught():
    @register.activity(retry_policy=RetryPolicy(maximum_attempts=1))
    def broken():
        raise ValueError('broken')

    @register.workflow()
    def recover(ctx):
        try:
            ctx.run_activity(broken)
        except ActivityError:
            return 'recovered'

    wf = start(recover)
    execute_activity(wf.activities.get())
    wf.refresh_from_db()
    assert wf.status == 'PENDING'
    step_workflow(wf)
    wf.refresh_from_db()
    assert wf.status == 'COMPLETED'
    assert wf.result == 'recovered'


@pytest.mark.parametrize('transition', ['complete', 'fail', 'timeout'])
def test_terminal_transition_rolls_back_if_history_write_crashes(transition):
    wf = start(answer_flow)
    task = wf.activities.get()
    assert task.start()
    token = task.lease_token

    class Crash(BaseException):
        pass

    with patch.object(HistoryEvent.objects, 'create', side_effect=Crash):
        with pytest.raises(Crash):
            if transition == 'complete':
                task.mark_completed(42)
            elif transition == 'fail':
                task.mark_failed('broken')
            else:
                task.mark_timed_out()
    task.refresh_from_db()
    wf.refresh_from_db()
    assert task.status == 'RUNNING'
    assert task.lease_token == token
    assert wf.status == 'RUNNING'
    assert not wf.history.filter(type__in=['activity_completed', 'activity_failed', 'activity_timed_out']).exists()


def test_completion_rolls_back_if_wakeup_fails():
    from django.db.models.query import QuerySet
    wf = start(answer_flow)
    task = wf.activities.get()
    assert task.start()
    update = QuerySet.update

    def fail_wakeup(qs, **kwargs):
        if qs.model is WorkflowExecution:
            raise RuntimeError('database unavailable')
        return update(qs, **kwargs)

    with patch.object(QuerySet, 'update', fail_wakeup):
        with pytest.raises(RuntimeError, match='database unavailable'):
            task.mark_completed(42)
    task.refresh_from_db()
    assert task.status == 'RUNNING'
    assert not wf.history.filter(type='activity_completed').exists()


def test_expired_lease_recovers_default_activity_and_fences_old_attempt():
    wf = start(answer_flow)
    old = wf.activities.get()
    assert old.start()
    ActivityTask.objects.filter(pk=old.pk).update(lease_expires_at=timezone.now()-timedelta(seconds=1))
    Command()._process_timeouts(timezone.now(), 100)
    replacement = wf.activities.get()
    assert replacement.status == 'QUEUED'
    assert replacement.lease_token is None
    ActivityTask.objects.filter(pk=old.pk).update(after_time=timezone.now())
    assert replacement.start()
    assert replacement.attempt == 2
    assert replacement.lease_token != old.lease_token
    assert not old.mark_completed('stale')
    assert not old.retry_or_fail('stale failure')
    assert not old.mark_timed_out()
    assert not old.renew_lease()
    _current_activity.task_id = str(old.pk)
    _current_activity.lease_token = old.lease_token
    try:
        with pytest.raises(ActivityCanceled):
            activity_heartbeat({'stale': True})
    finally:
        _current_activity.task_id = None
        _current_activity.lease_token = None
    assert replacement.mark_completed(42)
    assert wf.history.filter(type='activity_completed').count() == 1
    step_workflow(wf)
    wf.refresh_from_db()
    assert wf.result == 42


def test_follower_death_requeues_owned_task():
    from unittest.mock import Mock
    wf = start(answer_flow)
    task = wf.activities.get()
    assert task.start()
    proc = Mock()
    proc.poll.return_value = 1
    running = [{'type': 'activity', 'id': task.pk, 'token': str(task.lease_token), 'proc': proc}]
    cmd = Command()
    with patch.object(cmd, '_respawn_follower'):
        cmd._handle_running_processes(running, [], 100, timezone.now())
    task.refresh_from_db()
    assert task.status == 'QUEUED'
    assert running == []


def test_heartbeat_retry_fences_old_attempt_and_failure_is_catchable():
    wf = start(answer_flow)
    old = wf.activities.get()
    assert old.start()
    ActivityTask.objects.filter(pk=old.pk).update(heartbeat_timeout=1, heartbeat_at=timezone.now()-timedelta(seconds=2))
    Command()._heartbeat_timeouts(timezone.now(), 100)
    assert not old.mark_completed('stale')
    current = wf.activities.get()
    assert current.status == 'QUEUED'
    assert not wf.history.filter(type='activity_completed').exists()


def test_renewed_heartbeat_is_not_expired_from_stale_snapshot():
    wf = start(answer_flow)
    task = wf.activities.get()
    assert task.start()
    task.heartbeat_timeout = 1
    task.heartbeat_at = timezone.now()-timedelta(seconds=2)
    task.save()
    ActivityTask.objects.filter(pk=task.pk).update(heartbeat_at=timezone.now())
    assert not Command()._handle_heartbeat_timeout(task, timezone.now())
    task.refresh_from_db()
    assert task.status == 'RUNNING'


@pytest.mark.parametrize('attempt', [0, 1, 3])
def test_schedule_to_close_is_terminal_even_with_unlimited_retries(attempt):
    wf = start(answer_flow)
    task = wf.activities.get()
    task.attempt = attempt
    task.expires_at = timezone.now()-timedelta(seconds=1)
    task.save()
    cmd = Command()
    for _ in range(3):
        cmd._process_timeouts(timezone.now(), 100)
    task.refresh_from_db()
    wf.refresh_from_db()
    assert task.status == 'TIMED_OUT'
    assert wf.status == 'PENDING'
    assert wf.history.filter(type='activity_timed_out').count() == 1


def test_late_result_cannot_complete_expired_activity():
    wf = start(answer_flow)
    task = wf.activities.get()
    assert task.start()
    ActivityTask.objects.filter(pk=task.pk).update(expires_at=timezone.now()-timedelta(seconds=1))
    task.mark_completed(42)
    task.refresh_from_db()
    assert task.status == 'TIMED_OUT'
    assert not wf.history.filter(type='activity_completed').exists()


@pytest.mark.parametrize('kind', ['activity', 'child'])
def test_wait_deadline_wakes_workflow_and_survives_late_result(kind):
    @register.workflow()
    def child(ctx):
        ctx.wait_signal('child-go')
        return 42

    @register.workflow()
    def parent(ctx):
        try:
            if kind == 'activity':
                handle = ctx.start_activity(answer)
                result = ctx.wait_activity(handle, timeout=10)
            else:
                handle = ctx.start_workflow(child)
                result = ctx.wait_workflow(handle, timeout=10)
        except (WaitActivityTimeout, WaitWorkflowTimeout):
            result = 'timeout'
        ctx.wait_signal('parent-go')
        return result

    wf = start(parent)
    assert wf.wake_at is not None
    wait_type = 'activity_wait' if kind == 'activity' else 'child_workflow_wait'
    HistoryEvent.objects.filter(execution=wf, type=wait_type).update(created_at=timezone.now()-timedelta(seconds=20))
    WorkflowExecution.objects.filter(pk=wf.pk).update(wake_at=timezone.now()-timedelta(seconds=1))
    Command()._process_timeouts(timezone.now(), 100)
    wf.refresh_from_db()
    assert wf.status == 'PENDING'
    step_workflow(wf)
    wf.refresh_from_db()
    assert wf.wake_at is None
    if kind == 'activity':
        execute_activity(wf.activities.get())
    else:
        child_wf = wf.children.get()
        signal_workflow(child_wf, 'child-go')
        step_workflow(child_wf)
    signal_workflow(wf, 'parent-go')
    step_workflow(wf)
    wf.refresh_from_db()
    assert wf.status == 'COMPLETED'
    assert wf.result == 'timeout'


def test_result_before_wait_deadline_wins_even_on_late_replay():
    wf = start(answer_flow)
    task = wf.activities.get()
    execute_activity(task)
    event = wf.history.get(type='activity_wait')
    event.details = {'timeout': 10}
    event.save()
    with patch('django_durable.engine.timezone.now', return_value=timezone.now()+timedelta(seconds=20)):
        assert Context(wf).wait_activity(task.pos, timeout=10) == 42


def test_sync_run_honors_already_expired_workflow():
    with pytest.raises(WorkflowTimeout):
        run_workflow(answer_flow, timeout=-1)
    assert not ActivityTask.objects.exists()


def test_sync_run_honors_wait_deadline_without_external_worker():
    @register.workflow()
    def parent(ctx):
        handle = ctx.start_activity('__sleep__', 10)
        try:
            ctx.wait_activity(handle, timeout=0.01)
        except WaitActivityTimeout:
            return 'timeout'
    assert run_workflow(parent) == 'timeout'


def test_sync_run_honors_workflow_timeout_while_waiting():
    @register.workflow()
    def waiting(ctx):
        return ctx.wait_signal('never')
    with pytest.raises(WorkflowTimeout):
        run_workflow(waiting, timeout=0.01)


def test_workflow_timeout_atomically_finishes_tasks_and_notifies_parent():
    parent = WorkflowExecution.objects.create(workflow_name='parent', status='RUNNING')
    child = WorkflowExecution.objects.create(workflow_name='child', parent=parent, parent_pos=0)
    task = ActivityTask.objects.create(execution=child, activity_name=answer._durable_name)
    assert task.start()
    assert child.time_out()
    assert not child.time_out()
    parent.refresh_from_db()
    task.refresh_from_db()
    assert task.status == 'FAILED'
    assert parent.status == 'PENDING'
    assert parent.history.filter(type='child_workflow_timed_out').count() == 1


def test_cancel_fences_running_activity():
    wf = start(answer_flow)
    task = wf.activities.get()
    assert task.start()
    wf.cancel()
    assert not task.mark_completed(42)
    task.refresh_from_db()
    assert task.status == 'FAILED'


def test_early_signal_does_not_disable_new_patch():
    @register.workflow()
    def patched(ctx):
        return {'new': ctx.patched('feature'), 'payload': ctx.wait_signal('go')}
    wf = WorkflowExecution.objects.get(pk=start_workflow(patched))
    signal_workflow(wf, 'go', 42)
    step_workflow(wf)
    wf.refresh_from_db()
    assert wf.result == {'new': True, 'payload': 42}


@pytest.mark.parametrize('follower_alive', [True, False])
def test_terminal_workflow_does_not_leave_a_claimed_activity_running(follower_alive):
    from unittest.mock import Mock
    wf = start(answer_flow)
    task = wf.activities.get()
    assert task.start()
    WorkflowExecution.objects.filter(pk=wf.pk).update(status='FAILED')
    proc = Mock()
    proc.poll.return_value = None if follower_alive else 1
    running = [{'type': 'activity', 'id': task.pk, 'token': str(task.lease_token), 'proc': proc}]
    cmd = Command()
    with patch.object(cmd, '_respawn_follower'):
        cmd._handle_running_processes(running, [], 100, timezone.now())
    task.refresh_from_db()
    assert running == []
    assert task.status == 'FAILED'
    assert task.error == 'workflow_not_runnable'
    assert wf.history.filter(type='activity_failed').count() == 1


@pytest.mark.parametrize('kind', ['activity', 'workflow'])
def test_repeated_waits_have_independent_deadlines_and_replay(kind):
    @register.workflow()
    def child(ctx):
        return 42

    @register.workflow()
    def repeated(ctx):
        if kind == 'activity':
            handle = ctx.start_activity(answer)
            wait = ctx.wait_activity
            exc = WaitActivityTimeout
        else:
            handle = ctx.start_workflow(child)
            wait = ctx.wait_workflow
            exc = WaitWorkflowTimeout
        try:
            wait(handle, timeout=0)
        except exc:
            pass
        result = wait(handle, timeout=10)
        # Wait calls must not shift subsequent command positions.
        assert ctx.start_activity(answer) == 1
        return result

    wf = start(repeated)
    assert wf.status == 'RUNNING'
    assert wf.wake_at > timezone.now()
    wait_type = 'activity_wait' if kind == 'activity' else 'child_workflow_wait'
    waits = list(wf.history.filter(type=wait_type).order_by('id'))
    assert [e.details['timeout'] for e in waits] == [0, 10]
    # Replay without a result preserves both deadlines and creates no more waits.
    WorkflowExecution.objects.filter(pk=wf.pk).update(status='PENDING')
    step_workflow(wf)
    assert wf.history.filter(type=wait_type).count() == 2
    if kind == 'activity':
        execute_activity(wf.activities.get())
    else:
        step_workflow(wf.children.get())
    step_workflow(wf)
    wf.refresh_from_db()
    assert wf.status == 'COMPLETED'
    assert wf.result == 42
    assert wf.history.filter(type=wait_type).count() == 2


@pytest.mark.parametrize('fail', [False, True])
@pytest.mark.parametrize('claimed', [False, True])
def test_workflow_terminal_commit_finishes_outstanding_activities(fail, claimed):
    @register.workflow()
    def abandon(ctx):
        ctx.start_activity(answer)
        if fail:
            raise ValueError('workflow failed')
        return 7

    wf = WorkflowExecution.objects.get(pk=start_workflow(abandon))
    # Schedule ahead of replay to also cover an already claimed attempt.
    Context(wf).start_activity(answer)
    task = wf.activities.get()
    if claimed:
        assert task.start()
    step_workflow(wf)
    wf.refresh_from_db()
    task.refresh_from_db()
    assert wf.status == ('FAILED' if fail else 'COMPLETED')
    assert task.status == 'FAILED'
    assert task.error == 'workflow_not_runnable'
    assert task.finished_at == wf.finished_at
    assert task.lease_expires_at is None
    assert wf.history.filter(type='activity_failed', pos=task.pos).count() == 1
    assert not task.mark_completed('late')


def test_workflow_terminal_cleanup_is_atomic():
    @register.workflow()
    def abandon(ctx):
        ctx.start_activity(answer)
        return 7

    wf = WorkflowExecution.objects.get(pk=start_workflow(abandon))
    class Crash(BaseException):
        pass
    with patch.object(ActivityTask, 'mark_failed', side_effect=Crash):
        with pytest.raises(Crash):
            step_workflow(wf)
    wf.refresh_from_db()
    assert wf.status == 'PENDING'
    assert not wf.activities.exists()
    assert not wf.history.exists()


@pytest.mark.parametrize('kind', ['activity', 'workflow'])
def test_migration_requeues_legacy_timed_waits(kind):
    from django.db import connection
    from django.db.migrations.executor import MigrationExecutor

    @register.workflow()
    def child(ctx):
        return 42

    @register.workflow()
    def waiting(ctx):
        if kind == 'activity':
            return ctx.wait_activity(ctx.start_activity(answer), timeout=10)
        return ctx.wait_workflow(ctx.start_workflow(child), timeout=10)

    wf = start(waiting)
    # Preserve the historical records but remove metadata absent before 0008.
    for event in wf.history.filter(type__in=['activity_wait', 'child_workflow_wait']):
        event.details.pop('wait_index', None)
        event.details.pop('handle', None)
        event.save(update_fields=['details'])
    old = [('django_durable', '0007_historyevent_textchoices_unique')]
    latest = [('django_durable', '0008_activity_leases_and_wait_deadlines')]
    executor = MigrationExecutor(connection)
    try:
        executor.migrate(old)
        executor = MigrationExecutor(connection)
        historical = executor.loader.project_state(old).apps.get_model('django_durable', 'WorkflowExecution')
        historical.objects.filter(pk=wf.pk).update(status='RUNNING')
        completed = historical.objects.create(workflow_name='old', status='COMPLETED')
        executor.migrate(latest)
        wf.refresh_from_db()
        assert wf.status == 'PENDING'
        assert wf.wake_at is None
        assert WorkflowExecution.objects.get(pk=completed.pk).status == 'COMPLETED'
        step_workflow(wf)
        wf.refresh_from_db()
        assert wf.status == 'RUNNING'
        assert wf.wake_at is not None
        Command()._process_timeouts(wf.wake_at + timedelta(seconds=1), 100)
        wf.refresh_from_db()
        assert wf.status == 'PENDING'
    finally:
        MigrationExecutor(connection).migrate(latest)


@pytest.mark.parametrize('retiring', [False, True])
def test_fragmented_control_ack_never_blocks_or_reuses_retiring_follower(retiring):
    import json
    from unittest.mock import Mock

    read_fd, write_fd = os.pipe()
    proc = Mock()
    proc.poll.return_value = None
    proc.control = os.fdopen(read_fd, 'rb', buffering=0)
    os.set_blocking(read_fd, False)
    proc.control_buffer = b''
    info = {'type': 'activity', 'id': 123, 'token': 'owned', 'proc': proc}
    running, idle = [info], []
    cmd = Command()
    ack = json.dumps({'ok': True, 'id': 123, 'token': 'owned', 'retiring': retiring}).encode() + b'\n'
    try:
        # A read-ready channel does not imply a complete frame is available.
        os.write(write_fd, ack[:10])
        with patch.object(cmd, '_close_process') as close, patch.object(cmd, '_respawn_follower') as spawn:
            assert not cmd._refresh_idle_processes(idle, running, 1)
            assert running == [info]
            assert not idle
            os.write(write_fd, ack[10:])
            assert cmd._refresh_idle_processes(idle, running, 1)
            assert not running
            if retiring:
                assert not idle
                close.assert_called_once_with(proc)
                spawn.assert_called_once_with(idle, 1)
            else:
                assert idle == [proc]
                close.assert_not_called()
                spawn.assert_not_called()
    finally:
        os.close(write_fd)
        proc.control.close()


@pytest.mark.parametrize('kind', ['activity', 'child'])
@pytest.mark.parametrize('repeated', [False, True])
@pytest.mark.parametrize('legacy', [False, True])
def test_inserted_patch_before_existing_wait_preserves_old_path(kind, repeated, legacy):
    @register.workflow()
    def child(ctx):
        return 42

    changed = False
    @register.workflow()
    def evolving(ctx):
        if kind == 'activity':
            handle = ctx.start_activity(answer)
            wait, exc = ctx.wait_activity, WaitActivityTimeout
        else:
            handle = ctx.start_workflow(child)
            wait, exc = ctx.wait_workflow, WaitWorkflowTimeout
        if repeated:
            try:
                wait(handle, timeout=0)
            except exc:
                pass
        if changed and ctx.patched('before-wait'):
            return 'WRONG_NEW_PATH'
        return wait(handle, timeout=10)

    wf = start(evolving)
    if legacy:
        for event in wf.history.filter(type__in=['activity_wait', 'child_workflow_wait']):
            event.details.pop('command_pos', None)
            event.save(update_fields=['details'])
    changed = True
    WorkflowExecution.objects.filter(pk=wf.pk).update(status='PENDING')
    step_workflow(wf)
    wf.refresh_from_db()
    assert wf.status == 'RUNNING', wf.result
    assert not wf.history.filter(type='version_marker').exists()
    if kind == 'activity':
        execute_activity(wf.activities.get())
    else:
        step_workflow(wf.children.get())
    step_workflow(wf)
    wf.refresh_from_db()
    assert wf.result == 42
    # A wait reached earlier in this replay must not suppress a genuinely new patch.
    fresh = start(evolving)
    assert fresh.result == 'WRONG_NEW_PATH'


@pytest.mark.parametrize('kind', ['activity', 'workflow'])
def test_failed_dispatch_closes_real_broken_pipe_and_recovers(kind):
    import subprocess
    import sys
    from unittest.mock import Mock

    wf = start(answer_flow) if kind == 'activity' else WorkflowExecution.objects.get(pk=start_workflow(answer_flow))
    # An idle follower has exited after its last poll, leaving its write pipe open.
    proc = subprocess.Popen([sys.executable, '-c', 'pass'], stdin=subprocess.PIPE, text=True)
    read_fd, write_fd = os.pipe()
    os.close(write_fd)
    proc.control = os.fdopen(read_fd, 'rb', buffering=0)
    proc.wait(timeout=5)
    idle, running = [proc], []
    replacement = Mock()
    cmd = Command()
    try:
        with patch.object(cmd, '_spawn_follower_proc', return_value=replacement):
            dispatch = cmd._dispatch_due_activities if kind == 'activity' else cmd._dispatch_runnable_workflows
            dispatch(timezone.now(), 10, idle, running, 100)
        assert proc.stdin.closed
        assert proc.control.closed
        assert idle == [replacement]
        assert not running
        if kind == 'activity':
            task = wf.activities.get()
            assert task.status == 'QUEUED'
            assert task.lease_token is None
            assert task.lease_expires_at is None
        else:
            wf.refresh_from_db()
            assert wf.status == 'PENDING'
    finally:
        # Also close the control FD on the deliberately broken implementation.
        proc.control.close()
        try:
            proc.stdin.close()
        except OSError:
            pass


def test_exhausted_lease_failover_retires_original_hung_follower():
    from unittest.mock import Mock

    wf = start(answer_flow)
    task = wf.activities.get()
    task.retry_policy = {'maximum_attempts': 1}
    task.max_attempts = 1
    task.save()
    assert task.start()
    token = str(task.lease_token)
    ActivityTask.objects.filter(pk=task.pk).update(lease_expires_at=timezone.now()-timedelta(seconds=1))
    # Another dispatcher recovers the lease while the original follower is hung.
    Command()._process_timeouts(timezone.now(), 100)
    task.refresh_from_db()
    assert task.status == 'FAILED'
    wf.refresh_from_db()
    assert wf.status == 'PENDING'
    proc, replacement = Mock(), Mock()
    proc.poll.return_value = None
    running = [{'type': 'activity', 'id': task.pk, 'token': token, 'proc': proc}]
    idle = []
    owner = Command()
    with patch.object(owner, '_spawn_follower_proc', return_value=replacement):
        assert owner._handle_running_processes(running, idle, 100, timezone.now())
    assert not running
    assert idle == [replacement]
    proc.kill.assert_called_once()
    proc.control.close.assert_called_once()
    assert wf.history.filter(type='activity_failed').count() == 1


@pytest.mark.parametrize('operation', ['timeout', 'cancel'])
@pytest.mark.parametrize('failure', ['database', 'crash'])
def test_cascade_failure_rolls_back_entire_tree_and_can_be_retried(operation, failure):
    from django.db import DatabaseError

    class Crash(BaseException):
        pass

    root = WorkflowExecution.objects.create(
        workflow_name='root', status='RUNNING',
        expires_at=timezone.now()-timedelta(seconds=1),
    )
    first = WorkflowExecution.objects.create(workflow_name='first', parent=root, parent_pos=0)
    second = WorkflowExecution.objects.create(workflow_name='second', parent=root, parent_pos=1, status='RUNNING')
    grandchild = WorkflowExecution.objects.create(workflow_name='grandchild', parent=second, parent_pos=0, status='RUNNING')
    waiting = WorkflowExecution.objects.create(workflow_name='waiting', parent=second, parent_pos=1, status='WAITING')
    tree = [root, first, second, grandchild, waiting]
    original_statuses = [wf.status for wf in tree]
    for wf in tree:
        ActivityTask.objects.create(execution=wf, activity_name=answer._durable_name)
    claimed = second.activities.get()
    assert claimed.start()
    token = claimed.lease_token
    original_cancel = WorkflowExecution.cancel
    error = DatabaseError if failure == 'database' else Crash

    def fail_at_grandchild(wf, reason=None):
        if wf.pk == grandchild.pk:
            raise error('cascade interrupted')
        return original_cancel(wf, reason=reason)

    def transition():
        if operation == 'timeout':
            Command()._timeout_workflows(timezone.now(), 100)
        else:
            root.cancel()

    with patch.object(WorkflowExecution, 'cancel', fail_at_grandchild):
        with pytest.raises(error):
            transition()
    for wf, status in zip(tree, original_statuses):
        wf.refresh_from_db()
        assert wf.status == status
    assert not HistoryEvent.objects.exists()
    claimed.refresh_from_db()
    assert claimed.status == 'RUNNING'
    assert claimed.lease_token == token
    assert ActivityTask.objects.filter(status='QUEUED').count() == 4

    # The same timeout scan/cancel can retry every transition after rollback.
    transition()
    transition()
    root.refresh_from_db()
    assert root.status == ('TIMED_OUT' if operation == 'timeout' else 'CANCELED')
    for wf in tree[1:]:
        wf.refresh_from_db()
        assert wf.status == 'CANCELED'
        assert wf.history.filter(type='workflow_canceled').count() == 1
    assert ActivityTask.objects.filter(status='FAILED').count() == len(tree)
    assert HistoryEvent.objects.filter(type='activity_failed').count() == len(tree)


def test_undelivered_single_attempt_dispatch_preserves_retry_budget():
    import subprocess
    import sys
    from unittest.mock import Mock

    wf = start(answer_flow)
    wf.activities.update(max_attempts=1, retry_policy={'maximum_attempts': 1})
    proc = subprocess.Popen([sys.executable, '-c', 'pass'], stdin=subprocess.PIPE, text=True)
    read_fd, write_fd = os.pipe()
    os.close(write_fd)
    proc.control = os.fdopen(read_fd, 'rb', buffering=0)
    proc.wait(timeout=5)
    idle, running = [proc], []
    cmd = Command()
    try:
        with patch.object(cmd, '_spawn_follower_proc', return_value=Mock()):
            cmd._dispatch_due_activities(timezone.now(), 10, idle, running, 100)
        task = wf.activities.get()
        assert task.status == 'QUEUED'
        assert task.attempt == 0
        assert task.lease_token is None
        assert task.lease_expires_at is None
        assert task.after_time <= timezone.now()
        assert not wf.history.filter(type='activity_failed').exists()
        execute_activity(task)
        task.refresh_from_db()
        assert task.status == 'COMPLETED'
        assert task.attempt == 1
        step_workflow(wf)
        wf.refresh_from_db()
        assert wf.result == 42
    finally:
        proc.control.close()
        try:
            proc.stdin.close()
        except OSError:
            pass


@pytest.mark.parametrize('previous_attempts', [0, 2])
def test_claim_release_is_idempotent_and_cannot_touch_a_replacement(previous_attempts):
    wf = start(answer_flow)
    wf.activities.update(attempt=previous_attempts)
    claim = wf.activities.get()
    assert claim.start()
    stale = wf.activities.get()
    after_time = claim.after_time
    assert claim.release_unstarted_claim()
    assert claim.status == 'QUEUED'
    assert claim.attempt == previous_attempts
    assert claim.after_time == after_time
    assert not stale.release_unstarted_claim()
    replacement = wf.activities.get()
    assert replacement.start()
    assert replacement.attempt == previous_attempts + 1
    assert replacement.lease_token != stale.lease_token
    assert not stale.release_unstarted_claim()
    assert not stale.mark_completed('late')
    replacement.refresh_from_db()
    assert replacement.status == 'RUNNING'
    assert replacement.attempt == previous_attempts + 1
    assert not wf.history.filter(type__in=['activity_failed', 'activity_completed']).exists()


@pytest.mark.parametrize('completed', [False, True])
def test_ambiguous_dispatch_error_does_not_refund_started_execution(completed):
    from unittest.mock import Mock

    wf = start(answer_flow)
    wf.activities.update(max_attempts=1, retry_policy={'maximum_attempts': 1})
    proc = Mock()
    proc.poll.return_value = None
    def fail_after_receipt():
        claim = wf.activities.get()
        if completed:
            execute_activity(claim, claimed=True)
        else:
            wf.activities.update(started_at=timezone.now())
        raise BrokenPipeError('failure reported after receipt')
    proc.stdin.flush.side_effect = fail_after_receipt
    cmd = Command()
    with patch.object(cmd, '_spawn_follower_proc', return_value=Mock()):
        cmd._dispatch_due_activities(timezone.now(), 10, [proc], [], 100)
    task = wf.activities.get()
    assert task.attempt == 1
    assert task.status == ('COMPLETED' if completed else 'FAILED')
    event = 'activity_completed' if completed else 'activity_failed'
    assert wf.history.filter(type=event).count() == 1
