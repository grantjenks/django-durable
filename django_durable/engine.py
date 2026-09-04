import json
import threading
import time
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Callable

from django.db import DatabaseError, close_old_connections, transaction
from django.utils import timezone

from .constants import (
    FINAL_EVENT_POS,
    SLEEP_ACTIVITY_NAME,
    SPECIAL_EVENT_POS,
    ErrorCode,
    HistoryEventType,
)
from .exceptions import (
    ActivityCanceled,
    ActivityError,
    ActivityTimeout,
    NondeterminismError,
    UnknownActivityError,
    UnknownWorkflowError,
    WaitActivityTimeout,
    WaitWorkflowTimeout,
    WorkflowCanceled,
    WorkflowException,
    WorkflowTimeout,
)
from .models import ActivityTask, HistoryEvent, WorkflowExecution, lock_execution
from .registry import register

_current_activity = threading.local()
_PAUSED = object()


class NeedsPause(Exception):
    """Internal control-flow exception: workflow scheduled work and must pause."""


@dataclass
class Context:
    execution: WorkflowExecution
    pos: int = 0  # deterministic step counter

    def _bump(self) -> int:
        p = self.pos
        self.pos += 1
        return p

    def get_version(self, change_id: str, version: int) -> int:
        """Record or retrieve a deterministic version marker.

        The first time a workflow execution reaches this call position it
        appends a ``version_marker`` HistoryEvent storing ``change_id`` and
        ``version``.  On replay the stored value is returned ensuring the
        workflow continues executing the same code path even if the workflow
        definition changes between deployments.
        """

        pos = self._bump()
        ev = (
            HistoryEvent.objects.filter(
                execution=self.execution,
                pos=pos,
                type=HistoryEventType.VERSION_MARKER.value,
            )
            .order_by('id')
            .last()
        )
        if ev:
            if ev.details.get('change_id') != change_id:
                raise NondeterminismError('Version marker does not match history')
            return ev.details.get('version')
        HistoryEvent.objects.create(
            execution=self.execution,
            type=HistoryEventType.VERSION_MARKER.value,
            pos=pos,
            details={'change_id': change_id, 'version': version},
        )
        return version

    def patched(self, change_id: str) -> bool:
        """Return whether to take the new code path for a given change.

        Temporal's ``Workflow.patched`` helper evaluates to ``True`` only for
        workflows that have not previously executed beyond the patched
        location.  Existing executions that have already progressed past the
        patched section will receive ``False`` on replay so that they continue
        the old logic.

        This method mirrors that behaviour by recording a version marker the
        first time it is reached.  If the workflow history already contains
        events beyond the current call position, we treat the execution as
        replaying and return ``False`` while recording version ``0``.  New
        executions record version ``1`` and return ``True``.
        """

        ev = (
            HistoryEvent.objects.filter(
                execution=self.execution,
                pos=self.pos,
                type=HistoryEventType.VERSION_MARKER.value,
            )
            .order_by('id')
            .last()
        )
        if ev:
            if ev.details.get('change_id') != f'patch:{change_id}':
                raise NondeterminismError('Patch marker does not match history')
            self.pos += 1
            return ev.details.get('version', 0) >= 1

        # If there are existing events at or beyond the current position (other
        # than the workflow start), this execution has already advanced past
        # the patched section and should continue the old code path without
        # consuming a new position.
        has_current = HistoryEvent.objects.filter(
            execution=self.execution,
            pos=self.pos,
        ).exclude(type=HistoryEventType.WORKFLOW_STARTED.value)
        has_future = HistoryEvent.objects.filter(
            execution=self.execution,
            pos__gt=self.pos,
            pos__lt=SPECIAL_EVENT_POS,
        )
        if has_current.exists() or has_future.exists():
            return False

        pos = self._bump()
        HistoryEvent.objects.create(
            execution=self.execution,
            type=HistoryEventType.VERSION_MARKER.value,
            pos=pos,
            details={'change_id': f'patch:{change_id}', 'version': 1},
        )
        return True

    def deprecate_patch(self, change_id: str):
        """Record that a previously patched section has been removed.

        This is a no-op for the workflow logic but ensures a version marker
        exists so that replay of historical executions remains deterministic
        after the patch is removed from code.
        """

        self.get_version(f'patch:{change_id}', 1)

    def start_activity(self, name: str | Callable, *args, **kwargs) -> int:
        """Schedule an activity and return its handle."""
        if not isinstance(name, str):
            name = getattr(name, '_durable_name')
        pos = self._bump()
        ev = (
            HistoryEvent.objects.filter(
                execution=self.execution,
                pos=pos,
                type=HistoryEventType.ACTIVITY_SCHEDULED.value,
            )
            .order_by('id')
            .last()
        )
        input_json = json.dumps({'args': args, 'kwargs': kwargs}, sort_keys=True)
        if ev:
            if (
                ev.details.get('activity_name') != name
                or ev.details.get('input') != input_json
            ):
                raise NondeterminismError('Activity inputs do not match history')
        else:
            with transaction.atomic():
                timeout = kwargs.pop('schedule_to_close_timeout', None)
                heartbeat = kwargs.pop('heartbeat_timeout', None)
                fn = register.activities.get(name)
                policy_obj = getattr(fn, '_durable_retry_policy', None) if fn else None
                policy_dict = (
                    policy_obj.asdict() if policy_obj else {'maximum_attempts': 0}
                )
                if timeout is None and fn is not None:
                    timeout = getattr(fn, '_durable_timeout', None)
                if heartbeat is None and fn is not None:
                    heartbeat = getattr(fn, '_durable_heartbeat_timeout', None)
                HistoryEvent.objects.create(
                    execution=self.execution,
                    type=HistoryEventType.ACTIVITY_SCHEDULED.value,
                    pos=pos,
                    details={
                        'activity_name': name,
                        'input': input_json,
                        'timeout': timeout,
                        'heartbeat_timeout': heartbeat,
                        'retry_policy': policy_dict,
                    },
                )
                after_time = timezone.now()
                if name == SLEEP_ACTIVITY_NAME:
                    try:
                        seconds = float((args or [0])[0])
                    except Exception:
                        seconds = 0.0
                    if seconds < 0:
                        seconds = 0.0
                    after_time = after_time + timedelta(seconds=seconds)
                expires_at = None
                if timeout is not None:
                    expires_at = timezone.now() + timedelta(seconds=float(timeout))

                ActivityTask.objects.create(
                    execution=self.execution,
                    activity_name=name,
                    pos=pos,
                    args=args,
                    kwargs=kwargs,
                    max_attempts=policy_dict.get('maximum_attempts', 0),
                    after_time=after_time,
                    expires_at=expires_at,
                    retry_policy=policy_dict,
                    heartbeat_timeout=heartbeat,
                )
        return pos

    def _check_wait_deadline(self, event, timeout, completed, exception):
        if event is None:
            return
        timeout = event.details.get('timeout', timeout)
        if timeout is None:
            return
        deadline = event.created_at + timedelta(seconds=float(timeout))
        # A result committed after the deadline must not change a timed-out
        # branch when the workflow replays later.
        if timezone.now() >= deadline and (
            completed is None or completed.created_at > deadline
        ):
            raise exception()
        if completed is None:
            wake_at = self.execution.wake_at
            if wake_at is None or deadline < wake_at:
                self.execution.wake_at = deadline

    def wait_activity(self, handle: int, timeout: float | None = None) -> Any:
        """Wait for a previously started activity and return its result."""
        pos = handle

        ev_done = (
            HistoryEvent.objects.filter(
                execution=self.execution,
                pos=pos,
                type__in=(
                    HistoryEventType.ACTIVITY_COMPLETED.value,
                    HistoryEventType.ACTIVITY_FAILED.value,
                    HistoryEventType.ACTIVITY_TIMED_OUT.value,
                    HistoryEventType.ACTIVITY_CANCELED.value,
                ),
            )
            .order_by('id')
            .last()
        )
        ev_wait = (
            HistoryEvent.objects.filter(
                execution=self.execution,
                pos=pos,
                type=HistoryEventType.ACTIVITY_WAIT.value,
            )
            .order_by('id')
            .last()
        )
        self._check_wait_deadline(ev_wait, timeout, ev_done, WaitActivityTimeout)
        if ev_done:
            if ev_done.type == HistoryEventType.ACTIVITY_FAILED.value:
                err = ev_done.details.get('error', ErrorCode.ACTIVITY_FAILED.value)
                raise ActivityError(RuntimeError(err))
            if ev_done.type == HistoryEventType.ACTIVITY_TIMED_OUT.value:
                err = ev_done.details.get('error', ErrorCode.ACTIVITY_TIMEOUT.value)
                raise ActivityTimeout(err)
            if ev_done.type == HistoryEventType.ACTIVITY_CANCELED.value:
                err = ev_done.details.get('error', ErrorCode.WORKFLOW_CANCELED.value)
                raise ActivityCanceled(err)
            return ev_done.details.get('result')

        scheduled = HistoryEvent.objects.filter(
            execution=self.execution,
            pos=pos,
            type=HistoryEventType.ACTIVITY_SCHEDULED.value,
        ).exists()
        if not scheduled:
            raise RuntimeError(f'Unknown activity handle {handle}')

        if not ev_wait:
            ev_wait = HistoryEvent.objects.create(
                execution=self.execution,
                type=HistoryEventType.ACTIVITY_WAIT.value,
                pos=pos,
                details={'timeout': timeout},
            )

        self._check_wait_deadline(ev_wait, timeout, None, WaitActivityTimeout)

        raise NeedsPause()

    def cancel_activity(self, handle: int, reason: str | None = None):
        """Cancel a previously scheduled activity if still queued."""
        pos = handle
        ev_done = (
            HistoryEvent.objects.filter(
                execution=self.execution,
                pos=pos,
                type__in=[
                    HistoryEventType.ACTIVITY_COMPLETED.value,
                    HistoryEventType.ACTIVITY_FAILED.value,
                    HistoryEventType.ACTIVITY_TIMED_OUT.value,
                    HistoryEventType.ACTIVITY_CANCELED.value,
                ],
            )
            .order_by('id')
            .last()
        )
        if ev_done:
            return
        scheduled = HistoryEvent.objects.filter(
            execution=self.execution,
            pos=pos,
            type=HistoryEventType.ACTIVITY_SCHEDULED.value,
        ).exists()
        if not scheduled:
            raise RuntimeError(f'Unknown activity handle {handle}')
        with transaction.atomic():
            ev_done = (
                HistoryEvent.objects.filter(
                    execution=self.execution,
                    pos=pos,
                    type__in=[
                        HistoryEventType.ACTIVITY_COMPLETED.value,
                        HistoryEventType.ACTIVITY_FAILED.value,
                        HistoryEventType.ACTIVITY_TIMED_OUT.value,
                        HistoryEventType.ACTIVITY_CANCELED.value,
                    ],
                )
                .order_by('id')
                .last()
            )
            if ev_done:
                return
            now = timezone.now()
            ActivityTask.objects.filter(
                execution=self.execution,
                pos=pos,
                status=ActivityTask.Status.QUEUED,
            ).update(
                status=ActivityTask.Status.FAILED,
                error=ErrorCode.WORKFLOW_CANCELED.value,
                finished_at=now,
                updated_at=now,
            )
            details = {'error': ErrorCode.WORKFLOW_CANCELED.value}
            if reason:
                details['reason'] = reason
            HistoryEvent.objects.create(
                execution=self.execution,
                type=HistoryEventType.ACTIVITY_CANCELED.value,
                pos=pos,
                details=details,
            )

    def run_activity(self, name: str | Callable, *args, **kwargs) -> Any:
        handle = self.start_activity(name, *args, **kwargs)
        return self.wait_activity(handle)

    def sleep(self, seconds: float):
        """Durable timer implemented as a special internal activity."""
        return self.run_activity(SLEEP_ACTIVITY_NAME, seconds)

    def wait_signal(self, name: str) -> Any:
        """Deterministic wait for an external signal.
        Behavior mirrors activities:
        - If a signal was already consumed at this position -> return payload.
        - If a matching enqueued signal exists -> consume it and return payload.
        - If waiting already recorded -> pause.
        - Else record wait and pause.
        """
        pos = self._bump()

        # 1) If already consumed for this pos, return payload
        ev_done = (
            HistoryEvent.objects.filter(
                execution=self.execution,
                pos=pos,
                type=HistoryEventType.SIGNAL_CONSUMED.value,
            )
            .order_by('id')
            .last()
        )
        if ev_done:
            return ev_done.details.get('payload')

        # 2) Try to consume an enqueued signal
        with transaction.atomic():
            # Double-check after acquiring transaction
            ev_done = (
                HistoryEvent.objects.filter(
                    execution=self.execution,
                    pos=pos,
                    type=HistoryEventType.SIGNAL_CONSUMED.value,
                )
                .order_by('id')
                .last()
            )
            if ev_done:
                return ev_done.details.get('payload')

            # Find earliest enqueued signal of this name not yet consumed
            enqueued = list(
                HistoryEvent.objects.filter(
                    execution=self.execution,
                    type=HistoryEventType.SIGNAL_ENQUEUED.value,
                    details__name=name,
                ).order_by('id')
            )
            enq = None
            if enqueued:
                # Build set of consumed enqueued_ids
                consumed_ids = set(
                    HistoryEvent.objects.filter(
                        execution=self.execution,
                        type=HistoryEventType.SIGNAL_CONSUMED.value,
                    ).values_list('details__enqueued_id', flat=True)
                )
                for e in enqueued:
                    if e.id not in consumed_ids:
                        enq = e
                        break

            if enq is not None:
                HistoryEvent.objects.create(
                    execution=self.execution,
                    type=HistoryEventType.SIGNAL_CONSUMED.value,
                    pos=pos,
                    details={
                        'name': name,
                        'payload': enq.details.get('payload'),
                        'enqueued_id': enq.id,
                    },
                )
                return enq.details.get('payload')

            # 3) Else record wait if first time, then pause
            waiting_exists = HistoryEvent.objects.filter(
                execution=self.execution,
                pos=pos,
                type=HistoryEventType.SIGNAL_WAIT.value,
            ).exists()
            if not waiting_exists:
                HistoryEvent.objects.create(
                    execution=self.execution,
                    type=HistoryEventType.SIGNAL_WAIT.value,
                    pos=pos,
                    details={'name': name},
                )
        raise NeedsPause()

    def start_workflow(
        self, name: str | Callable, timeout: float | None = None, **input
    ) -> str:
        """Schedule a child workflow and return its handle."""
        if not isinstance(name, str):
            name = getattr(name, '_durable_name')
        pos = self._bump()
        scheduled = (
            HistoryEvent.objects.filter(
                execution=self.execution,
                pos=pos,
                type=HistoryEventType.CHILD_WORKFLOW_SCHEDULED.value,
            )
            .order_by('id')
            .last()
        )
        if scheduled:
            return scheduled.details.get('child_id')
        fn = register.workflows.get(name)
        if fn is None:
            raise UnknownWorkflowError(name)
        with transaction.atomic():
            if timeout is None:
                timeout = getattr(fn, '_durable_timeout', None)
            expires_at = None
            if timeout is not None:
                expires_at = timezone.now() + timedelta(seconds=float(timeout))
            child = WorkflowExecution.objects.create(
                workflow_name=name,
                input=input,
                expires_at=expires_at,
                parent=self.execution,
                parent_pos=pos,
            )
            HistoryEvent.objects.create(
                execution=self.execution,
                type=HistoryEventType.CHILD_WORKFLOW_SCHEDULED.value,
                pos=pos,
                details={
                    'workflow_name': name,
                    'input': input,
                    'child_id': str(child.id),
                    'timeout': timeout,
                },
            )
        return str(child.id)

    def wait_workflow(self, handle: str, timeout: float | None = None) -> Any:
        """Wait for a previously started child workflow."""
        ev_done = (
            HistoryEvent.objects.filter(
                execution=self.execution,
                type__in=[
                    HistoryEventType.CHILD_WORKFLOW_COMPLETED.value,
                    HistoryEventType.CHILD_WORKFLOW_FAILED.value,
                    HistoryEventType.CHILD_WORKFLOW_CANCELED.value,
                    HistoryEventType.CHILD_WORKFLOW_TIMED_OUT.value,
                ],
                details__child_id=handle,
            )
            .order_by('id')
            .last()
        )
        ev_wait = (
            HistoryEvent.objects.filter(
                execution=self.execution,
                type=HistoryEventType.CHILD_WORKFLOW_WAIT.value,
                details__child_id=handle,
            )
            .order_by('id')
            .last()
        )
        self._check_wait_deadline(ev_wait, timeout, ev_done, WaitWorkflowTimeout)
        if ev_done:
            if ev_done.type == HistoryEventType.CHILD_WORKFLOW_FAILED.value:
                err = ev_done.details.get('error', ErrorCode.ACTIVITY_FAILED.value)
                if err == ErrorCode.WORKFLOW_TIMEOUT.value:
                    raise WorkflowTimeout(err)
                raise WorkflowException(err)
            if ev_done.type == HistoryEventType.CHILD_WORKFLOW_CANCELED.value:
                err = ev_done.details.get('error', ErrorCode.WORKFLOW_CANCELED.value)
                raise WorkflowCanceled(err)
            if ev_done.type == HistoryEventType.CHILD_WORKFLOW_TIMED_OUT.value:
                err = ev_done.details.get('error', ErrorCode.WORKFLOW_TIMEOUT.value)
                raise WorkflowTimeout(err)
            return ev_done.details.get('result')

        scheduled = HistoryEvent.objects.filter(
            execution=self.execution,
            type=HistoryEventType.CHILD_WORKFLOW_SCHEDULED.value,
            details__child_id=handle,
        ).exists()
        if not scheduled:
            raise RuntimeError(f'Unknown workflow handle {handle}')

        if not ev_wait:
            ev_wait = HistoryEvent.objects.create(
                execution=self.execution,
                type=HistoryEventType.CHILD_WORKFLOW_WAIT.value,
                pos=SPECIAL_EVENT_POS,
                details={'child_id': handle, 'timeout': timeout},
            )

        self._check_wait_deadline(ev_wait, timeout, None, WaitWorkflowTimeout)

        raise NeedsPause()

    def run_workflow(
        self, name: str | Callable, timeout: float | None = None, **input
    ) -> Any:
        handle = self.start_workflow(name, timeout=timeout, **input)
        return self.wait_workflow(handle)

    def cancel_workflow(self, handle: str, reason: str | None = None):
        """Cancel a previously started child workflow."""
        ev_done = (
            HistoryEvent.objects.filter(
                execution=self.execution,
                type__in=[
                    HistoryEventType.CHILD_WORKFLOW_COMPLETED.value,
                    HistoryEventType.CHILD_WORKFLOW_FAILED.value,
                    HistoryEventType.CHILD_WORKFLOW_CANCELED.value,
                    HistoryEventType.CHILD_WORKFLOW_TIMED_OUT.value,
                ],
                details__child_id=handle,
            )
            .order_by('id')
            .last()
        )
        if ev_done:
            return
        cancel_workflow(handle, reason=reason)


def _run_workflow_once(exec_obj: WorkflowExecution) -> Any | None:
    """Run the workflow function until it needs to pause or completes."""
    fn = register.workflows[exec_obj.workflow_name]
    ctx = Context(execution=exec_obj)
    # Always start from the beginning; deterministic API uses replay + event log.
    ctx.pos = 0
    try:
        return fn(ctx, **(exec_obj.input or {}))
    except NeedsPause:
        return _PAUSED


def _notify_parent(exec_obj: WorkflowExecution, event_type: str, details: dict):
    """Append an event to the parent workflow and mark it runnable."""
    if not exec_obj.parent_id:
        return
    parent = exec_obj.parent
    HistoryEvent.objects.create(
        execution=parent,
        type=event_type,
        pos=exec_obj.parent_pos or 0,
        details={'child_id': str(exec_obj.id), **details},
    )
    WorkflowExecution.objects.filter(
        pk=parent.pk,
        status__in=[
            WorkflowExecution.Status.PENDING,
            WorkflowExecution.Status.RUNNING,
        ],
    ).update(status=WorkflowExecution.Status.PENDING)


def step_workflow(exec_obj: WorkflowExecution):
    """Advance a workflow execution by replaying until the next pause or completion."""
    with transaction.atomic():
        try:
            wf = lock_execution(exec_obj.pk, skip_locked=True)
        except WorkflowExecution.DoesNotExist:
            return
        if wf.status != WorkflowExecution.Status.PENDING:
            return
        if wf.expires_at and wf.expires_at <= timezone.now():
            wf.time_out()
            return
        wf.wake_at = None
        if not HistoryEvent.objects.filter(
            execution=wf, type=HistoryEventType.WORKFLOW_STARTED.value
        ).exists():
            HistoryEvent.objects.create(
                execution=wf,
                type=HistoryEventType.WORKFLOW_STARTED.value,
                pos=0,
                details={'input': wf.input},
            )

        try:
            result = _run_workflow_once(wf)
        except Exception as e:
            HistoryEvent.objects.create(
                execution=wf,
                type=HistoryEventType.WORKFLOW_FAILED.value,
                pos=FINAL_EVENT_POS,
                details={'error': str(e)},
            )
            wf.status = WorkflowExecution.Status.FAILED
            wf.error = str(e)
            wf.finished_at = timezone.now()
            wf.save(update_fields=['status', 'error', 'finished_at', 'updated_at'])
            _notify_parent(
                wf,
                HistoryEventType.CHILD_WORKFLOW_FAILED.value,
                {'error': str(e)},
            )
            return

        if wf.expires_at and wf.expires_at <= timezone.now():
            wf.time_out()
            return

        if result is _PAUSED:
            wf.status = WorkflowExecution.Status.RUNNING
            wf.save(update_fields=['status', 'wake_at', 'updated_at'])
            return

        HistoryEvent.objects.create(
            execution=wf,
            type=HistoryEventType.WORKFLOW_COMPLETED.value,
            pos=FINAL_EVENT_POS,
            details={'result': result},
        )
        wf.status = WorkflowExecution.Status.COMPLETED
        wf.result = result
        wf.finished_at = timezone.now()
        wf.save(
            update_fields=['status', 'result', 'finished_at', 'wake_at', 'updated_at']
        )
        _notify_parent(
            wf,
            HistoryEventType.CHILD_WORKFLOW_COMPLETED.value,
            {'result': result},
        )


def execute_activity(task: ActivityTask, *, claimed=False):
    """Execute an owned attempt; persist its outcome separately from user code."""
    if not claimed and not task.start():
        return
    if not ActivityTask.objects.filter(
        pk=task.pk, status=ActivityTask.Status.RUNNING, lease_token=task.lease_token
    ).exists():
        return
    task.execution.refresh_from_db(fields=['status', 'expires_at'])
    if task.execution.is_terminal():
        task.mark_failed(ErrorCode.WORKFLOW_NOT_RUNNABLE.value)
        return
    now = timezone.now()
    if task.execution.expires_at and task.execution.expires_at <= now:
        task.execution.time_out()
        return
    if task.expires_at and task.expires_at <= now:
        task.mark_timed_out()
        return

    now = timezone.now()
    if not ActivityTask.objects.filter(
        pk=task.pk,
        status=ActivityTask.Status.RUNNING,
        lease_token=task.lease_token,
        lease_expires_at__gt=now,
    ).update(started_at=now, heartbeat_at=now):
        return

    # The dispatcher renews follower leases. Inline execution needs the same
    # liveness mechanism without requiring user-written activity heartbeats.
    stopped = threading.Event()
    renewer = None
    if not claimed:

        def renew():
            try:
                while not stopped.wait(task.lease_duration().total_seconds() / 3):
                    try:
                        if not task.renew_lease():
                            return
                    except DatabaseError:
                        close_old_connections()
            finally:
                close_old_connections()

        renewer = threading.Thread(target=renew, daemon=True)
        renewer.start()
    try:
        _current_activity.task_id = str(task.id)
        _current_activity.lease_token = task.lease_token
        try:
            if task.activity_name == SLEEP_ACTIVITY_NAME:
                result = {'slept': (task.args or [0])[0]}
            else:
                fn = register.activities.get(task.activity_name)
                if fn is None:
                    raise UnknownActivityError(task.activity_name)
                result = fn(*task.args, **task.kwargs)
            json.dumps(result)
        except Exception as exc:
            non_retry = (task.retry_policy or {}).get('non_retryable_error_types', [])
            task.retry_or_fail(
                str(exc),
                retryable=not isinstance(exc, UnknownActivityError)
                and type(exc).__name__ not in non_retry,
            )
        else:
            # Database errors here must roll back the whole transition and leave
            # the lease recoverable, rather than retry a half-committed result.
            task.mark_completed(result)
    finally:
        _current_activity.task_id = None
        _current_activity.lease_token = None
        stopped.set()
        if renewer is not None:
            renewer.join()


def activity_heartbeat(details: Any = None):
    """Record progress only for the current, live activity attempt."""
    task_id = getattr(_current_activity, 'task_id', None)
    token = getattr(_current_activity, 'lease_token', None)
    if not task_id:
        raise RuntimeError('No activity is currently running')
    now = timezone.now()
    fields = {'heartbeat_at': now}
    if details is not None:
        fields['heartbeat_details'] = details
    updated = ActivityTask.objects.filter(
        id=task_id,
        status=ActivityTask.Status.RUNNING,
        lease_token=token,
        lease_expires_at__gt=now,
    ).update(**fields)
    if not updated:
        raise ActivityCanceled('Activity attempt is no longer current')


def cancel_workflow(
    execution: WorkflowExecution | int | str,
    reason: str | None = None,
):
    """Cancel a workflow execution and fail its queued activities.

    - Sets workflow status to CANCELED if not terminal; records 'workflow_canceled' event.
    - Marks queued activities as FAILED with error 'workflow_canceled' to prevent execution.
    """
    if not isinstance(execution, WorkflowExecution):
        execution = WorkflowExecution.objects.get(pk=execution)
    execution.cancel(reason=reason)


def signal_workflow(
    execution: WorkflowExecution | int | str, name: str, payload: Any | None = None
):
    """Signal a workflow by enqueueing an external signal and mark it runnable.

    - Appends a 'signal_enqueued' HistoryEvent with the given name/payload.
    - Sets the workflow status to PENDING if it is not terminal.
    """
    if not isinstance(execution, WorkflowExecution):
        execution = WorkflowExecution.objects.get(pk=execution)
    execution.enqueue_signal(name, payload=payload)


# ---------------------------------------------------------------------------
# High-level run/start/wait APIs
# ---------------------------------------------------------------------------


def _start_workflow(
    workflow: str | Callable, timeout: float | None = None, **inputs
) -> str:
    """Create a workflow execution and return its handle (ID)."""
    workflow_name = (
        workflow if isinstance(workflow, str) else getattr(workflow, '_durable_name')
    )
    fn = register.workflows.get(workflow_name)
    if fn is None:
        raise UnknownWorkflowError(workflow_name)
    if timeout is None:
        timeout = getattr(fn, '_durable_timeout', None)
    expires_at = None
    if timeout is not None:
        expires_at = timezone.now() + timedelta(seconds=float(timeout))
    wf = WorkflowExecution.objects.create(
        workflow_name=workflow_name, input=inputs, expires_at=expires_at
    )
    return str(wf.id)


def _run_loop(execution: WorkflowExecution, tick: float = 0.01):
    """Advance the given execution synchronously until completion."""
    from .management.commands.durable_worker import Command

    scheduler = Command()
    terminal = {
        WorkflowExecution.Status.COMPLETED,
        WorkflowExecution.Status.FAILED,
        WorkflowExecution.Status.CANCELED,
        WorkflowExecution.Status.TIMED_OUT,
    }
    while True:
        now = timezone.now()
        progressed = scheduler._process_timeouts(now, 100)

        # Execute any due activities across all workflows. This ensures that
        # child workflow activities also run when using the synchronous API.
        due = list(
            ActivityTask.objects.filter(
                status=ActivityTask.Status.QUEUED, after_time__lte=now
            )
        )
        for task in due:
            execute_activity(task)
            scheduler._process_timeouts(timezone.now(), 100)
            progressed = True

        # Step all runnable workflows (including children) so that parent
        # workflows notice child completion or failure events.
        runnable = WorkflowExecution.objects.filter(
            status=WorkflowExecution.Status.PENDING
        )
        for wf in runnable:
            step_workflow(wf)

        execution.refresh_from_db()
        if execution.status in terminal:
            break

        if not progressed:
            next_due = (
                ActivityTask.objects.filter(status=ActivityTask.Status.QUEUED)
                .order_by('after_time')
                .values_list('after_time', flat=True)
                .first()
            )
            if next_due:
                wait = max(0.0, (next_due - timezone.now()).total_seconds())
                time.sleep(min(wait, tick))
            else:
                time.sleep(tick)

    if execution.status == WorkflowExecution.Status.COMPLETED:
        return execution.result
    if execution.status == WorkflowExecution.Status.CANCELED:
        raise WorkflowCanceled(execution.error or ErrorCode.WORKFLOW_CANCELED.value)
    if execution.status == WorkflowExecution.Status.TIMED_OUT:
        raise WorkflowTimeout(execution.error or ErrorCode.WORKFLOW_TIMEOUT.value)
    raise WorkflowException(execution.error or execution.status)


def _wait_workflow(execution: WorkflowExecution | int | str) -> Any:
    """Wait for a workflow execution to complete and return its result."""
    if not isinstance(execution, WorkflowExecution):
        execution = WorkflowExecution.objects.get(pk=execution)
    return _run_loop(execution)


def _run_workflow(
    workflow: str | Callable, timeout: float | None = None, **inputs
) -> Any:
    """Convenience helper: start a workflow and wait for its result."""
    exec_id = _start_workflow(workflow, timeout=timeout, **inputs)
    return _wait_workflow(exec_id)
