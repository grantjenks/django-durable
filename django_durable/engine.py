import json
import threading
import time
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Optional, Union

from django.db import transaction
from django.utils import timezone

from .retry import compute_backoff
from .constants import (
    FINAL_EVENT_POS,
    SLEEP_ACTIVITY_NAME,
    SPECIAL_EVENT_POS,
    ErrorCode,
    HistoryEventType,
)
from .exceptions import (
    ActivityError,
    ActivityTimeout,
    NondeterminismError,
    UnknownActivityError,
    WorkflowException,
    WorkflowTimeout,
)
from .models import ActivityTask, HistoryEvent, WorkflowExecution
from .registry import register

_current_activity = threading.local()


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
            return ev.details.get('version')
        HistoryEvent.objects.create(
            execution=self.execution,
            type=HistoryEventType.VERSION_MARKER.value,
            pos=pos,
            details={'change_id': change_id, 'version': version},
        )
        return version

    def patched(self, change_id: str) -> bool:
        """Convenience wrapper for feature flags that may be removed later.

        Uses ``get_version`` under the hood to record a patch marker so that
        once all executions have moved past the old code path the patch can be
        safely removed while preserving determinism.
        """

        return self.get_version(f'patch:{change_id}', 1) >= 1

    def deprecate_patch(self, change_id: str):
        """Record that a previously patched section has been removed.

        This is a no-op for the workflow logic but ensures a version marker
        exists so that replay of historical executions remains deterministic
        after the patch is removed from code.
        """

        self.get_version(f'patch:{change_id}', 1)

    def start_activity(self, name: str, *args, **kwargs) -> int:
        """Schedule an activity and return its handle."""
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

    def wait_activity(self, handle: int) -> Any:
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
                ),
            )
            .order_by('id')
            .last()
        )
        if ev_done:
            if ev_done.type == HistoryEventType.ACTIVITY_FAILED.value:
                err = ev_done.details.get('error', ErrorCode.ACTIVITY_FAILED.value)
                raise ActivityError(RuntimeError(err))
            if ev_done.type == HistoryEventType.ACTIVITY_TIMED_OUT.value:
                err = ev_done.details.get('error', ErrorCode.ACTIVITY_TIMEOUT.value)
                raise ActivityTimeout(err)
            return ev_done.details.get('result')

        scheduled = HistoryEvent.objects.filter(
            execution=self.execution,
            pos=pos,
            type=HistoryEventType.ACTIVITY_SCHEDULED.value,
        ).exists()
        if scheduled:
            raise NeedsPause()
        raise RuntimeError(f'Unknown activity handle {handle}')

    def run_activity(self, name: str, *args, **kwargs) -> Any:
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
        self, name: str, timeout: Optional[float] = None, **input
    ) -> str:
        """Schedule a child workflow and return its handle."""
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
        with transaction.atomic():
            fn = register.workflows.get(name)
            if timeout is None and fn is not None:
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

    def wait_workflow(self, handle: str) -> Any:
        """Wait for a previously started child workflow."""
        ev_done = (
            HistoryEvent.objects.filter(
                execution=self.execution,
                type__in=[
                    HistoryEventType.CHILD_WORKFLOW_COMPLETED.value,
                    HistoryEventType.CHILD_WORKFLOW_FAILED.value,
                ],
                details__child_id=handle,
            )
            .order_by('id')
            .last()
        )
        if ev_done:
            if ev_done.type == HistoryEventType.CHILD_WORKFLOW_FAILED.value:
                err = ev_done.details.get('error', ErrorCode.ACTIVITY_FAILED.value)
                if err == ErrorCode.WORKFLOW_TIMEOUT.value:
                    raise WorkflowTimeout(err)
                raise WorkflowException(err)
            return ev_done.details.get('result')
        scheduled = HistoryEvent.objects.filter(
            execution=self.execution,
            type=HistoryEventType.CHILD_WORKFLOW_SCHEDULED.value,
            details__child_id=handle,
        ).exists()
        if scheduled:
            raise NeedsPause()
        raise RuntimeError(f'Unknown workflow handle {handle}')

    def run_workflow(self, name: str, timeout: Optional[float] = None, **input) -> Any:
        handle = self.start_workflow(name, timeout=timeout, **input)
        return self.wait_workflow(handle)


def _run_workflow_once(exec_obj: WorkflowExecution) -> Optional[Any]:
    """Run the workflow function until it needs to pause or completes."""
    fn = register.workflows[exec_obj.workflow_name]
    ctx = Context(execution=exec_obj)
    # Always start from the beginning; deterministic API uses replay + event log.
    ctx.pos = 0
    try:
        return fn(ctx, **(exec_obj.input or {}))
    except NeedsPause:
        return None


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
            wf = WorkflowExecution.objects.select_for_update(skip_locked=True).get(
                pk=exec_obj.pk
            )
        except WorkflowExecution.DoesNotExist:
            return
        if wf.status != WorkflowExecution.Status.PENDING:
            return
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

        if result is None:
            wf.status = WorkflowExecution.Status.RUNNING
            wf.save(update_fields=['status', 'updated_at'])
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
        wf.save(update_fields=['status', 'result', 'finished_at', 'updated_at'])
        _notify_parent(
            wf,
            HistoryEventType.CHILD_WORKFLOW_COMPLETED.value,
            {'result': result},
        )


def execute_activity(task: ActivityTask):
    """Run one activity and append completion/failure events."""
    fn = register.activities.get(task.activity_name)

    # If workflow is not runnable (completed/failed/canceled), don't execute.
    task.execution.refresh_from_db(fields=['status'])
    if task.execution.status in (
        WorkflowExecution.Status.COMPLETED,
        WorkflowExecution.Status.FAILED,
        WorkflowExecution.Status.CANCELED,
        WorkflowExecution.Status.TIMED_OUT,
    ):
        task.status = ActivityTask.Status.FAILED
        if task.execution.status == WorkflowExecution.Status.CANCELED:
            task.error = ErrorCode.WORKFLOW_CANCELED.value
        else:
            task.error = ErrorCode.WORKFLOW_NOT_RUNNABLE.value
        task.finished_at = timezone.now()
        task.save(update_fields=['status', 'error', 'finished_at', 'updated_at'])
        HistoryEvent.objects.create(
            execution=task.execution,
            type=HistoryEventType.ACTIVITY_FAILED.value,
            pos=task.pos,
            details={'error': task.error},
        )
        return

    task.status = ActivityTask.Status.RUNNING
    now = timezone.now()
    task.started_at = now
    task.heartbeat_at = now
    task.attempt += 1
    task.save(
        update_fields=['status', 'started_at', 'heartbeat_at', 'attempt', 'updated_at']
    )

    try:
        _current_activity.task_id = str(task.id)
        if task.activity_name == SLEEP_ACTIVITY_NAME:
            seconds = (task.args or [0])[0]
            # Only run when due; worker should fetch only due tasks.
            result = {'slept': seconds}
        else:
            if fn is None:
                raise UnknownActivityError(task.activity_name)
            result = fn(*task.args, **task.kwargs)

        task.status = ActivityTask.Status.COMPLETED
        task.result = result
        task.finished_at = timezone.now()
        task.save(update_fields=['status', 'result', 'finished_at', 'updated_at'])

        HistoryEvent.objects.create(
            execution=task.execution,
            type=HistoryEventType.ACTIVITY_COMPLETED.value,
            pos=task.pos,
            details={'activity_name': task.activity_name, 'result': result},
        )

        # Nudge workflow runnable again unless terminal (e.g., canceled)
        WorkflowExecution.objects.filter(
            pk=task.execution_id,
            status__in=[
                WorkflowExecution.Status.PENDING,
                WorkflowExecution.Status.RUNNING,
            ],
        ).update(status=WorkflowExecution.Status.PENDING)

    except Exception as e:
        task.error = str(e)
        policy = task.retry_policy or {}
        error_type = e.__class__.__name__
        non_retry = policy.get('non_retryable_error_types', [])
        max_attempts = policy.get('maximum_attempts', 0)
        should_retry = error_type not in non_retry and (
            max_attempts == 0 or task.attempt < max_attempts
        )
        if isinstance(e, UnknownActivityError):
            should_retry = False
        if should_retry:
            interval = compute_backoff(policy, task.attempt)
            task.status = ActivityTask.Status.QUEUED
            task.after_time = timezone.now() + timedelta(seconds=interval)
            task.save(update_fields=['status', 'error', 'after_time', 'updated_at'])
        else:
            task.status = ActivityTask.Status.FAILED
            task.finished_at = timezone.now()
            task.save(update_fields=['status', 'error', 'finished_at', 'updated_at'])
            HistoryEvent.objects.create(
                execution=task.execution,
                type=HistoryEventType.ACTIVITY_FAILED.value,
                pos=task.pos,
                details={'error': str(e)},
            )
    finally:
        _current_activity.task_id = None


def activity_heartbeat(details: Any = None):
    """Record a heartbeat for the currently running activity."""
    task_id = getattr(_current_activity, 'task_id', None)
    if not task_id:
        raise RuntimeError('No activity is currently running')
    now = timezone.now()
    fields = {'heartbeat_at': now}
    if details is not None:
        fields['heartbeat_details'] = details
    ActivityTask.objects.filter(id=task_id).update(**fields)


def cancel_workflow(
    execution: Union[WorkflowExecution, str],
    reason: Optional[str] = None,
    cancel_queued_activities: bool = True,
):
    """Cancel a workflow execution and optionally cancel its queued activities.

    - Sets workflow status to CANCELED if not terminal; records 'workflow_canceled' event.
    - Marks queued activities as FAILED with error 'workflow_canceled' to prevent execution.
    """
    if not isinstance(execution, WorkflowExecution):
        execution = WorkflowExecution.objects.get(pk=execution)
    with transaction.atomic():
        execution.refresh_from_db()
        if execution.status in (
            WorkflowExecution.Status.COMPLETED,
            WorkflowExecution.Status.FAILED,
            WorkflowExecution.Status.CANCELED,
            WorkflowExecution.Status.TIMED_OUT,
        ):
            return
        HistoryEvent.objects.create(
            execution=execution,
            type=HistoryEventType.WORKFLOW_CANCELED.value,
            pos=SPECIAL_EVENT_POS,
            details={'reason': reason} if reason else {},
        )
        execution.status = WorkflowExecution.Status.CANCELED
        execution.error = execution.error or ''
        if reason:
            execution.error = (
                execution.error + '\n' if execution.error else ''
            ) + f'Canceled: {reason}'
        execution.finished_at = timezone.now()
        execution.save(update_fields=['status', 'error', 'finished_at', 'updated_at'])

        if cancel_queued_activities:
            now = timezone.now()
            qs = ActivityTask.objects.select_for_update().filter(
                execution=execution, status=ActivityTask.Status.QUEUED
            )
            for t in qs:
                t.status = ActivityTask.Status.FAILED
                t.error = ErrorCode.WORKFLOW_CANCELED.value
                t.finished_at = now
                t.save(update_fields=['status', 'error', 'finished_at', 'updated_at'])
                HistoryEvent.objects.create(
                    execution=execution,
                    type=HistoryEventType.ACTIVITY_FAILED.value,
                    pos=t.pos,
                    details={'error': ErrorCode.WORKFLOW_CANCELED.value},
                )

        _notify_parent(
            execution,
            HistoryEventType.CHILD_WORKFLOW_FAILED.value,
            {'error': ErrorCode.WORKFLOW_CANCELED.value},
        )

    for child in WorkflowExecution.objects.filter(
        parent=execution,
        status__in=[
            WorkflowExecution.Status.PENDING,
            WorkflowExecution.Status.RUNNING,
        ],
    ):
        cancel_workflow(
            child,
            reason=reason or ErrorCode.PARENT_CANCELED.value,
            cancel_queued_activities=cancel_queued_activities,
        )


def send_signal(
    execution: Union[WorkflowExecution, str], name: str, payload: Any = None
):
    """Enqueue an external signal for a workflow and mark it runnable.

    - Appends a 'signal_enqueued' HistoryEvent with the given name/payload.
    - Sets the workflow status to PENDING if it is not terminal.
    """
    if not isinstance(execution, WorkflowExecution):
        execution = WorkflowExecution.objects.get(pk=execution)
    with transaction.atomic():
        HistoryEvent.objects.create(
            execution=execution,
            type=HistoryEventType.SIGNAL_ENQUEUED.value,
            pos=0,
            details={'name': name, 'payload': payload},
        )
        if execution.status not in (
            WorkflowExecution.Status.COMPLETED,
            WorkflowExecution.Status.FAILED,
            WorkflowExecution.Status.CANCELED,
            WorkflowExecution.Status.TIMED_OUT,
        ):
            WorkflowExecution.objects.filter(pk=execution.pk).update(
                status=WorkflowExecution.Status.PENDING
            )


# ---------------------------------------------------------------------------
# High-level run/start/wait APIs
# ---------------------------------------------------------------------------


def _start_workflow(
    workflow_name: str, timeout: Optional[float] = None, **inputs
) -> str:
    """Create a workflow execution and return its handle (ID)."""
    if workflow_name not in register.workflows:
        raise KeyError(f"Unknown workflow '{workflow_name}'")
    fn = register.workflows[workflow_name]
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
    terminal = {
        WorkflowExecution.Status.COMPLETED,
        WorkflowExecution.Status.FAILED,
        WorkflowExecution.Status.CANCELED,
        WorkflowExecution.Status.TIMED_OUT,
    }
    while True:
        now = timezone.now()
        progressed = False

        # Execute any due activities across all workflows. This ensures that
        # child workflow activities also run when using the synchronous API.
        due = list(
            ActivityTask.objects.filter(
                status=ActivityTask.Status.QUEUED, after_time__lte=now
            )
        )
        for task in due:
            execute_activity(task)
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
    if execution.status == WorkflowExecution.Status.TIMED_OUT:
        raise WorkflowTimeout(execution.error or ErrorCode.WORKFLOW_TIMEOUT.value)
    raise WorkflowException(execution.error or execution.status)


def _wait_workflow(execution: Union[WorkflowExecution, str]) -> Any:
    """Wait for a workflow execution to complete and return its result."""
    if not isinstance(execution, WorkflowExecution):
        execution = WorkflowExecution.objects.get(pk=execution)
    return _run_loop(execution)


def _run_workflow(workflow_name: str, timeout: Optional[float] = None, **inputs) -> Any:
    """Convenience helper: start a workflow and wait for its result."""
    exec_id = _start_workflow(workflow_name, timeout=timeout, **inputs)
    return _wait_workflow(exec_id)
