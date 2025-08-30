from contextlib import contextmanager
from dataclasses import dataclass
import threading
from typing import Any, Optional, Union
from django.db import transaction
from django.utils import timezone

from .registry import register
from .models import WorkflowExecution, HistoryEvent, ActivityTask


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
                execution=self.execution, pos=pos, type='version_marker'
            )
            .order_by('id')
            .last()
        )
        if ev:
            return ev.details.get('version')
        HistoryEvent.objects.create(
            execution=self.execution,
            type='version_marker',
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

    def activity(self, name: str, *args, **kwargs) -> Any:
        """Deterministic activity call with replay:
        - If completed event exists for this pos -> return its result.
        - If failed -> raise RuntimeError.
        - If scheduled and not finished -> pause.
        - Else schedule task -> pause.
        """
        pos = self._bump()
        # 1) Check for completion/failed
        ev_done = (
            HistoryEvent.objects.filter(
                execution=self.execution,
                pos=pos,
                type__in=(
                    'activity_completed',
                    'activity_failed',
                    'activity_timed_out',
                ),
            )
            .order_by('id')
            .last()
        )
        if ev_done:
            if ev_done.type in ('activity_failed', 'activity_timed_out'):
                raise RuntimeError(ev_done.details.get('error', 'activity_failed'))
            return ev_done.details.get('result')

        # 2) If scheduled but pending -> pause
        scheduled = HistoryEvent.objects.filter(
            execution=self.execution, pos=pos, type='activity_scheduled'
        ).exists()
        if scheduled:
            raise NeedsPause()

        # 3) First-time schedule
        with transaction.atomic():
            timeout = kwargs.pop('schedule_to_close_timeout', None)
            heartbeat = kwargs.pop('heartbeat_timeout', None)
            fn = register.activities.get(name)
            policy_obj = getattr(fn, '_durable_retry_policy', None) if fn else None
            policy_dict = (
                policy_obj.asdict() if policy_obj else {'maximum_attempts': getattr(fn, '_durable_max_retries', 0)}
            )
            if timeout is None and fn is not None:
                timeout = getattr(fn, '_durable_timeout', None)
            if heartbeat is None and fn is not None:
                heartbeat = getattr(fn, '_durable_heartbeat_timeout', None)
            HistoryEvent.objects.create(
                execution=self.execution,
                type='activity_scheduled',
                pos=pos,
                details={
                    'activity_name': name,
                    'args': args,
                    'kwargs': kwargs,
                    'timeout': timeout,
                    'heartbeat_timeout': heartbeat,
                    'retry_policy': policy_dict,
                },
            )
            # For internal sleep, defer until due time instead of immediate run.
            after_time = timezone.now()
            if name == '__sleep__':
                try:
                    seconds = float((args or [0])[0])
                except Exception:
                    seconds = 0.0
                if seconds < 0:
                    seconds = 0.0
                from datetime import timedelta

                after_time = after_time + timedelta(seconds=seconds)
            expires_at = None
            if timeout is not None:
                from datetime import timedelta

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
        raise NeedsPause()

    def sleep(self, seconds: float):
        """Durable timer implemented as a special internal activity."""
        return self.activity('__sleep__', seconds)

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
                execution=self.execution, pos=pos, type='signal_consumed'
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
                    execution=self.execution, pos=pos, type='signal_consumed'
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
                    type='signal_enqueued',
                    details__name=name,
                )
                .order_by('id')
            )
            enq = None
            if enqueued:
                # Build set of consumed enqueued_ids
                consumed_ids = set(
                    HistoryEvent.objects.filter(
                        execution=self.execution, type='signal_consumed'
                    ).values_list('details__enqueued_id', flat=True)
                )
                for e in enqueued:
                    if e.id not in consumed_ids:
                        enq = e
                        break

            if enq is not None:
                HistoryEvent.objects.create(
                    execution=self.execution,
                    type='signal_consumed',
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
                execution=self.execution, pos=pos, type='signal_wait'
            ).exists()
            if not waiting_exists:
                HistoryEvent.objects.create(
                    execution=self.execution,
                    type='signal_wait',
                    pos=pos,
                    details={'name': name},
                )
        raise NeedsPause()

    def workflow(self, name: str, timeout: Optional[float] = None, **input) -> Any:
        """Start and wait for a child workflow.

        Mirrors the activity API:
        - If a completion/failed event exists for this position, return/raise.
        - If already scheduled but not finished, pause.
        - Otherwise schedule the child workflow and pause.
        """

        pos = self._bump()

        ev_done = (
            HistoryEvent.objects.filter(
                execution=self.execution,
                pos=pos,
                type__in=[
                    'child_workflow_completed',
                    'child_workflow_failed',
                ],
            )
            .order_by('id')
            .last()
        )
        if ev_done:
            if ev_done.type == 'child_workflow_failed':
                raise RuntimeError(ev_done.details.get('error', 'child_workflow_failed'))
            return ev_done.details.get('result')

        scheduled = HistoryEvent.objects.filter(
            execution=self.execution, pos=pos, type='child_workflow_scheduled'
        ).exists()
        if scheduled:
            raise NeedsPause()

        with transaction.atomic():
            fn = register.workflows.get(name)
            if timeout is None and fn is not None:
                timeout = getattr(fn, '_durable_timeout', None)
            expires_at = None
            if timeout is not None:
                from datetime import timedelta

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
                type='child_workflow_scheduled',
                pos=pos,
                details={
                    'workflow_name': name,
                    'input': input,
                    'child_id': str(child.id),
                    'timeout': timeout,
                },
            )
        raise NeedsPause()


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
        details={"child_id": str(exec_obj.id), **details},
    )
    WorkflowExecution.objects.filter(
        pk=parent.pk,
        status__in=[
            WorkflowExecution.Status.PENDING,
            WorkflowExecution.Status.RUNNING,
            WorkflowExecution.Status.WAITING,
        ],
    ).update(status=WorkflowExecution.Status.PENDING)


def step_workflow(exec_obj: WorkflowExecution):
    """Advance a workflow execution by replaying until the next pause or completion."""
    with transaction.atomic():
        exec_obj.refresh_from_db()
        if exec_obj.status not in (
            WorkflowExecution.Status.PENDING,
            WorkflowExecution.Status.RUNNING,
        ):
            return
        if not HistoryEvent.objects.filter(
            execution=exec_obj, type='workflow_started'
        ).exists():
            HistoryEvent.objects.create(
                execution=exec_obj,
                type='workflow_started',
                pos=0,
                details={'input': exec_obj.input},
            )
        exec_obj.status = WorkflowExecution.Status.RUNNING
        exec_obj.save(update_fields=['status', 'updated_at'])

    try:
        result = _run_workflow_once(exec_obj)
    except Exception as e:
        with transaction.atomic():
            HistoryEvent.objects.create(
                execution=exec_obj,
                type='workflow_failed',
                pos=999999,
                details={'error': str(e)},
            )
            exec_obj.status = WorkflowExecution.Status.FAILED
            exec_obj.error = str(e)
            exec_obj.finished_at = timezone.now()
            exec_obj.save(
                update_fields=['status', 'error', 'finished_at', 'updated_at']
            )
            _notify_parent(exec_obj, 'child_workflow_failed', {'error': str(e)})
        return

    if result is None:
        # paused, waiting for activities/timers
        WorkflowExecution.objects.filter(pk=exec_obj.pk).update(
            status=WorkflowExecution.Status.WAITING
        )
        return

    # Completed
    with transaction.atomic():
        HistoryEvent.objects.create(
            execution=exec_obj,
            type='workflow_completed',
            pos=999999,
            details={'result': result},
        )
        exec_obj.status = WorkflowExecution.Status.COMPLETED
        exec_obj.result = result
        exec_obj.finished_at = timezone.now()
        exec_obj.save(update_fields=['status', 'result', 'finished_at', 'updated_at'])
        _notify_parent(exec_obj, 'child_workflow_completed', {'result': result})


def execute_activity(task: ActivityTask):
    """Run one activity and append completion/failure events."""
    from .registry import register

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
            task.error = 'workflow_canceled'
        else:
            task.error = 'workflow_not_runnable'
        task.finished_at = timezone.now()
        task.save(update_fields=['status', 'error', 'finished_at', 'updated_at'])
        HistoryEvent.objects.create(
            execution=task.execution,
            type='activity_failed',
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
        if task.activity_name == '__sleep__':
            seconds = (task.args or [0])[0]
            # Only run when due; worker should fetch only due tasks.
            result = {'slept': seconds}
        else:
            if fn is None:
                raise RuntimeError(f'Unknown activity {task.activity_name}')
            result = fn(*task.args, **task.kwargs)

        task.status = ActivityTask.Status.COMPLETED
        task.result = result
        task.finished_at = timezone.now()
        task.save(update_fields=['status', 'result', 'finished_at', 'updated_at'])

        HistoryEvent.objects.create(
            execution=task.execution,
            type='activity_completed',
            pos=task.pos,
            details={'activity_name': task.activity_name, 'result': result},
        )

        # Nudge workflow runnable again unless terminal (e.g., canceled)
        WorkflowExecution.objects.filter(
            pk=task.execution_id,
            status__in=[
                WorkflowExecution.Status.PENDING,
                WorkflowExecution.Status.RUNNING,
                WorkflowExecution.Status.WAITING,
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
        if should_retry:
            from datetime import timedelta

            interval = policy.get('initial_interval', 1.0) * (
                policy.get('backoff_coefficient', 2.0) ** (task.attempt - 1)
            )
            max_interval = policy.get('maximum_interval')
            if max_interval is not None:
                interval = min(interval, max_interval)

            task.status = ActivityTask.Status.QUEUED
            task.after_time = timezone.now() + timedelta(seconds=interval)
            task.save(update_fields=['status', 'error', 'after_time', 'updated_at'])
        else:
            task.status = ActivityTask.Status.FAILED
            task.finished_at = timezone.now()
            task.save(update_fields=['status', 'error', 'finished_at', 'updated_at'])
            HistoryEvent.objects.create(
                execution=task.execution,
                type='activity_failed',
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


def cancel_workflow(execution: Union[WorkflowExecution, str], reason: Optional[str] = None, cancel_queued_activities: bool = True):
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
            type='workflow_canceled',
            pos=999998,
            details={'reason': reason} if reason else {},
        )
        execution.status = WorkflowExecution.Status.CANCELED
        execution.error = (execution.error or '')
        if reason:
            execution.error = (execution.error + '\n' if execution.error else '') + f'Canceled: {reason}'
        execution.finished_at = timezone.now()
        execution.save(update_fields=['status', 'error', 'finished_at', 'updated_at'])

        if cancel_queued_activities:
            now = timezone.now()
            qs = ActivityTask.objects.select_for_update().filter(
                execution=execution, status=ActivityTask.Status.QUEUED
            )
            for t in qs:
                t.status = ActivityTask.Status.FAILED
                t.error = 'workflow_canceled'
                t.finished_at = now
                t.save(update_fields=['status', 'error', 'finished_at', 'updated_at'])
                HistoryEvent.objects.create(
                    execution=execution,
                    type='activity_failed',
                    pos=t.pos,
                    details={'error': 'workflow_canceled'},
                )

        _notify_parent(execution, 'child_workflow_failed', {'error': 'workflow_canceled'})

    for child in WorkflowExecution.objects.filter(
        parent=execution,
        status__in=[
            WorkflowExecution.Status.PENDING,
            WorkflowExecution.Status.RUNNING,
            WorkflowExecution.Status.WAITING,
        ],
    ):
        cancel_workflow(
            child,
            reason=reason or 'parent_canceled',
            cancel_queued_activities=cancel_queued_activities,
        )


def send_signal(execution: Union[WorkflowExecution, str], name: str, payload: Any = None):
    """Enqueue an external signal for a workflow and mark it runnable.

    - Appends a 'signal_enqueued' HistoryEvent with the given name/payload.
    - Sets the workflow status to PENDING if it is not terminal.
    """
    if not isinstance(execution, WorkflowExecution):
        execution = WorkflowExecution.objects.get(pk=execution)
    with transaction.atomic():
        HistoryEvent.objects.create(
            execution=execution,
            type='signal_enqueued',
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
