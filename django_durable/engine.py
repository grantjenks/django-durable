from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Optional
from django.db import transaction
from django.utils import timezone

from .registry import register
from .models import WorkflowExecution, HistoryEvent, ActivityTask


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
                type__in=('activity_completed', 'activity_failed'),
            )
            .order_by('id')
            .last()
        )
        if ev_done:
            if ev_done.type == 'activity_failed':
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
            HistoryEvent.objects.create(
                execution=self.execution,
                type='activity_scheduled',
                pos=pos,
                details={'activity_name': name, 'args': args, 'kwargs': kwargs},
            )
            max_retries = getattr(
                register.activities[name], '_durable_max_retries', 0
            )
            ActivityTask.objects.create(
                execution=self.execution,
                activity_name=name,
                pos=pos,
                args=args,
                kwargs=kwargs,
                max_attempts=max_retries,
                not_before=timezone.now(),
            )
        raise NeedsPause()

    def sleep(self, seconds: float):
        """Durable timer implemented as a special internal activity."""
        return self.activity('__sleep__', seconds)


def _run_workflow_once(exec_obj: WorkflowExecution) -> Optional[Any]:
    """Run the workflow function until it needs to pause or completes."""
    fn = register.workflows[exec_obj.workflow_name]
    ctx = Context(execution=exec_obj)
    # Prime ctx.pos = number of already scheduled calls to maintain determinism.
    ctx.pos = HistoryEvent.objects.filter(
        execution=exec_obj, type='activity_scheduled'
    ).count()
    try:
        return fn(ctx, **(exec_obj.input or {}))
    except NeedsPause:
        return None


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


def execute_activity(task: ActivityTask):
    """Run one activity and append completion/failure events."""
    from .registry import register

    fn = register.activities.get(task.activity_name)
    if fn is None:
        raise RuntimeError(f'Unknown activity {task.activity_name}')

    task.status = ActivityTask.Status.RUNNING
    task.started_at = timezone.now()
    task.attempt += 1
    task.save(update_fields=['status', 'started_at', 'attempt', 'updated_at'])

    try:
        if task.activity_name == '__sleep__':
            seconds = (task.args or [0])[0]
            # Only run when due; worker should fetch only due tasks.
            result = {'slept': seconds}
        else:
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

        # Nudge workflow runnable again
        WorkflowExecution.objects.filter(pk=task.execution_id).update(
            status=WorkflowExecution.Status.PENDING
        )

    except Exception as e:
        task.error = str(e)
        if task.attempt <= task.max_attempts:
            # Simple linear backoff: +30s per attempt
            from datetime import timedelta

            task.status = ActivityTask.Status.QUEUED
            task.not_before = timezone.now() + timedelta(seconds=30 * task.attempt)
            task.save(update_fields=['status', 'error', 'not_before', 'updated_at'])
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
