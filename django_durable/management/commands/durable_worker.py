import socket
import time
from datetime import timedelta, timedelta as _td

from django.core.management.base import BaseCommand
from django.db import DatabaseError
from django.utils import timezone

from django_durable.constants import SPECIAL_EVENT_POS, ErrorCode, HistoryEventType
from django_durable.engine import execute_activity, step_workflow
from django_durable.models import ActivityTask, HistoryEvent, WorkflowExecution


class Command(BaseCommand):
    help = 'Run the django-durable worker (workflows + activities).'

    def add_arguments(self, parser):
        parser.add_argument(
            '--tick', type=float, default=0.5, help='Poll interval in seconds.'
        )
        parser.add_argument('--batch', type=int, default=10, help='Max tasks per tick.')
        parser.add_argument(
            '--iterations',
            type=int,
            default=None,
            help='Optional number of loop iterations to run (for testing).',
        )

    def handle(self, *args, **opts):
        tick = opts['tick']
        batch = opts['batch']
        iterations = opts['iterations']
        hostname = socket.gethostname()
        self.stdout.write(self.style.SUCCESS(f'[durable] worker started on {hostname}'))

        loops = 0
        while True:
            now = timezone.now()
            progressed = False

            # 0) Timeout queued activities
            timed_ids = list(
                ActivityTask.objects.filter(
                    status=ActivityTask.Status.QUEUED,
                    expires_at__isnull=False,
                    expires_at__lte=now,
                ).values_list('id', flat=True)[:batch]
            )
            if timed_ids:
                progressed = True
            for tid in timed_ids:
                try:
                    task = ActivityTask.objects.get(id=tid)
                except ActivityTask.DoesNotExist:
                    continue
                task.error = ErrorCode.ACTIVITY_TIMEOUT.value
                policy = task.retry_policy or {}
                max_attempts = policy.get('maximum_attempts', 0)
                curr_attempt = task.attempt or 0
                should_retry = curr_attempt > 0 and (
                    max_attempts == 0 or curr_attempt < max_attempts
                )
                if should_retry:
                    interval = policy.get('initial_interval', 1.0) * (
                        policy.get('backoff_coefficient', 2.0) ** curr_attempt
                    )
                    max_interval = policy.get('maximum_interval')
                    if max_interval is not None:
                        interval = min(interval, max_interval)
                    task.after_time = timezone.now() + _td(seconds=interval)
                    task.save(update_fields=['error', 'after_time', 'updated_at'])
                else:
                    task.status = ActivityTask.Status.TIMED_OUT
                    task.finished_at = now
                    task.save(
                        update_fields=['status', 'error', 'finished_at', 'updated_at']
                    )
                    HistoryEvent.objects.create(
                        execution=task.execution,
                        type=HistoryEventType.ACTIVITY_TIMED_OUT.value,
                        pos=task.pos,
                        details={'error': ErrorCode.ACTIVITY_TIMEOUT.value},
                    )
                    WorkflowExecution.objects.filter(
                        pk=task.execution_id,
                        status__in=[
                            WorkflowExecution.Status.PENDING,
                            WorkflowExecution.Status.RUNNING,
                        ],
                    ).update(status=WorkflowExecution.Status.PENDING)

            # 0b) Timeout workflows
            wf_timeouts = list(
                WorkflowExecution.objects.filter(
                    status__in=[
                        WorkflowExecution.Status.PENDING,
                        WorkflowExecution.Status.RUNNING,
                    ],
                    expires_at__isnull=False,
                    expires_at__lte=now,
                ).values_list('id', flat=True)[:batch]
            )
            if wf_timeouts:
                progressed = True
            for wid in wf_timeouts:
                try:
                    wf = WorkflowExecution.objects.get(id=wid)
                except WorkflowExecution.DoesNotExist:
                    continue
                HistoryEvent.objects.create(
                    execution=wf,
                    type=HistoryEventType.WORKFLOW_TIMED_OUT.value,
                    pos=SPECIAL_EVENT_POS,
                    details={'error': ErrorCode.WORKFLOW_TIMEOUT.value},
                )
                wf.status = WorkflowExecution.Status.TIMED_OUT
                wf.error = ErrorCode.WORKFLOW_TIMEOUT.value
                wf.finished_at = now
                wf.save(update_fields=['status', 'error', 'finished_at', 'updated_at'])
                qs = ActivityTask.objects.select_for_update().filter(
                    execution=wf, status=ActivityTask.Status.QUEUED
                )
                for t in qs:
                    t.status = ActivityTask.Status.FAILED
                    t.error = ErrorCode.WORKFLOW_TIMEOUT.value
                    t.finished_at = now
                    t.save(
                        update_fields=['status', 'error', 'finished_at', 'updated_at']
                    )
                    HistoryEvent.objects.create(
                        execution=wf,
                        type=HistoryEventType.ACTIVITY_FAILED.value,
                        pos=t.pos,
                        details={'error': ErrorCode.WORKFLOW_TIMEOUT.value},
                    )

            # 0c) Heartbeat timeouts for running activities
            hb_ids = list(
                ActivityTask.objects.filter(
                    status=ActivityTask.Status.RUNNING,
                    heartbeat_timeout__isnull=False,
                ).values_list('id', flat=True)[:batch]
            )
            if hb_ids:
                progressed = True
            for tid in hb_ids:
                try:
                    task = ActivityTask.objects.get(id=tid)
                except ActivityTask.DoesNotExist:
                    continue
                hb_at = task.heartbeat_at or task.started_at or now
                if (
                    task.heartbeat_timeout is not None
                    and hb_at + timedelta(seconds=float(task.heartbeat_timeout)) <= now
                ):
                    task.error = ErrorCode.HEARTBEAT_TIMEOUT.value
                    policy = task.retry_policy or {}
                    max_attempts = policy.get('maximum_attempts', 0)
                    curr_attempt = task.attempt or 1
                    should_retry = max_attempts == 0 or curr_attempt < max_attempts
                    if should_retry:
                        interval = policy.get('initial_interval', 1.0) * (
                            policy.get('backoff_coefficient', 2.0) ** (curr_attempt - 1)
                        )
                        max_interval = policy.get('maximum_interval')
                        if max_interval is not None:
                            interval = min(interval, max_interval)
                        task.status = ActivityTask.Status.QUEUED
                        task.after_time = timezone.now() + _td(seconds=interval)
                        task.save(
                            update_fields=[
                                'status',
                                'error',
                                'after_time',
                                'updated_at',
                            ]
                        )
                    else:
                        task.status = ActivityTask.Status.TIMED_OUT
                        task.finished_at = now
                        task.save(
                            update_fields=[
                                'status',
                                'error',
                                'finished_at',
                                'updated_at',
                            ]
                        )
                        HistoryEvent.objects.create(
                            execution=task.execution,
                            type=HistoryEventType.ACTIVITY_TIMED_OUT.value,
                            pos=task.pos,
                            details={'error': ErrorCode.HEARTBEAT_TIMEOUT.value},
                        )
                        WorkflowExecution.objects.filter(
                            pk=task.execution_id,
                            status__in=[
                                WorkflowExecution.Status.PENDING,
                                WorkflowExecution.Status.RUNNING,
                            ],
                        ).update(status=WorkflowExecution.Status.PENDING)

            # 0d) Schedule-to-close timeouts for running activities
            sc_ids = list(
                ActivityTask.objects.filter(
                    status=ActivityTask.Status.RUNNING,
                    expires_at__isnull=False,
                    expires_at__lte=now,
                ).values_list('id', flat=True)[:batch]
            )
            if sc_ids:
                progressed = True
            for tid in sc_ids:
                try:
                    task = ActivityTask.objects.get(id=tid)
                except ActivityTask.DoesNotExist:
                    continue
                task.error = ErrorCode.ACTIVITY_TIMEOUT.value
                policy = task.retry_policy or {}
                max_attempts = policy.get('maximum_attempts', 0)
                curr_attempt = task.attempt or 1
                should_retry = max_attempts == 0 or curr_attempt < max_attempts
                if should_retry:
                    interval = policy.get('initial_interval', 1.0) * (
                        policy.get('backoff_coefficient', 2.0) ** (curr_attempt - 1)
                    )
                    max_interval = policy.get('maximum_interval')
                    if max_interval is not None:
                        interval = min(interval, max_interval)
                    task.status = ActivityTask.Status.QUEUED
                    task.after_time = timezone.now() + _td(seconds=interval)
                    task.save(
                        update_fields=['status', 'error', 'after_time', 'updated_at']
                    )
                else:
                    task.status = ActivityTask.Status.TIMED_OUT
                    task.finished_at = now
                    task.save(
                        update_fields=['status', 'error', 'finished_at', 'updated_at']
                    )
                    HistoryEvent.objects.create(
                        execution=task.execution,
                        type=HistoryEventType.ACTIVITY_TIMED_OUT.value,
                        pos=task.pos,
                        details={'error': ErrorCode.ACTIVITY_TIMEOUT.value},
                    )
                    WorkflowExecution.objects.filter(
                        pk=task.execution_id,
                        status__in=[
                            WorkflowExecution.Status.PENDING,
                            WorkflowExecution.Status.RUNNING,
                        ],
                    ).update(status=WorkflowExecution.Status.PENDING)

            # 1) Run due activities
            due_ids = list(
                ActivityTask.objects.filter(
                    status=ActivityTask.Status.QUEUED,
                    after_time__lte=now,
                    execution__status__in=[
                        WorkflowExecution.Status.PENDING,
                        WorkflowExecution.Status.RUNNING,
                    ],
                )
                .order_by('updated_at')
                .values_list('id', flat=True)[:batch]
            )
            if due_ids:
                progressed = True
            for tid in due_ids:
                # Claim the task atomically so other workers skip it.
                claimed = ActivityTask.objects.filter(
                    id=tid,
                    status=ActivityTask.Status.QUEUED,
                    after_time__lte=now,
                ).update(status=ActivityTask.Status.RUNNING)
                if not claimed:
                    continue
                try:
                    task = ActivityTask.objects.get(id=tid)
                    execute_activity(task)
                except DatabaseError:
                    # Revert claim so another worker can retry.
                    ActivityTask.objects.filter(id=tid).update(
                        status=ActivityTask.Status.QUEUED
                    )
                    continue

            # 2) Advance workflows
            runnable_ids = list(
                WorkflowExecution.objects.filter(
                    status=WorkflowExecution.Status.PENDING
                )
                .order_by('updated_at')
                .values_list('id', flat=True)[:batch]
            )
            if runnable_ids:
                progressed = True
            for wid in runnable_ids:
                try:
                    wf = WorkflowExecution.objects.get(id=wid)
                    step_workflow(wf)
                except DatabaseError:
                    continue

            loops += 1
            if iterations is not None and loops >= iterations:
                break
            if not progressed:
                time.sleep(tick)
