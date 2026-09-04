import time
import uuid
from contextlib import contextmanager
from datetime import timedelta

from django.conf import settings
from django.db import connection, models, transaction
from django.utils import timezone

from .constants import SPECIAL_EVENT_POS, ErrorCode, HistoryEventType
from .exceptions import WaitWorkflowTimeout, WorkflowException, WorkflowTimeout
from .retry import compute_backoff


def lock_execution(pk, *, skip_locked=False):
    """Lock before reading; SQLite needs a write to avoid a deferred upgrade."""
    if connection.vendor == 'sqlite':
        WorkflowExecution.objects.filter(pk=pk).update(
            updated_at=models.F('updated_at')
        )
    return WorkflowExecution.objects.select_for_update(skip_locked=skip_locked).get(
        pk=pk
    )


class WorkflowExecution(models.Model):
    class Status(models.TextChoices):
        PENDING = 'PENDING'
        RUNNING = 'RUNNING'
        WAITING = 'WAITING'
        COMPLETED = 'COMPLETED'
        FAILED = 'FAILED'
        CANCELED = 'CANCELED'
        TIMED_OUT = 'TIMED_OUT'

    TERMINAL_STATUSES = {
        Status.COMPLETED,
        Status.FAILED,
        Status.CANCELED,
        Status.TIMED_OUT,
    }

    workflow_name = models.CharField(max_length=200)
    input = models.JSONField(default=dict, blank=True)
    status = models.CharField(
        max_length=20, choices=Status.choices, default=Status.PENDING
    )
    result = models.JSONField(null=True, blank=True)
    error = models.TextField(null=True, blank=True)
    started_at = models.DateTimeField(default=timezone.now)
    finished_at = models.DateTimeField(null=True, blank=True)
    expires_at = models.DateTimeField(null=True, blank=True)
    wake_at = models.DateTimeField(null=True, blank=True)
    parent = models.ForeignKey(
        'self', null=True, blank=True, related_name='children', on_delete=models.CASCADE
    )
    parent_pos = models.IntegerField(null=True, blank=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f'{self.workflow_name}:{self.id}'

    def is_terminal(self) -> bool:
        return self.status in self.TERMINAL_STATUSES

    def wait(self, timeout: float | None = None):
        deadline = None
        if timeout is not None:
            deadline = time.monotonic() + float(timeout)

        while True:
            self.refresh_from_db()
            if self.status == self.Status.COMPLETED:
                return self.result
            if self.status == self.Status.FAILED:
                raise WorkflowException(self.error or ErrorCode.ACTIVITY_FAILED.value)
            if self.status == self.Status.CANCELED:
                raise WorkflowException(self.error or ErrorCode.WORKFLOW_CANCELED.value)
            if self.status == self.Status.TIMED_OUT:
                raise WorkflowTimeout(self.error or ErrorCode.WORKFLOW_TIMEOUT.value)

            if timeout == 0 or (deadline and time.monotonic() >= deadline):
                raise WaitWorkflowTimeout()

            time.sleep(1)

    def _notify_parent(self, event_type: str, details: dict):
        if not self.parent_id:
            return
        parent = self.parent
        HistoryEvent.objects.create(
            execution=parent,
            type=event_type,
            pos=self.parent_pos or 0,
            details={'child_id': str(self.id), **details},
        )
        WorkflowExecution.objects.filter(
            pk=parent.pk,
            status__in=[
                WorkflowExecution.Status.PENDING,
                WorkflowExecution.Status.RUNNING,
            ],
        ).update(status=WorkflowExecution.Status.PENDING)

    def cancel(self, reason: str | None = None):
        with transaction.atomic():
            locked = lock_execution(self.pk)
            self.__dict__.update(locked.__dict__)
            if self.is_terminal():
                return

            HistoryEvent.objects.create(
                execution=self,
                type=HistoryEventType.WORKFLOW_CANCELED.value,
                pos=SPECIAL_EVENT_POS,
                details={'reason': reason} if reason else {},
            )

            self.status = WorkflowExecution.Status.CANCELED
            self.error = self.error or ''
            if reason:
                self.error = (
                    self.error + '\n' if self.error else ''
                ) + f'Canceled: {reason}'
            self.finished_at = timezone.now()
            self.save(update_fields=['status', 'error', 'finished_at', 'updated_at'])

            now = timezone.now()
            queued = list(
                ActivityTask.objects.select_for_update().filter(
                    execution=self,
                    status__in=[
                        ActivityTask.Status.QUEUED,
                        ActivityTask.Status.RUNNING,
                    ],
                )
            )
            for task in queued:
                task.fail_due_to_cancel(finished_at=now)

            self._notify_parent(
                HistoryEventType.CHILD_WORKFLOW_CANCELED.value,
                {'error': ErrorCode.WORKFLOW_CANCELED.value},
            )

        children = WorkflowExecution.objects.filter(
            parent=self,
            status__in=[
                WorkflowExecution.Status.PENDING,
                WorkflowExecution.Status.RUNNING,
            ],
        )
        for child in children:
            child.cancel(reason=reason or ErrorCode.PARENT_CANCELED.value)

    def time_out(self):
        """Finish an execution and its outstanding tasks in one transaction."""
        with transaction.atomic():
            locked = lock_execution(self.pk)
            self.__dict__.update(locked.__dict__)
            if self.is_terminal():
                return False
            self.status = self.Status.TIMED_OUT
            self.error = ErrorCode.WORKFLOW_TIMEOUT.value
            self.finished_at = timezone.now()
            self.wake_at = None
            self.save(
                update_fields=[
                    'status',
                    'error',
                    'finished_at',
                    'wake_at',
                    'updated_at',
                ]
            )
            HistoryEvent.objects.create(
                execution=self,
                type=HistoryEventType.WORKFLOW_TIMED_OUT.value,
                pos=SPECIAL_EVENT_POS,
                details={'error': self.error},
            )
            for task in self.activities.filter(
                status__in=[ActivityTask.Status.QUEUED, ActivityTask.Status.RUNNING]
            ):
                task.mark_failed(self.error, finished_at=self.finished_at)
            self._notify_parent(
                HistoryEventType.CHILD_WORKFLOW_TIMED_OUT.value, {'error': self.error}
            )
        for child in self.children.exclude(status__in=self.TERMINAL_STATUSES):
            child.cancel(reason=ErrorCode.WORKFLOW_TIMEOUT.value)
        return True

    def enqueue_signal(self, name: str, payload=None):
        with transaction.atomic():
            locked = lock_execution(self.pk)
            self.status = locked.status
            HistoryEvent.objects.create(
                execution=self,
                type=HistoryEventType.SIGNAL_ENQUEUED.value,
                pos=SPECIAL_EVENT_POS,
                details={'name': name, 'payload': payload},
            )
            if not self.is_terminal():
                WorkflowExecution.objects.filter(pk=self.pk).update(
                    status=WorkflowExecution.Status.PENDING
                )

    class Meta:
        indexes = [
            models.Index(fields=['status', 'updated_at'], name='wf_status_updated_idx'),
            models.Index(fields=['status', 'expires_at'], name='wf_status_expires_idx'),
            models.Index(fields=['status', 'wake_at']),
        ]


class HistoryEvent(models.Model):
    # Append-only event log; ordered by autoincrement PK
    execution = models.ForeignKey(
        WorkflowExecution, related_name='history', on_delete=models.CASCADE
    )
    type = models.CharField(max_length=64, choices=HistoryEventType.choices)
    pos = models.IntegerField(
        default=0
    )  # deterministic call index within workflow replay
    details = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f'{self.execution_id}:{self.pos}:{self.type}'

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=['execution', 'pos', 'type'],
                condition=~models.Q(pos=SPECIAL_EVENT_POS),
                name='historyevent_execution_pos_type_unique',
            )
        ]
        indexes = [
            models.Index(fields=['execution', 'type']),
            models.Index(
                fields=['execution', 'pos', 'type'], name='he_exec_pos_type_idx'
            ),
            models.Index(
                fields=['execution', 'type', 'id'], name='he_exec_type_id_idx'
            ),
        ]


class ActivityTask(models.Model):
    class Status(models.TextChoices):
        QUEUED = 'QUEUED'
        RUNNING = 'RUNNING'
        COMPLETED = 'COMPLETED'
        FAILED = 'FAILED'
        TIMED_OUT = 'TIMED_OUT'

    execution = models.ForeignKey(
        WorkflowExecution, related_name='activities', on_delete=models.CASCADE
    )
    activity_name = models.CharField(max_length=200)
    pos = models.IntegerField(default=0)  # matches HistoryEvent.pos
    args = models.JSONField(default=list, blank=True)
    kwargs = models.JSONField(default=dict, blank=True)
    status = models.CharField(
        max_length=20, choices=Status.choices, default=Status.QUEUED
    )
    after_time = models.DateTimeField(default=timezone.now)
    expires_at = models.DateTimeField(null=True, blank=True)
    lease_token = models.UUIDField(null=True, blank=True)
    lease_expires_at = models.DateTimeField(null=True, blank=True)
    attempt = models.IntegerField(default=0)
    max_attempts = models.IntegerField(default=0)
    retry_policy = models.JSONField(default=dict, blank=True)
    heartbeat_timeout = models.FloatField(null=True, blank=True)
    heartbeat_at = models.DateTimeField(null=True, blank=True)
    heartbeat_details = models.JSONField(default=dict, blank=True)
    result = models.JSONField(null=True, blank=True)
    error = models.TextField(null=True, blank=True)
    started_at = models.DateTimeField(null=True, blank=True)
    finished_at = models.DateTimeField(null=True, blank=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f'{self.activity_name}:{self.execution_id}:{self.pos}'

    @staticmethod
    def lease_duration():
        return timedelta(
            seconds=max(
                1.0, float(getattr(settings, 'DURABLE_ACTIVITY_LEASE_SECONDS', 30))
            )
        )

    def start(self):
        """Claim a queued task before dispatch, including an attempt identity."""
        with transaction.atomic():
            wf = lock_execution(self.execution_id)
            now = timezone.now()
            if wf.is_terminal() or (wf.expires_at and wf.expires_at <= now):
                return False
            claimed = (
                type(self)
                .objects.filter(
                    pk=self.pk,
                    status=self.Status.QUEUED,
                    after_time__lte=now,
                )
                .filter(models.Q(expires_at=None) | models.Q(expires_at__gt=now))
                .update(
                    status=self.Status.RUNNING,
                    started_at=None,
                    heartbeat_at=None,
                    attempt=models.F('attempt') + 1,
                    lease_token=uuid.uuid4(),
                    lease_expires_at=now + self.lease_duration(),
                    updated_at=now,
                )
            )
            if claimed:
                self.refresh_from_db()
            return bool(claimed)

    def renew_lease(self):
        now = timezone.now()
        return (
            type(self)
            .objects.filter(
                pk=self.pk,
                status=self.Status.RUNNING,
                lease_token=self.lease_token,
                lease_expires_at__gt=now,
            )
            .update(lease_expires_at=now + self.lease_duration())
        )

    @contextmanager
    def _transition(self):
        # Match the workflow step's lock order. A stale attempt must never write
        # results, retry state, or history on behalf of its replacement.
        with transaction.atomic():
            wf = lock_execution(self.execution_id)
            task = type(self).objects.select_for_update().get(pk=self.pk)
            if (
                task.status not in (self.Status.QUEUED, self.Status.RUNNING)
                or task.lease_token != self.lease_token
            ):
                yield None, wf
                return
            yield task, wf
            self.__dict__.update(task.__dict__)

    def _finish(
        self,
        wf,
        status,
        event_type,
        details,
        *,
        result=None,
        error=None,
        finished_at=None,
    ):
        """Called only while holding the execution/task transaction."""
        self.status = status
        self.result = result
        self.error = error
        self.finished_at = finished_at or timezone.now()
        self.lease_expires_at = None
        self.save(
            update_fields=[
                'status',
                'result',
                'error',
                'finished_at',
                'lease_token',
                'lease_expires_at',
                'updated_at',
            ]
        )
        HistoryEvent.objects.create(
            execution=wf, type=event_type, pos=self.pos, details=details
        )
        if not wf.is_terminal():
            WorkflowExecution.objects.filter(pk=wf.pk).update(
                status=WorkflowExecution.Status.PENDING
            )
        return True

    def mark_completed(self, result):
        with self._transition() as (task, wf):
            if task is None or task.status != self.Status.RUNNING or wf.is_terminal():
                return False
            now = timezone.now()
            if wf.expires_at and wf.expires_at <= now:
                wf.time_out()
                return False
            if task.expires_at and task.expires_at <= now:
                return task._finish(
                    wf,
                    self.Status.TIMED_OUT,
                    HistoryEventType.ACTIVITY_TIMED_OUT.value,
                    {'error': ErrorCode.ACTIVITY_TIMEOUT.value},
                    error=ErrorCode.ACTIVITY_TIMEOUT.value,
                )
            if task.lease_expires_at and task.lease_expires_at <= now:
                return False
            if (
                task.heartbeat_timeout is not None
                and task.heartbeat_at
                and task.heartbeat_at + timedelta(seconds=task.heartbeat_timeout) <= now
            ):
                return False
            return task._finish(
                wf,
                self.Status.COMPLETED,
                HistoryEventType.ACTIVITY_COMPLETED.value,
                {'activity_name': task.activity_name, 'result': result},
                result=result,
            )

    def mark_failed(self, error: str, finished_at=None):
        with self._transition() as (task, wf):
            if task is None:
                return False
            return task._finish(
                wf,
                self.Status.FAILED,
                HistoryEventType.ACTIVITY_FAILED.value,
                {'error': error},
                error=error,
                finished_at=finished_at,
            )

    def mark_timed_out(self, error=ErrorCode.ACTIVITY_TIMEOUT.value):
        with self._transition() as (task, wf):
            if task is None:
                return False
            return task._finish(
                wf,
                self.Status.TIMED_OUT,
                HistoryEventType.ACTIVITY_TIMED_OUT.value,
                {'error': error},
                error=error,
            )

    def retry_or_fail(
        self,
        error,
        *,
        retryable=True,
        timed_out=False,
        heartbeat_before=None,
        lease_before=None,
    ):
        with self._transition() as (task, wf):
            if task is None:
                return False
            if wf.is_terminal():
                error = ErrorCode.WORKFLOW_NOT_RUNNABLE.value
                return task._finish(
                    wf,
                    self.Status.FAILED,
                    HistoryEventType.ACTIVITY_FAILED.value,
                    {'error': error},
                    error=error,
                )
            now = timezone.now()
            if heartbeat_before is not None:
                heartbeat = task.heartbeat_at or task.started_at or task.updated_at
                if (
                    task.status != self.Status.RUNNING
                    or task.heartbeat_timeout is None
                    or heartbeat + timedelta(seconds=task.heartbeat_timeout)
                    > heartbeat_before
                ):
                    return False
            if lease_before is not None:
                lease = task.lease_expires_at or task.updated_at + self.lease_duration()
                if task.status != self.Status.RUNNING or lease > lease_before:
                    return False
            if wf.expires_at and wf.expires_at <= now:
                wf.time_out()
                return True
            if task.expires_at and task.expires_at <= now:
                return task._finish(
                    wf,
                    self.Status.TIMED_OUT,
                    HistoryEventType.ACTIVITY_TIMED_OUT.value,
                    {'error': ErrorCode.ACTIVITY_TIMEOUT.value},
                    error=ErrorCode.ACTIVITY_TIMEOUT.value,
                )
            policy = task.retry_policy or {}
            maximum = policy.get('maximum_attempts', 0)
            if retryable and (maximum == 0 or task.attempt < maximum):
                task.error = error
                task._queue_retry(compute_backoff(policy, task.attempt))
                return True
            status = self.Status.TIMED_OUT if timed_out else self.Status.FAILED
            event = (
                HistoryEventType.ACTIVITY_TIMED_OUT.value
                if timed_out
                else HistoryEventType.ACTIVITY_FAILED.value
            )
            return task._finish(wf, status, event, {'error': error}, error=error)

    def _queue_retry(self, backoff_seconds):
        self.status = self.Status.QUEUED
        self.after_time = timezone.now() + timedelta(seconds=backoff_seconds)
        self.lease_token = None
        self.lease_expires_at = None
        self.save(
            update_fields=[
                'status',
                'error',
                'after_time',
                'lease_token',
                'lease_expires_at',
                'updated_at',
            ]
        )

    def schedule_retry(self, backoff_seconds: float):
        with self._transition() as (task, wf):
            if task is None or wf.is_terminal():
                return False
            task.error = self.error
            task._queue_retry(backoff_seconds)
            return True

    def fail_due_to_cancel(self, finished_at=None):
        return self.mark_failed(
            ErrorCode.WORKFLOW_CANCELED.value, finished_at=finished_at
        )

    class Meta:
        indexes = [
            models.Index(fields=['execution', 'status']),
            models.Index(
                fields=['status', 'after_time'], name='django_dura_status_af_idx'
            ),
            models.Index(fields=['status', 'expires_at'], name='at_status_expires_idx'),
            models.Index(
                fields=['status', 'heartbeat_timeout'], name='at_status_hb_idx'
            ),
            models.Index(fields=['status', 'lease_expires_at']),
            models.Index(fields=['status', 'updated_at'], name='at_status_updated_idx'),
        ]
