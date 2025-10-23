import time
from datetime import timedelta

from django.db import models, transaction
from django.utils import timezone

from .constants import SPECIAL_EVENT_POS, ErrorCode, HistoryEventType
from .exceptions import (
    WaitWorkflowTimeout,
    WorkflowException,
    WorkflowTimeout,
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
                raise WorkflowException(
                    self.error or ErrorCode.ACTIVITY_FAILED.value
                )
            if self.status == self.Status.CANCELED:
                raise WorkflowException(
                    self.error or ErrorCode.WORKFLOW_CANCELED.value
                )
            if self.status == self.Status.TIMED_OUT:
                raise WorkflowTimeout(
                    self.error or ErrorCode.WORKFLOW_TIMEOUT.value
                )

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
            self.refresh_from_db()
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
                    execution=self, status=ActivityTask.Status.QUEUED
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

    def enqueue_signal(self, name: str, payload=None):
        with transaction.atomic():
            self.refresh_from_db(fields=['status'])
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
            models.Index(fields=['status', 'updated_at']),
            models.Index(fields=['status', 'expires_at']),
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
        return f"{self.execution_id}:{self.pos}:{self.type}"

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
            models.Index(fields=['execution', 'pos', 'type']),
            models.Index(fields=['execution', 'type', 'id']),
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
        return f"{self.activity_name}:{self.execution_id}:{self.pos}"

    def start(self):
        now = timezone.now()
        self.status = ActivityTask.Status.RUNNING
        self.started_at = now
        self.heartbeat_at = now
        self.attempt += 1
        self.save(
            update_fields=['status', 'started_at', 'heartbeat_at', 'attempt', 'updated_at']
        )

    def mark_completed(self, result):
        self.status = ActivityTask.Status.COMPLETED
        self.result = result
        self.finished_at = timezone.now()
        self.save(update_fields=['status', 'result', 'finished_at', 'updated_at'])
        HistoryEvent.objects.create(
            execution=self.execution,
            type=HistoryEventType.ACTIVITY_COMPLETED.value,
            pos=self.pos,
            details={'activity_name': self.activity_name, 'result': result},
        )

    def mark_failed(self, error: str, finished_at=None):
        if finished_at is None:
            finished_at = timezone.now()
        self.status = ActivityTask.Status.FAILED
        self.error = error
        self.finished_at = finished_at
        self.save(update_fields=['status', 'error', 'finished_at', 'updated_at'])
        HistoryEvent.objects.create(
            execution=self.execution,
            type=HistoryEventType.ACTIVITY_FAILED.value,
            pos=self.pos,
            details={'error': error},
        )

    def schedule_retry(self, backoff_seconds: float):
        self.status = ActivityTask.Status.QUEUED
        self.after_time = timezone.now() + timedelta(seconds=backoff_seconds)
        self.save(update_fields=['status', 'error', 'after_time', 'updated_at'])

    def fail_due_to_cancel(self, finished_at=None):
        self.mark_failed(ErrorCode.WORKFLOW_CANCELED.value, finished_at=finished_at)

    class Meta:
        indexes = [
            models.Index(fields=['execution', 'status']),
            models.Index(fields=['status', 'after_time']),
            models.Index(fields=['status', 'expires_at']),
            models.Index(fields=['status', 'heartbeat_timeout']),
            models.Index(fields=['status', 'updated_at']),
        ]
