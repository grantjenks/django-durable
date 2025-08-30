import uuid
from django.db import models
from django.utils import timezone


class WorkflowExecution(models.Model):
    class Status(models.TextChoices):
        PENDING = 'PENDING'
        RUNNING = 'RUNNING'
        WAITING = 'WAITING'
        COMPLETED = 'COMPLETED'
        FAILED = 'FAILED'
        CANCELED = 'CANCELED'

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    workflow_name = models.CharField(max_length=200)
    input = models.JSONField(default=dict, blank=True)
    status = models.CharField(
        max_length=20, choices=Status.choices, default=Status.PENDING
    )
    result = models.JSONField(null=True, blank=True)
    error = models.TextField(null=True, blank=True)
    started_at = models.DateTimeField(default=timezone.now)
    finished_at = models.DateTimeField(null=True, blank=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f'{self.workflow_name}:{self.id}'


class HistoryEvent(models.Model):
    # Append-only event log; ordered by autoincrement PK
    execution = models.ForeignKey(
        WorkflowExecution, related_name='history', on_delete=models.CASCADE
    )
    type = models.CharField(max_length=64)
    pos = models.IntegerField(
        default=0
    )  # deterministic call index within workflow replay
    details = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        indexes = [
            models.Index(fields=['execution', 'pos']),
            models.Index(fields=['execution', 'type']),
        ]


class ActivityTask(models.Model):
    class Status(models.TextChoices):
        QUEUED = 'QUEUED'
        RUNNING = 'RUNNING'
        COMPLETED = 'COMPLETED'
        FAILED = 'FAILED'

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
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
    attempt = models.IntegerField(default=0)
    max_attempts = models.IntegerField(default=0)
    result = models.JSONField(null=True, blank=True)
    error = models.TextField(null=True, blank=True)
    started_at = models.DateTimeField(null=True, blank=True)
    finished_at = models.DateTimeField(null=True, blank=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        indexes = [
            models.Index(fields=['execution', 'status']),
            models.Index(fields=['status', 'after_time']),
        ]
