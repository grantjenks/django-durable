from enum import Enum

from django.db import models


class HistoryEventType(models.TextChoices):
    VERSION_MARKER = 'version_marker'
    ACTIVITY_SCHEDULED = 'activity_scheduled'
    ACTIVITY_COMPLETED = 'activity_completed'
    ACTIVITY_FAILED = 'activity_failed'
    ACTIVITY_TIMED_OUT = 'activity_timed_out'
    ACTIVITY_CANCELED = 'activity_canceled'
    ACTIVITY_WAIT = 'activity_wait'
    SIGNAL_ENQUEUED = 'signal_enqueued'
    SIGNAL_WAIT = 'signal_wait'
    SIGNAL_CONSUMED = 'signal_consumed'
    CHILD_WORKFLOW_SCHEDULED = 'child_workflow_scheduled'
    CHILD_WORKFLOW_COMPLETED = 'child_workflow_completed'
    CHILD_WORKFLOW_FAILED = 'child_workflow_failed'
    CHILD_WORKFLOW_CANCELED = 'child_workflow_canceled'
    CHILD_WORKFLOW_TIMED_OUT = 'child_workflow_timed_out'
    CHILD_WORKFLOW_WAIT = 'child_workflow_wait'
    WORKFLOW_STARTED = 'workflow_started'
    WORKFLOW_COMPLETED = 'workflow_completed'
    WORKFLOW_FAILED = 'workflow_failed'
    WORKFLOW_CANCELED = 'workflow_canceled'
    WORKFLOW_TIMED_OUT = 'workflow_timed_out'


class ErrorCode(str, Enum):
    ACTIVITY_FAILED = 'activity_failed'
    ACTIVITY_TIMEOUT = 'activity_timeout'
    WORKFLOW_TIMEOUT = 'workflow_timeout'
    WORKFLOW_CANCELED = 'workflow_canceled'
    WORKFLOW_NOT_RUNNABLE = 'workflow_not_runnable'
    HEARTBEAT_TIMEOUT = 'heartbeat_timeout'
    PARENT_CANCELED = 'parent_canceled'


SLEEP_ACTIVITY_NAME = '__sleep__'
FINAL_EVENT_POS = 999_999
SPECIAL_EVENT_POS = 999_998
