class DurableException(Exception):
    """Base class for all django durable exceptions."""


class WorkflowException(DurableException):
    """Base class for workflow-related exceptions."""


class WorkflowTimeout(WorkflowException):
    """Occurs when a workflow times out."""


class NondeterminismError(WorkflowException):
    """Event history does not line up during step/replay of a workflow."""


class ActivityException(DurableException):
    """Base class for activity-related exceptions."""


class ActivityTimeout(ActivityException):
    """Occurs when an activity times out."""


class ActivityError(ActivityException):
    """Wraps an error that propagates from an activity."""

    def __init__(self, error: Exception):
        self.error = error
        super().__init__(str(error))
