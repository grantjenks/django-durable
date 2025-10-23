from typing import Any, Callable

from .engine import (
    _run_workflow,
    _start_workflow,
    cancel_workflow,
    signal_workflow,
)
from .models import WorkflowExecution
from .registry import register

__all__ = [
    'start_workflow',
    'wait_workflow',
    'run_workflow',
    'signal_workflow',
    'cancel_workflow',
    'register',
]


def start_workflow(
    workflow: str | Callable, timeout: float | None = None, **inputs
) -> str:
    """Create a workflow execution and return its handle (ID)."""
    return _start_workflow(workflow, timeout=timeout, **inputs)


def wait_workflow(
    execution: WorkflowExecution | int | str, timeout: float | None = None
) -> Any:
    """Wait for a workflow execution to complete and return its result.

    Args:
        execution: WorkflowExecution object or its ID.
        timeout: Maximum seconds to wait. ``0`` checks once without waiting.

    Raises:
        WaitWorkflowTimeout: If the workflow does not complete within ``timeout``.
        WorkflowTimeout: If the workflow itself times out.
        WorkflowException: If the workflow ends in FAILED or CANCELED.
    """
    if not isinstance(execution, WorkflowExecution):
        execution = WorkflowExecution.objects.get(pk=execution)

    return execution.wait(timeout=timeout)


def run_workflow(
    workflow: str | Callable, timeout: float | None = None, **inputs
) -> Any:
    """Convenience helper: start a workflow and wait for its result."""
    return _run_workflow(workflow, timeout=timeout, **inputs)
