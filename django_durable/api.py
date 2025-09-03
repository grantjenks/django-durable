from typing import Any, Optional, Union

from .engine import (
    _run_workflow,
    _start_workflow,
    _wait_workflow,
    cancel_workflow,
    query_workflow,
    send_signal,
)
from .models import WorkflowExecution

__all__ = [
    "start_workflow",
    "wait_workflow",
    "run_workflow",
    "send_signal",
    "query_workflow",
    "cancel_workflow",
]

def start_workflow(workflow_name: str, timeout: Optional[float] = None, **inputs) -> str:
    """Create a workflow execution and return its handle (ID)."""
    return _start_workflow(workflow_name, timeout=timeout, **inputs)

def wait_workflow(execution: Union[WorkflowExecution, str]) -> Any:
    """Wait for a workflow execution to complete and return its result."""
    return _wait_workflow(execution)

def run_workflow(workflow_name: str, timeout: Optional[float] = None, **inputs) -> Any:
    """Convenience helper: start a workflow and wait for its result."""
    return _run_workflow(workflow_name, timeout=timeout, **inputs)
