from typing import Any

from .engine import (
    _run_workflow,
    _start_workflow,
    _wait_workflow,
    cancel_workflow,
    signal_workflow,
)
from .models import WorkflowExecution
from .registry import register

__all__ = [
    "start_workflow",
    "wait_workflow",
    "run_workflow",
    "signal_workflow",
    "cancel_workflow",
    "register",
]

def start_workflow(workflow_name: str, timeout: float | None = None, **inputs) -> str:
    """Create a workflow execution and return its handle (ID)."""
    return _start_workflow(workflow_name, timeout=timeout, **inputs)

def wait_workflow(execution: WorkflowExecution | str) -> Any:
    """Wait for a workflow execution to complete and return its result."""
    return _wait_workflow(execution)

def run_workflow(workflow_name: str, timeout: float | None = None, **inputs) -> Any:
    """Convenience helper: start a workflow and wait for its result."""
    return _run_workflow(workflow_name, timeout=timeout, **inputs)
