---
title: API Reference
---

# API Reference

This reference lists the public surface exposed by `django_durable` and commonly used helpers. Functions re-exported via `django_durable.__all__` are the primary public API.

## Execution Functions

```{autofunction} django_durable.api.start_workflow
```

- Summary: Create a workflow execution and return its handle (UUID string).
- Params: `workflow_name: str`, `timeout: float | None = None`, `**inputs`
- Returns: `str` execution ID
- Example:

```python
from django_durable import start_workflow
exec_id = start_workflow("onboard_user", user_id=7)
```

```{autofunction} django_durable.api.wait_workflow
```

- Summary: Block until the workflow completes and return its result.
- Params: `execution: Union[WorkflowExecution, str]`
- Returns: JSON-serializable result
- Example:

```python
from django_durable import wait_workflow
result = wait_workflow(exec_id)
```

```{autofunction} django_durable.api.run_workflow
```

- Summary: Convenience helper: start a workflow and wait for its result.
- Params: `workflow_name: str`, `timeout: float | None = None`, `**inputs`
- Returns: JSON-serializable result

```{autofunction} django_durable.api.send_signal
```

- Summary: Enqueue an external signal for a workflow and mark it runnable.
- Params: `execution: Union[WorkflowExecution, str]`, `name: str`, `payload: Any = None`
- Returns: `None`
- Example:

```python
from django_durable import send_signal
send_signal(exec_id, "go", {"clicked": True})
```

```{autofunction} django_durable.api.query_workflow
```

- Summary: Execute a registered read-only query against a workflow.
- Params: `execution: Union[WorkflowExecution, str]`, `name: str`, `**payload`
- Returns: query result (JSON-serializable)
- Notes: Built-in `status` query returns `{id, workflow_name, status, result, error, pending_activities}`.

## Registry and Decorators

The registry provides decorators to declare workflows, activities, and queries. Import from `django_durable.registry`.

- `register.workflow(name: str | None = None, timeout: float | None = None)`
  - Registers a workflow function. The function signature is `fn(ctx, **inputs)` and must be deterministic relative to inputs and prior results.
  - Optional `timeout` sets a deadline for the workflow; when exceeded, the workflow times out and children are canceled.
  - Example:
    ```python
    from django_durable.registry import register

    @register.workflow(timeout=3600)
    def add_flow(ctx, a: int, b: int):
        res = ctx.run_activity("add", a, b)
        return {"value": res["value"]}
    ```

- `register.activity(name: str | None = None, max_retries: int = 0, timeout: float | None = None, heartbeat_timeout: float | None = None, retry_policy: RetryPolicy | None = None)`
  - Registers an activity function that runs outside workflow replay. Must return JSON-serializable data.
  - Retries: either set `max_retries` or pass a `RetryPolicy` for backoff control.
  - Timeouts: `timeout` sets schedule-to-close deadline; `heartbeat_timeout` enforces activity heartbeats.
  - Example:
    ```python
    from django_durable.registry import register, RetryPolicy

    @register.activity(
        retry_policy=RetryPolicy(initial_interval=0.1, maximum_attempts=3)
    )
    def flaky_op(key: str):
        ...
    ```

- `register.query(workflow_name: str, name: str | None = None)`
  - Registers a read-only query handler for a workflow. Handlers run inline without modifying state.
  - Example:
    ```python
    @register.query("e2e_flow")
    def history(execution):
        return {"events": execution.history.count()}
    ```

```{autoclass} django_durable.registry.RetryPolicy
:members:
```

## Workflow Context (for workflow functions)

Each workflow function receives `ctx`, which exposes deterministic APIs used during replay. Key methods:

- `ctx.run_activity(name, *args, **kwargs) -> Any`: schedule and wait for an activity; returns its result.
- `ctx.start_activity(name, *args, **kwargs) -> int`: schedule an activity and return a handle.
- `ctx.wait_activity(handle: int) -> Any`: wait for a previously started activity.
- `ctx.sleep(seconds: float)`: durable timer; never blocks a worker thread.
- `ctx.wait_signal(name: str) -> Any`: wait for an external signal and resume with its payload.
- `ctx.run_workflow(name: str, **inputs) -> Any`: start and wait for a child workflow.
- `ctx.start_workflow(name: str, **inputs) -> str`: start a child workflow; returns its handle.
- `ctx.wait_workflow(handle: str) -> Any`: wait for a child workflow by handle.
- Versioning helpers:
  - `ctx.get_version(change_id: str, version: int) -> int`
  - `ctx.patched(change_id: str) -> bool`
  - `ctx.deprecate_patch(change_id: str) -> None`

These methods are deterministic: on replay, they consult the `HistoryEvent` log to return prior results.

## Activity Heartbeats

Activities can emit heartbeats to report liveness and progress. If `heartbeat_timeout` is set on the activity decorator, workers enforce the timeout across heartbeats.

```python
from django_durable.engine import activity_heartbeat

def my_activity():
    activity_heartbeat({"step": 1})
    ...
```

## Management Commands

- `durable_worker [--tick FLOAT] [--batch INT] [--iterations INT] [--threads INT]`
  - Runs the worker loop executing due activities and stepping runnable workflows.
  - `--threads`: number of worker threads (0 runs synchronously in the foreground)
  - `--iterations`: stop after N iterations (testing)

- `durable_start WORKFLOW_NAME [--input JSON] [--timeout FLOAT]`
  - Starts a workflow by name with optional JSON kwargs. Prints the execution UUID.

- `durable_signal EXECUTION_ID SIGNAL_NAME [--input JSON]`
  - Sends a signal to a workflow with an optional JSON payload.

- `durable_status EXECUTION_ID [--query NAME] [--input JSON]`
  - Executes a read-only query for the execution (defaults to `status`). Prints JSON.

- `durable_cancel EXECUTION_ID [--reason STR] [--keep-queued]`
  - Cancels the workflow. By default, queued activities are failed to prevent execution.

## Settings and Conventions

- Auto-discovery: Django Durable imports `durable_workflows` and `durable_activities` modules from installed apps on startup.
- Serialization: inputs/outputs must be JSON-serializable.
- Determinism: avoid branching on non-deterministic values not derived from prior results or inputs.

## Errors and Exceptions

- `start_workflow`: raises `KeyError` if the named workflow is not registered.
- `wait_workflow`/`run_workflow`: raise `RuntimeError` if the workflow ends in FAILED/TIMED_OUT/CANCELED; message contains the error code or text.
- Activities with retries/timeouts propagate failure info to the workflow via history events.

