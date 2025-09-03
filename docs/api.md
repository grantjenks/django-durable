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
exec_id = start_workflow("myapp.onboard_user", user_id=7)
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

```{autofunction} django_durable.api.signal_workflow
```

- Summary: Enqueue an external signal for a workflow and mark it runnable.
- Params: `execution: Union[WorkflowExecution, str]`, `name: str`, `payload: Any = None`
- Returns: `None`
- Example:

```python
from django_durable import signal_workflow
signal_workflow(exec_id, "go", {"clicked": True})
```

```{autofunction} django_durable.api.cancel_workflow
```

- Summary: Cancel a workflow execution and optionally fail queued activities.
- Params: `execution: Union[WorkflowExecution, str]`, `reason: str | None = None`, `cancel_queued_activities: bool = True`
- Returns: `None`
- Example:

```python
from django_durable import cancel_workflow
cancel_workflow(exec_id, reason="user requested")
```


## Registry and Decorators

The registry provides decorators to declare workflows and activities. Import from `django_durable`.

- `register.workflow(timeout: float | None = None)`
  - Registers a workflow function. The function signature is `fn(ctx, **inputs)` and must be deterministic relative to inputs and prior results.
  - Registered names are automatically generated as `{app_name}.{func_name}` and stored on the function at `._durable_name`.
  - Optional `timeout` sets a deadline for the workflow; when exceeded, the workflow times out and children are canceled.
  - Example:
    ```python
    from django_durable import register

    @register.workflow(timeout=3600)
    def add_flow(ctx, a: int, b: int):
        res = ctx.run_activity("myapp.add", a, b)
        return {"value": res["value"]}
    ```

- `register.activity(timeout: float | None = None, heartbeat_timeout: float | None = None, retry_policy: RetryPolicy | None = None)`
  - Registers an activity function that runs outside workflow replay. Must return JSON-serializable data.
  - Retries: pass a `RetryPolicy` for backoff control. Defaults: `initial_interval=1s`, `backoff_coefficient=2.0`, `maximum_interval=60s`, `strategy='exponential'`, `jitter=0`.
  - Timeouts: `timeout` sets schedule-to-close deadline; `heartbeat_timeout` enforces activity heartbeats.
  - Example:
    ```python
    from django_durable import register
    from django_durable.retry import RetryPolicy

    @register.activity(
        retry_policy=RetryPolicy(initial_interval=0.1, maximum_attempts=3)
    )
    def flaky_op(key: str):
        ...
    ```

```{autoclass} django_durable.retry.RetryPolicy
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

- `durable_worker [--tick FLOAT] [--batch INT] [--iterations INT] [--procs INT]`
  - Runs the worker loop executing due activities and stepping runnable workflows.
  - `--iterations`: stop after N iterations (testing)
  - `--procs`: maximum concurrent subprocesses (default 4)

- `durable_start WORKFLOW_NAME [--input JSON] [--timeout FLOAT]`
  - Starts a workflow by name with optional JSON kwargs. Prints the execution UUID.

- `durable_signal EXECUTION_ID SIGNAL_NAME [--input JSON]`
  - Sends a signal to a workflow with an optional JSON payload.

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

