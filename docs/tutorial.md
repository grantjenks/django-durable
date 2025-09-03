---
title: Tutorial
---

# Tutorial

Goal: build and run a durable workflow in a real Django project. Copy–paste friendly and minimal.

## 1) Setup

- Install and enable the app:

```bash
pip install django-durable
```

```python
# settings.py
INSTALLED_APPS = [
    # ...
    'django_durable',
]
```

- Migrate the database and create a superuser if you want to browse admin:

```bash
python manage.py migrate
python manage.py createsuperuser  # optional, for admin
```

- Create two files in one of your installed apps (Django Durable auto-discovers them):

```python
# yourapp/durable_activities.py
from django_durable.registry import register

@register.activity()
def send_welcome_email(user_id: int):
    # Do your side effect here; must return JSON-serializable data
    return {"status": "sent", "user_id": user_id}

@register.activity()
def compute_score(user_id: int):
    return {"score": 42}
```

```python
# yourapp/durable_workflows.py
from django_durable.registry import register

@register.workflow()
def onboard_user(ctx, user_id: int):
    ctx.run_activity("yourapp.send_welcome_email", user_id)
    ctx.sleep(3600)  # durable timer; no thread blocked
    score = ctx.run_activity("yourapp.compute_score", user_id)
    return {"ok": True, "score": score["score"]}
```

Notes:
- Activities and workflow results must be JSON-serializable.
- Workflows must be deterministic relative to their prior inputs and results. Avoid non-deterministic branching (e.g., using random/time/os state) unless derived from previous results or inputs.

## 2) Run the Worker

Start the worker process that executes workflows and activities out-of-band. You can run multiple workers in parallel.

```bash
python manage.py durable_worker --batch 20 --tick 0.2 --procs 4
```

Flags:
- `--batch`: max tasks per poll
- `--tick`: poll interval in seconds
- `--procs`: max subprocesses to manage concurrently

## 3) Start a Workflow

Start from the CLI:

```bash
python manage.py durable_start yourapp.onboard_user --input '{"user_id": 7}'
```

Or programmatically:

```python
from django_durable import start_workflow, wait_workflow

exec_id = start_workflow("yourapp.onboard_user", user_id=7)
result = wait_workflow(exec_id)  # blocks until completion
```

Alternatively, you can run synchronously for testing without a long-lived worker:

```python
from django_durable import run_workflow
result = run_workflow("yourapp.onboard_user", user_id=7)
```

## 4) Monitor in Admin

Open Django admin and browse:
- Workflow Executions (status, input, result, error)
- Activity Tasks (queued/running/completed/failed/timed out)
- History Events (append-only event log)

You’ll see timers, scheduled activities, and completions in the history.

## 5) Signals (optional)

Workflows can wait for external signals and resume deterministically:

```python
# yourapp/durable_workflows.py
@register.workflow()
def example_with_signal(ctx):
    ctx.sleep(0)
    payload = ctx.wait_signal("go")  # will pause until signal arrives
    return {"sig": payload}
```

Send a signal from the CLI or code:

```bash
python manage.py durable_signal <execution_uuid> go --input '{"clicked": true}'
```

```python
from django_durable import send_signal
send_signal(exec_id, "go", {"clicked": True})
```

## 6) Prove Durability

1. Start a workflow that has a timer and multiple steps (like `onboard_user`).
2. Kill the worker process in the middle of execution.
3. Restart the worker: `python manage.py durable_worker --procs 4`.
4. The workflow resumes from the next step. No work is lost; no step runs twice.

## 7) Wrap-up

You built a durable workflow, ran it with a worker, sent a signal, and observed execution in Django admin. From here you can:
- Add retries and timeouts to activities via `@register.activity(...)` or `RetryPolicy`.
- Use `ctx.get_version()` / `ctx.patched()` to roll out safe workflow changes while preserving determinism.
- Start child workflows via `ctx.run_workflow()` and wait on results.

