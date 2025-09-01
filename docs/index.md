---
title: Django Durable — Durable Workflows for Django
---

# Django Durable — Durable Workflows for Django

Django Durable provides durable execution of long-running workflows natively inside Django. Workflows are composed of deterministic steps and external activities that survive crashes and restarts. State is persisted in the database; execution is driven by a worker via management commands; progress and results are visible in Django admin.

## Quick Example

Define activities and a workflow in your Django app (auto-discovered from `durable_activities.py` and `durable_workflows.py`):

```python
# myapp/durable_activities.py
from django_durable.registry import register

@register.activity()
def send_email(user_id: int):
    return {"sent": True, "user_id": user_id}

@register.activity()
def compute_score(user_id: int):
    return {"score": 42}
```

```python
# myapp/durable_workflows.py
from django_durable.registry import register

@register.workflow()
def onboard_user(ctx, user_id: int):
    ctx.run_activity("send_email", user_id)
    ctx.sleep(3600)  # timer; no worker thread is blocked
    score = ctx.run_activity("compute_score", user_id)
    return {"ok": True, "score": score["score"]}
```

Run the worker and start the workflow:

```bash
python manage.py migrate
python manage.py durable_worker --threads 4
python manage.py durable_start onboard_user --input '{"user_id": 7}'
```

Monitor executions in Django admin. If the process crashes or restarts, the worker resumes from the next step.

## Features

- Durable workflows: resume after crashes and restarts
- First-class Django integration: ORM models, migrations, admin
- Management commands: worker, start, signal, status, cancel
- Pure-Python: no external services required
- Observable state: browse executions, events, and tasks in admin
- Tested and documented

## Installation

```bash
pip install django-durable
```

Add the app and run migrations:

```python
# settings.py
INSTALLED_APPS = [
    # ...
    'django_durable',
]
```

```bash
python manage.py migrate
```

Optional: Django Durable auto-discovers your code in `durable_workflows.py` and `durable_activities.py` modules of your installed apps.

## Documentation

- [Tutorial](tutorial.md)
- [Benchmarks](benchmarks.md)
- [API Reference](api.md)
- [Design](design.md)
- [Development](development.md)

```{toctree}
:hidden:
:maxdepth: 2

tutorial
benchmarks
api
design
development
```
