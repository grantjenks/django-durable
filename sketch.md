## Example: define tasks & workflows in your app

Create `myapp/durable_tasks.py`:

```python
from durable.registry import REG
from time import sleep

@REG.activity(max_retries=3)
def send_welcome_email(user_id: int):
    # do side-effect; return serializable result
    # (pretend)
    return {"status": "sent", "user_id": user_id}

@REG.activity()
def confirm_clicked(user_id: int):
    # pretend we looked up a flag somewhere
    return {"clicked": True}

# Internal example: long compute (avoid real sleeps; use workflow ctx.sleep instead)
@REG.activity()
def compute_score(user_id: int):
    # pure CPU or short IO; return JSON-serializable data
    return {"score": 42}
```

Create `myapp/durable_workflows.py`:

```python
from durable.registry import REG

@REG.workflow()
def onboard_user(ctx, user_id: int):
    # 1) send email (schedules ActivityTask, then pauses; worker resumes deterministically)
    res = ctx.activity("send_welcome_email", user_id)
    # 2) wait 1 hour without blocking a worker thread
    ctx.sleep(3600)
    # 3) check confirmation
    clicked = ctx.activity("confirm_clicked", user_id)
    if not clicked["clicked"]:
        # try again in a day
        ctx.sleep(24 * 3600)
        ctx.activity("send_welcome_email", user_id)

    # 4) compute score and finish
    score = ctx.activity("compute_score", user_id)
    return {"ok": True, "score": score["score"]}
```

---

## How to run

1. Add the app:

```python
# settings.py
INSTALLED_APPS += ["durable"]
```

2. Migrate:

```bash
python manage.py makemigrations durable
python manage.py migrate
```

3. Define your `durable_tasks.py` / `durable_workflows.py` in one of your apps (auto-discovered).

4. Start a workflow and a worker:

```bash
python manage.py start_workflow onboard_user --input '{"user_id": 7}'
python manage.py durable_worker --batch 20 --tick 0.2
```

---

## Design notes & limitations (by intent)

* **Deterministic replay**: Workflows are ordinary Python functions using `ctx.activity()` and `ctx.sleep()`. On each step, we replay from the start and use the **event log** (`HistoryEvent`) to return prior results. Avoid non-deterministic branching not derived from previous results or inputs.
* **Timers**: Implemented as a special `"__sleep__"` activity with a `not_before` timestamp; workers only run due timers.
* **Retries**: Simple linear backoff (`+30s/attempt`) up to `max_retries` set on the activity decorator.
* **Safety**: Activity and workflow results must be JSON-serializable.
* **Postgres**: The worker uses `SELECT … FOR UPDATE SKIP LOCKED` to avoid thundering herds.

If you’d like, I can add signals/cancellation next, plus a tiny admin view for executions and history, or a `Procfile` snippet for Dokku.
