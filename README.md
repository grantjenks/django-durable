# Django Durable

Durable execution framework for Django web apps.

Workflows are ordinary Python functions using `ctx.activity()` and `ctx.sleep()`. On each step, we replay from the start and use the **event log** (`HistoryEvent`) to return prior results. Avoid non-deterministic branching not derived from previous results or inputs.

Activity and workflow results must be JSON-serializable.


# TODO

- The worker currently uses `SELECT … FOR UPDATE SKIP LOCKED` to avoid thundering herds. Update this so it's used if postgres is the database, else not.

- Add support for signals

- Add support for cancellation

- Add Django admin.py setup for simple view of executions and history

- Rename the `not_before` activity field to `after_time`


# Commands

```bash
python manage.py durable_start onboard_user --input '{"user_id": 7}'
```

```bash
python manage.py durable_worker --batch 20 --tick 0.2
```
