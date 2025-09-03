# Django Durable

Durable execution framework for Django web apps.

Workflows are ordinary Python functions using `ctx.run_activity()` and `ctx.sleep()`. On each step, we replay from the start and use the **event log** (`HistoryEvent`) to return prior results. Avoid non-deterministic branching not derived from previous results or inputs.

Activity and workflow results must be JSON-serializable.


# Commands

```bash
python manage.py durable_start onboard_user --input '{"user_id": 7}'
```

```bash
python manage.py durable_worker --batch 20 --tick 0.2 --procs 4
```

## Queries

- Inspect running workflow state via CLI:

```bash
python manage.py durable_status <execution_uuid>
```

- Programmatically:

```python
from django_durable import query_workflow
info = query_workflow(execution_id, 'status')
```

## Signals

- In code, a workflow can wait for signals using `ctx.wait_signal("signal_name")`, which pauses execution until a matching signal arrives and returns its payload.
- To send a signal from outside code:

```bash
python manage.py durable_signal <execution_uuid> user_clicked --input '{"clicked": true}'
```

You can also send a signal programmatically:

```python
from django_durable import send_signal
send_signal(execution_id, "user_clicked", {"clicked": True})
```

## Cancellation

- Cancel a workflow via CLI:

```bash
python manage.py durable_cancel <execution_uuid> --reason "user requested" [--keep-queued]
```

- In code:

```python
from django_durable import cancel_workflow
cancel_workflow(execution_id, reason="user requested")
```

Cancellation moves the workflow to CANCELED and, by default, marks queued activities as failed so workers will not run them.

## Benchmark

Run the repeatable benchmark via Nox. It supports configurable concurrency,
payload sizes and database backends. For example, to run 20 workflows with ten
workers on SQLite:

```bash
nox -s bench -- --tasks 20 --concurrency 10 --payload-size 0 --backend sqlite
```

Sample output:

```
backend  conc payload  p50(ms)  p95(ms)  throughput
sqlite     10       0    200.0    210.0        4.92
```

These numbers provide a rough sense of system overhead; actual throughput will
vary based on hardware and configuration.

## License

This project is licensed under the Apache License, Version 2.0. See the [LICENSE](LICENSE) file for details.
