---
title: Benchmarks and Comparison
---

# Benchmarks and Comparison

This page positions Django Durable against Temporal, DBOS, and (briefly) task queues like Celery/RQ. Results are indicative and easy to reproduce.

## What to Compare

- Architecture: embedded-in-Django vs external orchestrator
- Operational overhead: services and databases required
- Failure recovery model: per-activity checkpoints vs message passing / replay
- Developer effort: APIs, determinism constraints, migration strategy
- Throughput and latency: steady-state and tail behavior
- Monitoring and tooling: Django admin vs custom UIs and CLIs

## Qualitative Matrix

| Dimension | Django Durable | Temporal | DBOS | Celery/RQ |
|---|---|---|---|---|
| Orchestration | In-process worker, DB-backed | External service cluster | Embedded runtime + DB | Queue + workers |
| Infra | Django + DB only | Temporal Server + DB + dependencies | App + DB | Broker (Redis/Rabbit) + workers |
| Model | Deterministic workflow replay, per-activity checkpoints, signals | Deterministic workflow replay with event history | Durable execution with DB transaction logs | Tasks; no workflow determinism |
| State | Django models (executions, events, tasks) | Temporal namespaces/histories | DB tables/logs | Queue state only |
| Failure Recovery | Resume at next step via event log | Resume via event history replay | Resume via transaction log | Retries; manual compensation |
| Dev Experience | Pure Python, Django-first, admin | SDKs; strong typing; external UI | Python/TypeScript SDKs | Plain functions + retries |
| Determinism | Required inside workflows | Required inside workflows | Required inside workflows | Not required |
| Timers/Signals | Built-in (`ctx.sleep`, `ctx.wait_signal`) | Built-in | Built-in | Timers/signals ad hoc |
| Observability | Django admin + queries | Web UI + metrics | CLI/UI (project-specific) | Admin panels optional |
| Best Fit | Django apps seeking zero extra infra | High-scale, polyglot, multi-service | DB-first teams wanting embedded | Fire-and-forget jobs |

Notes:
- Django Durable uses the Django database for durable state—no new infra.
- Temporal provides rich multi-language support and production tooling at the cost of running its control plane.
- DBOS embeds durable execution directly with your DB; conceptually close to Django Durable’s embedded approach.

## Throughput (Example)

Repository includes a simple benchmark for workflow throughput:

```bash
python testproj/benchmark.py --tasks 20
```

Environment (example):
- MacBook Pro (Apple Silicon), Python 3.13, SQLite default DB
- 10 worker processes, `--tick 0.01`

Example results (one run):

```
Workflows per second: 5.0
```

Interpretation:
- The number reflects framework overhead for a minimal workflow (`add_flow`): a single activity call + JSON serialization + DB writes for events and tasks.
- Results vary by hardware, DB backend, and settings. For Postgres, throughput typically improves with proper indexing and connections.

## When to Choose What

- Django Durable
  - You want durable workflows inside a Django app without new infrastructure.
  - Admin visibility, migrations, and ORM integration matter.
  - Workflows are modest in scale and Python-only is fine.

- Temporal
  - You need polyglot SDKs, massive scale, and mature UI/operability.
  - Running the control plane is acceptable (Kubernetes, services, DB).

- DBOS
  - You prefer an embedded runtime centered on your database.
  - You want durable execution with transactional semantics close to SQL.

- Celery/RQ (task queues)
  - One-off jobs and background tasks without deterministic workflow needs.
  - You accept building your own compensation and recovery logic.

## Reproducibility Notes

- Use the included benchmark script and adjust `--tasks` to your environment.
- For apples-to-apples, pin Python and Django versions and keep worker settings constant.
- Always state DB (SQLite/Postgres), hardware, and worker counts when reporting numbers.

