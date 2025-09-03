---
title: Design and Architecture
---

# Design and Architecture

This document explains how and why Django Durable works the way it does.

## Durable Execution Model

- Core idea: workflows are pure Python functions that are re-executed from the beginning on each step. Deterministic calls (activities, timers, signals) consult an append-only event log to return prior results instead of re-running side effects.
- Event log: `HistoryEvent` rows capture every deterministic decision: activity scheduled/completed/failed, signal wait/consumed, version markers, workflow start/completion/failure.
- Checkpointing: each activity call, signal, and timer creates a checkpoint. On crash/restart, the worker replays up to the first missing checkpoint and pauses.
- Determinism: avoid branching on non-replayable sources (e.g., random, time) unless derived from prior results or `ctx.get_version()` markers.

## Django Integration

- Models: durable state is stored in three models:
  - `WorkflowExecution`: lifecycle and result/error of each execution (with optional parent linkage for child workflows).
  - `HistoryEvent`: append-only event log, ordered by PK; includes a deterministic `pos` counter per execution.
  - `ActivityTask`: queued/running/completed activities with retry and timeout metadata.
- Admin: model admins provide visibility into executions, activities, and the full event history.
- Auto-discovery: at app startup, `django_durable.apps.DjangoDurableConfig` imports `durable_workflows` and `durable_activities` modules from installed apps.

## Worker Process

- Implementation: management command `durable_worker` runs a polling loop that:
  1) marks activity timeouts and heartbeats,
  2) executes due activities by spawning the `durable_internal_run_activity` command in a subprocess,
  3) steps runnable workflows by spawning the `durable_internal_step_workflow` command in a subprocess.
- Isolation: each activity or workflow step runs in its own process so the worker can terminate it if a timeout occurs.
- Concurrency: run multiple worker processes across hosts; database locks prevent double execution.
- The worker can manage multiple subprocesses at once; `--procs` controls the limit.
- Scheduling: activities have `after_time` and optional `expires_at`; retries use exponential backoff from `RetryPolicy`.

## Transactions and Atomicity

- The worker uses `transaction.atomic()` for scheduling and stepping to ensure consistency between event rows and state updates.
- Activity execution transitions (QUEUED → RUNNING → COMPLETED/FAILED/TIMED_OUT) update both `ActivityTask` and `HistoryEvent` in the same logical step.
- Workflow stepping writes `WORKFLOW_STARTED/COMPLETED/FAILED` events atomically with status changes.

## Serialization

- Inputs and outputs of activities and workflows are persisted as JSON; functions must return JSON-serializable data.
- Heartbeats optionally store JSON `heartbeat_details` on `ActivityTask`.

## Scaling

- Horizontal: run multiple worker processes across hosts; DB locking prevents double execution.
- Database: ensure appropriate indexes (provided via migrations) and tune connections. For Postgres, consider connection pooling.
- Timers: the worker calculates sleep time based on the next due activity to minimize idle polling.

## Reliability

- Crashes during workflow replay: replay is idempotent; a `NeedsPause` control-flow exception indicates when to yield until new checkpoints exist.
- Crashes during activity: the task remains RUNNING; heartbeat and schedule-to-close timeouts detect stalled tasks and retry or mark them timed out.
- Cancellation: `cancel_workflow` sets status to CANCELED, records events, and (by default) fails queued activities to prevent later execution. Child workflows are canceled recursively.
- Versioning: `ctx.get_version`, `ctx.patched`, and `ctx.deprecate_patch` enable safe migration of workflow logic while preserving determinism for in-flight executions.

## Comparison Notes

- Temporal: similar durable replay model with an external control plane and event histories; Django Durable trades external infra for Django-native storage and admin.
- DBOS: like Django Durable, emphasizes embedded durability via the database; implementation details and APIs differ.

## Execution Flow (Sequence)

1) User calls `start_workflow` → create `WorkflowExecution` and `WORKFLOW_STARTED` event.
2) Worker sees the execution as PENDING → calls workflow function from the beginning, consulting `HistoryEvent` rows.
3) On `ctx.run_activity`: append `ACTIVITY_SCHEDULED`, enqueue `ActivityTask`, and pause.
4) Worker executes due `ActivityTask` and writes `ACTIVITY_COMPLETED/FAILED/TIMED_OUT` → marks execution PENDING.
5) Worker replays workflow; when all steps have checkpoints, it reaches the next missing step → repeats until completion.

