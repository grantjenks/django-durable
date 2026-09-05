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
  2) dispatches due activities and runnable workflows to long-lived follower
     subprocesses via JSON over stdin, with acknowledgements on a dedicated
     control pipe. Nonblocking framed reads keep partial acknowledgements from
     blocking the dispatcher; application stdout/stderr go to worker stderr.
- Isolation: each activity or workflow step runs in a follower process so the
  worker can terminate it if a timeout occurs.
- Concurrency: run multiple worker processes across hosts; atomic claims and
  attempt tokens coordinate activity ownership. External side effects must be
  idempotent because execution is at least once.
- The worker manages a pool of follower subprocesses; `--procs` controls the
  limit. Final acknowledgements mark retiring followers so the dispatcher
  replaces them before claiming another task.
- Scheduling: activities have `after_time` and optional `expires_at`; retries
  use exponential backoff from `RetryPolicy`.

## Transactions and Atomicity

- The worker uses `transaction.atomic()` for scheduling and stepping to ensure consistency between event rows and state updates.
- Activity execution transitions (QUEUED → RUNNING → COMPLETED/FAILED/TIMED_OUT) update both `ActivityTask` and `HistoryEvent` in the same logical step.
- Workflow stepping writes `WORKFLOW_STARTED/COMPLETED/FAILED` events atomically with status changes.

## Serialization

- Inputs and outputs of activities and workflows are persisted as JSON; functions must return JSON-serializable data.
- Heartbeats optionally store JSON `heartbeat_details` on `ActivityTask`.

## Scaling

- Horizontal: run multiple worker processes across hosts; database transactions
  serialize workflow replay and activity transitions.
- Database: ensure appropriate indexes (provided via migrations) and tune connections. For Postgres, consider connection pooling.
- Timers: the worker calculates sleep time based on the next due activity to minimize idle polling.

## Reliability

- Crashes during workflow replay: replay is idempotent; a `NeedsPause` control-flow exception indicates when to yield until new checkpoints exist.
- Crashes during activity: expiring leases recover orphaned attempts; heartbeat
  timeouts detect stalled progress; schedule-to-close deadlines are terminal.
- Cancellation: `cancel_workflow` sets status to CANCELED, records events, and fails queued activities to prevent later execution. Child workflows and active activities are canceled automatically.
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


## Recovery and deadline guarantees

Activity delivery is **at least once**. An activity can perform an external side
effect and crash before its result is committed, so external operations must be
idempotent (for example, using an application-defined idempotency key).

Each activity attempt is claimed with a unique lease token before dispatch. The
worker renews the lease while its follower is alive. A replacement worker can
recover an expired lease, including a task whose original worker crashed before
sending it to a follower. Completion, failure, retries, and heartbeats check the
attempt token so that an older attempt cannot overwrite its replacement's state.
This protects stored results; it cannot undo an external side effect.

The default lease duration is 30 seconds and can be configured with
`DURABLE_ACTIVITY_LEASE_SECONDS`. Set it comfortably above the worker's `--tick`
and expected scheduling/database delays. Application `heartbeat_timeout` remains
separate: it detects stalled activity progress even while the worker is alive.

Activity terminal state, its history event, and the workflow wakeup commit in one
transaction. A schedule-to-close timeout covers the entire activity, including
queueing and retries, and always ends it as `TIMED_OUT`. Heartbeat failures and
worker loss may retry within that overall deadline and the retry policy.
Workflow completion or failure also finishes outstanding queued/running activities
with `workflow_not_runnable`, in the same transaction.

Timed activity/child waits persist a workflow wake time. A result committed after
the wait deadline does not change the timeout branch on later replay. Timing out
a wait does not cancel the underlying activity or child workflow. Each wait call
on a handle has its own deterministic identity and deadline; catching a timeout
and waiting again starts a separate wait without shifting command positions.

`run_workflow()` drives the scheduler inline, including deadlines, and renews
activity leases while application code runs. It cannot interrupt Python code in
the caller's thread: deadlines are enforced before/after that code returns. Use
`start_workflow()` with `durable_worker` for subprocess-enforced interruption.

For upgrades, stop all old workers before applying migration 0008 and restarting
workers. The migration requeues existing running workflows once, allowing replay
to rebuild wake times for legacy waits. Older worker versions do not participate
in the lease protocol.
