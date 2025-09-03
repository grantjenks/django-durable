"""Benchmark script for django-durable.

Runs a simple workflow that calls a single activity. The script measures
throughput and step latencies while varying concurrency, payload sizes and
database backend. The results are printed as a small table showing p50/p95
latencies and overall throughput.
"""

import argparse
import multiprocessing
import os
import sys
import time


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark django-durable")
    parser.add_argument("--tasks", type=int, default=100, help="Number of workflows to run")
    parser.add_argument("--concurrency", type=int, default=10, help="Number of worker processes")
    parser.add_argument(
        "--payload-size",
        type=int,
        default=0,
        help="Size of payload (in bytes) passed to the activity",
    )
    parser.add_argument(
        "--backend",
        choices=["sqlite", "postgres"],
        default="sqlite",
        help="Database backend",
    )
    return parser.parse_args()


def worker(tick: float = 0.01) -> None:
    from django.core.management import call_command

    call_command("durable_worker", tick=tick)


def _wait_until_done(ActivityTask, WorkflowExecution) -> None:  # type: ignore[N803]
    while True:
        active_tasks = ActivityTask.objects.exclude(
            status__in=[
                ActivityTask.Status.COMPLETED,
                ActivityTask.Status.FAILED,
                ActivityTask.Status.TIMED_OUT,
            ]
        ).exists()
        active_wfs = WorkflowExecution.objects.exclude(
            status__in=[
                WorkflowExecution.Status.COMPLETED,
                WorkflowExecution.Status.FAILED,
                WorkflowExecution.Status.CANCELED,
                WorkflowExecution.Status.TIMED_OUT,
            ]
        ).exists()
        if not active_tasks and not active_wfs:
            break
        time.sleep(0.01)


def run_benchmark(tasks: int, concurrency: int, payload_size: int):
    from django.core.management import call_command
    from django.db import connection
    from django_durable import start_workflow
    from django_durable.models import ActivityTask, WorkflowExecution

    call_command("flush", verbosity=0, interactive=False)
    connection.close()

    ctx = multiprocessing.get_context("fork")
    procs = [ctx.Process(target=worker) for _ in range(concurrency)]
    for p in procs:
        p.start()
    time.sleep(0.5)

    payload = "x" * payload_size
    start = time.perf_counter()
    for _ in range(tasks):
        start_workflow("testproj.bench_flow", payload=payload)
    _wait_until_done(ActivityTask, WorkflowExecution)
    elapsed = time.perf_counter() - start

    for p in procs:
        p.terminate()
        p.join()

    durations = [
        (t.finished_at - t.started_at).total_seconds()
        for t in ActivityTask.objects.filter(activity_name="testproj.bench_activity")
    ]
    durations.sort()
    p50 = durations[len(durations) // 2]
    p95 = durations[int(0.95 * (len(durations) - 1))]
    throughput = tasks / elapsed if elapsed else 0.0
    return p50, p95, throughput


def main() -> None:
    args = parse_args()

    # Configure Django before setup
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "testproj.settings")
    os.environ["DJANGO_DB_BACKEND"] = args.backend
    sys.path.append(os.path.dirname(os.path.dirname(__file__)))

    import django
    from django.core.management import call_command

    django.setup()

    from django_durable.registry import register

    @register.activity()
    def bench_activity(payload: str) -> dict:
        return {"size": len(payload)}

    @register.workflow()
    def bench_flow(ctx, payload: str) -> dict:  # type: ignore[no-untyped-def]
        ctx.run_activity("testproj.bench_activity", payload)
        return {}

    call_command("migrate", verbosity=0, interactive=False)
    p50, p95, throughput = run_benchmark(
        args.tasks, args.concurrency, args.payload_size
    )
    print("backend  conc payload  p50(ms)  p95(ms)  throughput")
    print(
        f"{args.backend:<8} {args.concurrency:>5} {args.payload_size:>7} "
        f"{p50*1000:>8.2f} {p95*1000:>8.2f} {throughput:>11.2f}"
    )


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1].startswith("durable_internal"):
        os.environ.setdefault("DJANGO_SETTINGS_MODULE", "testproj.settings")
        sys.path.append(os.path.dirname(os.path.dirname(__file__)))
        import django
        django.setup()
        from django.core.management import call_command

        call_command(sys.argv[1], *sys.argv[2:])
    else:
        main()

