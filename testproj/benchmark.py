import os
import time
import multiprocessing
import sys
import argparse

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "testproj.settings")

import django

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
django.setup()

from django.core.management import call_command
from django.db import connection
from django_durable.engine import start_activity, start_workflow
from django_durable.models import ActivityTask, WorkflowExecution

WORKERS = 10
TASKS = 100


def worker():
    call_command("durable_worker", tick=0.01)


def _wait_until_done():
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


def _run_benchmark(start_fn, count):
    call_command("flush", verbosity=0, interactive=False)
    connection.close()
    procs = [multiprocessing.Process(target=worker) for _ in range(WORKERS)]
    for p in procs:
        p.start()
    time.sleep(0.5)

    start = time.time()
    for _ in range(count):
        start_fn()
    _wait_until_done()
    elapsed = time.time() - start

    for p in procs:
        p.terminate()
        p.join()
    return count / elapsed


def benchmark_activities(count=TASKS):
    return _run_benchmark(lambda: start_activity("add", 1, 1), count)


def benchmark_workflows(count=TASKS):
    return _run_benchmark(lambda: start_workflow("add_flow", a=1, b=1), count)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Benchmark django-durable")
    parser.add_argument("--tasks", type=int, default=TASKS, help="Number of tasks per benchmark")
    args = parser.parse_args()
    call_command("migrate", verbosity=0, interactive=False)
    act_rate = benchmark_activities(args.tasks)
    wf_rate = benchmark_workflows(args.tasks)
    print(f"Activities per second: {act_rate:.2f}")
    print(f"Workflows per second: {wf_rate:.2f}")
