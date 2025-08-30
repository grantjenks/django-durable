import socket
import time
from datetime import timedelta
from django.core.management.base import BaseCommand
from django.db import transaction
from django.db import DatabaseError
from django.db.utils import NotSupportedError
from django.utils import timezone

from django_durable.engine import step_workflow, execute_activity
from django_durable.models import WorkflowExecution, ActivityTask


class Command(BaseCommand):
    help = 'Run the django-durable worker (workflows + activities).'

    def add_arguments(self, parser):
        parser.add_argument(
            '--tick', type=float, default=0.5, help='Poll interval in seconds.'
        )
        parser.add_argument('--batch', type=int, default=10, help='Max tasks per tick.')
        parser.add_argument(
            '--iterations',
            type=int,
            default=None,
            help='Optional number of loop iterations to run (for testing).',
        )

    def handle(self, *args, **opts):
        tick = opts['tick']
        batch = opts['batch']
        iterations = opts['iterations']
        hostname = socket.gethostname()
        self.stdout.write(self.style.SUCCESS(f'[durable] worker started on {hostname}'))

        loops = 0
        while True:
            now = timezone.now()
            progressed = False

            # 1) Run due activities
            due_ids = list(
                ActivityTask.objects.filter(
                    status=ActivityTask.Status.QUEUED,
                    after_time__lte=now,
                    execution__status__in=[
                        WorkflowExecution.Status.PENDING,
                        WorkflowExecution.Status.RUNNING,
                        WorkflowExecution.Status.WAITING,
                    ],
                )
                .order_by('updated_at')
                .values_list('id', flat=True)[:batch]
            )
            if due_ids:
                progressed = True
            for tid in due_ids:
                with transaction.atomic():
                    # Attempt to lock the row if backend supports it; fall back gracefully.
                    qs = ActivityTask.objects.filter(id=tid)
                    task = None
                    try:
                        task = qs.select_for_update(skip_locked=True).first()
                    except (NotSupportedError, DatabaseError):
                        try:
                            task = qs.select_for_update().first()
                        except (NotSupportedError, DatabaseError):
                            task = qs.first()

                    if not task:
                        continue
                    # Recheck current state to avoid double-processing.
                    if (
                        task.status != ActivityTask.Status.QUEUED
                        or task.after_time > now
                    ):
                        continue
                    execute_activity(task)

            # 2) Advance workflows
            runnable_ids = list(
                WorkflowExecution.objects.filter(
                    status__in=[
                        WorkflowExecution.Status.PENDING,
                        WorkflowExecution.Status.RUNNING,
                    ]
                )
                .order_by('updated_at')
                .values_list('id', flat=True)[:batch]
            )
            if runnable_ids:
                progressed = True
            for wid in runnable_ids:
                with transaction.atomic():
                    qs = WorkflowExecution.objects.filter(id=wid)
                    wf = None
                    try:
                        wf = qs.select_for_update(skip_locked=True).first()
                    except (NotSupportedError, DatabaseError):
                        try:
                            wf = qs.select_for_update().first()
                        except (NotSupportedError, DatabaseError):
                            wf = qs.first()

                    if not wf:
                        continue
                    if wf.status not in (
                        WorkflowExecution.Status.PENDING,
                        WorkflowExecution.Status.RUNNING,
                    ):
                        continue
                    step_workflow(wf)

            loops += 1
            if iterations is not None and loops >= iterations:
                break
            if not progressed:
                time.sleep(tick)
