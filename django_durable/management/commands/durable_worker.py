import socket
import time
from django.core.management.base import BaseCommand
from django.db import DatabaseError
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
                # Claim the task atomically so other workers skip it.
                claimed = (
                    ActivityTask.objects.filter(
                        id=tid,
                        status=ActivityTask.Status.QUEUED,
                        after_time__lte=now,
                    ).update(status=ActivityTask.Status.RUNNING)
                )
                if not claimed:
                    continue
                try:
                    task = ActivityTask.objects.get(id=tid)
                    execute_activity(task)
                except DatabaseError:
                    # Revert claim so another worker can retry.
                    ActivityTask.objects.filter(id=tid).update(
                        status=ActivityTask.Status.QUEUED
                    )
                    continue

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
                # Only run if we can transition PENDING -> RUNNING. If another
                # worker grabbed it first, the update returns 0 and we skip.
                claimed = (
                    WorkflowExecution.objects.filter(
                        id=wid,
                        status=WorkflowExecution.Status.PENDING,
                    ).update(status=WorkflowExecution.Status.RUNNING)
                )
                if not claimed:
                    continue
                try:
                    wf = WorkflowExecution.objects.get(id=wid)
                    step_workflow(wf)
                except DatabaseError:
                    WorkflowExecution.objects.filter(id=wid).update(
                        status=WorkflowExecution.Status.PENDING
                    )
                    continue

            loops += 1
            if iterations is not None and loops >= iterations:
                break
            if not progressed:
                time.sleep(tick)
