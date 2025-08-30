import socket
import time
from datetime import timedelta
from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone

from durable.engine import step_workflow, execute_activity
from durable.models import WorkflowExecution, ActivityTask


class Command(BaseCommand):
    help = 'Run the django-durable worker (workflows + activities).'

    def add_arguments(self, parser):
        parser.add_argument(
            '--tick', type=float, default=0.5, help='Poll interval in seconds.'
        )
        parser.add_argument('--batch', type=int, default=10, help='Max tasks per tick.')

    def handle(self, *args, **opts):
        tick = opts['tick']
        batch = opts['batch']
        hostname = socket.gethostname()
        self.stdout.write(self.style.SUCCESS(f'[durable] worker started on {hostname}'))

        while True:
            now = timezone.now()
            progressed = False

            # 1) Run due activities
            due = list(
                ActivityTask.objects.select_for_update(skip_locked=True)
                .filter(status=ActivityTask.Status.QUEUED, not_before__lte=now)
                .order_by('updated_at')[:batch]
            )
            if due:
                progressed = True
            for t in due:
                with transaction.atomic():
                    # Lock row
                    t.refresh_from_db()
                    if t.status != ActivityTask.Status.QUEUED or t.not_before > now:
                        continue
                    execute_activity(t)

            # 2) Advance workflows
            runnables = list(
                WorkflowExecution.objects.select_for_update(skip_locked=True)
                .filter(
                    status__in=[
                        WorkflowExecution.Status.PENDING,
                        WorkflowExecution.Status.RUNNING,
                    ]
                )
                .order_by('updated_at')[:batch]
            )
            if runnables:
                progressed = True
            for wf in runnables:
                with transaction.atomic():
                    wf.refresh_from_db()
                    if wf.status not in (
                        WorkflowExecution.Status.PENDING,
                        WorkflowExecution.Status.RUNNING,
                    ):
                        continue
                    step_workflow(wf)

            if not progressed:
                time.sleep(tick)
