from django.core.management.base import BaseCommand, CommandError

from django_durable.engine import cancel_workflow
from django_durable.models import WorkflowExecution


class Command(BaseCommand):
    help = 'Cancel a workflow execution and its queued activities.'

    def add_arguments(self, parser):
        parser.add_argument('execution_id', help='WorkflowExecution UUID')
        parser.add_argument(
            '--reason', default='', help='Optional cancellation reason (string)'
        )
        parser.add_argument(
            '--keep-queued', action='store_true', help='Do not fail queued activities'
        )

    def handle(self, *args, **opts):
        exec_id = opts['execution_id']
        reason = opts['reason'] or None
        keep = bool(opts['keep_queued'])

        try:
            wf = WorkflowExecution.objects.get(pk=exec_id)
        except WorkflowExecution.DoesNotExist:
            raise CommandError(f'WorkflowExecution not found: {exec_id}')

        cancel_workflow(wf, reason=reason, cancel_queued_activities=not keep)
        self.stdout.write(self.style.SUCCESS('CANCELED'))
