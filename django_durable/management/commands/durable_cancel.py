from django.core.management.base import BaseCommand, CommandError

from django_durable import cancel_workflow
from django_durable.models import WorkflowExecution


class Command(BaseCommand):
    help = 'Cancel a workflow execution.'

    def add_arguments(self, parser):
        parser.add_argument('execution_id', help='WorkflowExecution ID')
        parser.add_argument(
            '--reason', default='', help='Optional cancellation reason (string)'
        )

    def handle(self, *args, **opts):
        exec_id = opts['execution_id']
        reason = opts['reason'] or None

        try:
            wf = WorkflowExecution.objects.get(pk=exec_id)
        except WorkflowExecution.DoesNotExist:
            raise CommandError(f'WorkflowExecution not found: {exec_id}')

        cancel_workflow(wf, reason=reason)
        self.stdout.write(self.style.SUCCESS('CANCELED'))
