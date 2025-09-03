import json

from django.core.management.base import BaseCommand, CommandError

from django_durable.engine import signal_workflow
from django_durable.models import WorkflowExecution


class Command(BaseCommand):
    help = 'Send a signal to a workflow execution.'

    def add_arguments(self, parser):
        parser.add_argument('execution_id', help='WorkflowExecution UUID')
        parser.add_argument('signal_name', help='Signal name')
        parser.add_argument(
            '--input',
            default='null',
            help='JSON payload for the signal (default null).',
        )

    def handle(self, *args, **opts):
        exec_id = opts['execution_id']
        name = opts['signal_name']
        try:
            payload = json.loads(opts['input'])
        except json.JSONDecodeError as e:
            raise CommandError(f'Invalid JSON for --input: {e}')

        try:
            wf = WorkflowExecution.objects.get(pk=exec_id)
        except WorkflowExecution.DoesNotExist:
            raise CommandError(f'WorkflowExecution not found: {exec_id}')

        signal_workflow(wf, name, payload)
        self.stdout.write(self.style.SUCCESS('OK'))
