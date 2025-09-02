from django.core.management.base import BaseCommand, CommandError

from django_durable.engine import step_workflow
from django_durable.models import WorkflowExecution


class Command(BaseCommand):
    help = 'Internal helper to advance a workflow execution.'

    def add_arguments(self, parser):
        parser.add_argument('execution_id', help='ID of the WorkflowExecution to step')

    def handle(self, *args, **options):
        exec_id = options['execution_id']
        try:
            wf = WorkflowExecution.objects.get(id=exec_id)
        except WorkflowExecution.DoesNotExist as exc:
            raise CommandError(f'Unknown workflow execution {exec_id}') from exc
        step_workflow(wf)
