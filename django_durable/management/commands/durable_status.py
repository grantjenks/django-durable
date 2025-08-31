import json

from django.core.management.base import BaseCommand, CommandError

from django_durable.engine import query_workflow
from django_durable.models import WorkflowExecution


class Command(BaseCommand):
    help = 'Query a workflow execution for its current state.'

    def add_arguments(self, parser):
        parser.add_argument('execution_id', help='WorkflowExecution UUID')
        parser.add_argument(
            '--query', default='status', help='Query name (default: status)'
        )
        parser.add_argument(
            '--input', default='{}', help='JSON kwargs for the query (default: {})'
        )

    def handle(self, *args, **opts):
        exec_id = opts['execution_id']
        query_name = opts['query']
        try:
            payload = json.loads(opts['input'])
            if not isinstance(payload, dict):
                raise ValueError('Query input must be a JSON object')
        except Exception as e:
            raise CommandError(f'Invalid JSON for --input: {e}')

        try:
            wf = WorkflowExecution.objects.get(pk=exec_id)
        except WorkflowExecution.DoesNotExist:
            raise CommandError(f'WorkflowExecution not found: {exec_id}')

        try:
            res = query_workflow(wf, query_name, **payload)
        except KeyError as e:
            raise CommandError(str(e))

        self.stdout.write(json.dumps(res))
