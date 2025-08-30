import json
from django.core.management.base import BaseCommand, CommandError
from durable.models import WorkflowExecution
from durable.registry import REG


class Command(BaseCommand):
    help = 'Start a workflow by name with optional JSON input.'

    def add_arguments(self, parser):
        parser.add_argument('workflow_name')
        parser.add_argument(
            '--input',
            default='{}',
            help='JSON object for workflow kwargs, e.g. \'{"user_id": 1}\'',
        )

    def handle(self, *args, **opts):
        name = opts['workflow_name']
        if name not in REG.workflows:
            raise CommandError(
                f"Unknown workflow '{name}'. Registered: {list(REG.workflows)}"
            )

        data = json.loads(opts['input'])
        wf = WorkflowExecution.objects.create(workflow_name=name, input=data)
        self.stdout.write(self.style.SUCCESS(str(wf.id)))
