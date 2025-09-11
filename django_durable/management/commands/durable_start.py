import json

from django.core.management.base import BaseCommand, CommandError

from django_durable import register, start_workflow
from django_durable.exceptions import UnknownWorkflowError


class Command(BaseCommand):
    help = 'Start a workflow by name with optional JSON input.'

    def add_arguments(self, parser):
        parser.add_argument('workflow_name')
        parser.add_argument(
            '--input',
            default='{}',
            help='JSON object for workflow kwargs, e.g. \'{"user_id": 1}\'',
        )
        parser.add_argument(
            '--timeout',
            type=float,
            default=None,
            help='Optional workflow timeout in seconds.',
        )

    def handle(self, *args, **opts):
        name = opts['workflow_name']
        data = json.loads(opts['input'])
        timeout = opts['timeout']
        try:
            exec_id = start_workflow(name, timeout=timeout, **data)
        except UnknownWorkflowError as exc:
            raise CommandError(
                f"Unknown workflow '{name}'. Registered: {list(register.workflows)}"
            ) from exc
        self.stdout.write(self.style.SUCCESS(str(exec_id)))
