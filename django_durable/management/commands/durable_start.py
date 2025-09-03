import json
from datetime import timedelta

from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from django_durable import register
from django_durable.models import WorkflowExecution


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
        if name not in register.workflows:
            raise CommandError(
                f"Unknown workflow '{name}'. Registered: {list(register.workflows)}"
            )

        data = json.loads(opts['input'])
        fn = register.workflows[name]
        timeout = opts['timeout']
        if timeout is None:
            timeout = getattr(fn, '_durable_timeout', None)
        expires_at = (
            timezone.now() + timedelta(seconds=float(timeout))
            if timeout is not None
            else None
        )
        wf = WorkflowExecution.objects.create(
            workflow_name=name, input=data, expires_at=expires_at
        )
        self.stdout.write(self.style.SUCCESS(str(wf.id)))
