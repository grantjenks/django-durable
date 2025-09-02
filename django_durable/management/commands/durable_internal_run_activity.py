from django.core.management.base import BaseCommand, CommandError

from django_durable.engine import execute_activity
from django_durable.models import ActivityTask


class Command(BaseCommand):
    help = 'Internal helper to run a single activity task.'

    def add_arguments(self, parser):
        parser.add_argument('task_id', help='ID of the ActivityTask to run')

    def handle(self, *args, **options):
        task_id = options['task_id']
        try:
            task = ActivityTask.objects.get(id=task_id)
        except ActivityTask.DoesNotExist as exc:
            raise CommandError(f'Unknown activity task {task_id}') from exc
        execute_activity(task)
