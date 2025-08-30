from django.apps import AppConfig
from django.utils.module_loading import autodiscover_modules


class DjangoDurableConfig(AppConfig):
    name = 'django_durable'
    verbose_name = 'Django Durable'

    def ready(self):
        # Auto-discover user-defined workflows/activities
        autodiscover_modules('durable_workflows')
        autodiscover_modules('durable_activities')
