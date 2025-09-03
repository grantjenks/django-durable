__all__ = [
    "start_workflow",
    "wait_workflow",
    "run_workflow",
    "signal_workflow",
    "cancel_workflow",
    "register",
]


def __getattr__(name):
    if name in __all__:
        from . import api as _api

        return getattr(_api, name)
    raise AttributeError(name)


default_app_config = 'django_durable.apps.DjangoDurableConfig'
