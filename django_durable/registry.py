import sys
from collections.abc import Callable
from pathlib import Path

from django.apps import apps

from .retry import RetryPolicy


class Register:
    def __init__(self):
        self.workflows: dict[str, Callable] = {}
        self.activities: dict[str, Callable] = {}

    def _durable_name(self, fn: Callable) -> str:
        module = sys.modules.get(fn.__module__)
        file = getattr(module, "__file__", None)
        app_name = None
        if file:
            mod_path = Path(file).resolve()
            for cfg in apps.get_app_configs():
                if mod_path.is_relative_to(Path(cfg.path).resolve()):
                    app_name = cfg.label
                    break
        if app_name is None:
            app_name = fn.__module__.split('.')[0]
        return f"{app_name}.{fn.__name__}"

    def workflow(self, timeout: float | None = None):
        def deco(fn):
            if timeout is not None:
                fn._durable_timeout = timeout
            name = self._durable_name(fn)
            fn._durable_name = name
            self.workflows[name] = fn
            return fn

        return deco

    def activity(
        self,
        timeout: float | None = None,
        heartbeat_timeout: float | None = None,
        retry_policy: RetryPolicy | None = None,
    ):
        def deco(fn):
            rp = retry_policy or RetryPolicy()
            fn._durable_retry_policy = rp
            if timeout is not None:
                fn._durable_timeout = timeout
            if heartbeat_timeout is not None:
                fn._durable_heartbeat_timeout = heartbeat_timeout
            name = self._durable_name(fn)
            fn._durable_name = name
            self.activities[name] = fn
            return fn

        return deco


register = Register()
