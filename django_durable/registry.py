import sys
from pathlib import Path
from typing import Callable, Dict, Optional

from django.apps import apps

from .retry import RetryPolicy


class Register:
    def __init__(self):
        self.workflows: Dict[str, Callable] = {}
        self.activities: Dict[str, Callable] = {}

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

    def workflow(self, timeout: Optional[float] = None):
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
        timeout: Optional[float] = None,
        heartbeat_timeout: Optional[float] = None,
        retry_policy: Optional[RetryPolicy] = None,
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
