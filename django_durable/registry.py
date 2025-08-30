from typing import Callable, Dict


class Register:
    def __init__(self):
        self.workflows: Dict[str, Callable] = {}
        self.activities: Dict[str, Callable] = {}

    def workflow(self, name=None):
        def deco(fn):
            self.workflows[name or fn.__name__] = fn
            return fn

        return deco

    def activity(self, name=None, max_retries=0):
        def deco(fn):
            fn._durable_max_retries = max_retries
            self.activities[name or fn.__name__] = fn
            return fn

        return deco


register = Register()
