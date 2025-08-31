from dataclasses import dataclass, field, asdict
from typing import Callable, Dict, List, Optional


class Register:
    def __init__(self):
        self.workflows: Dict[str, Callable] = {}
        self.activities: Dict[str, Callable] = {}
        self.queries: Dict[str, Dict[str, Callable]] = {}

    def workflow(self, name: Optional[str] = None, timeout: Optional[float] = None):
        def deco(fn):
            if timeout is not None:
                fn._durable_timeout = timeout
            self.workflows[name or fn.__name__] = fn
            return fn

        return deco

    def query(self, workflow_name: str, name: Optional[str] = None):
        """Register a read-only query handler for a workflow."""

        def deco(fn: Callable):
            wf_queries = self.queries.setdefault(workflow_name, {})
            wf_queries[name or fn.__name__] = fn
            return fn

        return deco

    def activity(
        self,
        name: Optional[str] = None,
        max_retries: int = 0,
        timeout: Optional[float] = None,
        heartbeat_timeout: Optional[float] = None,
        retry_policy: Optional["RetryPolicy"] = None,
    ):
        def deco(fn):
            if retry_policy is None:
                rp = RetryPolicy(maximum_attempts=max_retries)
            else:
                rp = retry_policy
            fn._durable_retry_policy = rp
            if timeout is not None:
                fn._durable_timeout = timeout
            if heartbeat_timeout is not None:
                fn._durable_heartbeat_timeout = heartbeat_timeout
            self.activities[name or fn.__name__] = fn
            return fn

        return deco


register = Register()


@dataclass
class RetryPolicy:
    initial_interval: float = 1.0
    backoff_coefficient: float = 2.0
    maximum_interval: float = 60.0
    maximum_attempts: int = 0  # 0 for unlimited
    non_retryable_error_types: List[str] = field(default_factory=list)

    def asdict(self) -> Dict:
        return asdict(self)
