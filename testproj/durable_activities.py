from django_durable.registry import register, RetryPolicy
from django_durable.engine import activity_heartbeat
from time import sleep

@register.activity(max_retries=3)
def send_welcome_email(user_id: int):
    # do side-effect; return serializable result
    # (pretend)
    return {"status": "sent", "user_id": user_id}

@register.activity()
def confirm_clicked(user_id: int):
    # pretend we looked up a flag somewhere
    return {"clicked": True}

# Internal example: long compute (avoid real sleeps; use workflow ctx.sleep instead)
@register.activity()
def compute_score(user_id: int):
    # pure CPU or short IO; return JSON-serializable data
    return {"score": 42}


# E2E Test helpers
@register.activity()
def echo(value):
    return {"value": value}


@register.activity()
def add(a, b):
    return {"value": a + b}


@register.activity()
def multiply(a, b):
    return {"value": a * b}


@register.activity()
def do_work(i):
    """Simple activity used for concurrency tests."""
    return {"i": i}


@register.activity(
    retry_policy=RetryPolicy(
        initial_interval=0.1,
        backoff_coefficient=2.0,
        maximum_interval=1.0,
        maximum_attempts=3,
    )
)
def flaky(key, fail_times):
    """Activity that fails a given number of times before succeeding."""
    from django_durable.engine import _current_activity
    from django_durable.models import ActivityTask

    task = ActivityTask.objects.get(id=_current_activity.task_id)
    if task.attempt <= fail_times:
        raise ValueError("boom")
    return {"attempts": task.attempt}


@register.activity(
    retry_policy=RetryPolicy(
        initial_interval=0.1,
        strategy='linear',
        maximum_interval=1.0,
        maximum_attempts=3,
    )
)
def flaky_linear(key, fail_times):
    """Like ``flaky`` but with linear backoff."""
    from django_durable.engine import _current_activity
    from django_durable.models import ActivityTask

    task = ActivityTask.objects.get(id=_current_activity.task_id)
    if task.attempt <= fail_times:
        raise ValueError("boom")
    return {"attempts": task.attempt}


@register.activity(heartbeat_timeout=0.1)
def heartbeat_activity():
    activity_heartbeat({"beat": 1})
    sleep(0.05)
    activity_heartbeat({"beat": 2})
    sleep(0.05)
    return {"ok": True}


@register.activity(heartbeat_timeout=0.1, max_retries=1)
def no_heartbeat_activity(delay=0.2):
    sleep(delay)
    return {"ok": True}

@register.activity(
    timeout=1.0,
    retry_policy=RetryPolicy(
        initial_interval=0.1,
        backoff_coefficient=2.0,
        maximum_interval=1.0,
        maximum_attempts=0,
    ),
)
def slow_sleep(delay=0.2):
    """Activity that sleeps for a bit to simulate long work."""
    sleep(delay)
    return {"slept": delay}
