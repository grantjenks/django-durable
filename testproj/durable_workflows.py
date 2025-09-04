import time

from django_durable import register

@register.workflow()
def onboard_user(ctx, user_id: int):
    # 1) send email (schedules ActivityTask, then pauses; worker resumes deterministically)
    res = ctx.run_activity("testproj.send_welcome_email", user_id)
    # 2) wait 1 hour without blocking a worker thread
    ctx.sleep(3600)
    # 3) check confirmation
    clicked = ctx.run_activity("testproj.confirm_clicked", user_id)
    if not clicked["clicked"]:
        # try again in a day
        ctx.sleep(24 * 3600)
        ctx.run_activity("testproj.send_welcome_email", user_id)

    # 4) compute score and finish
    score = ctx.run_activity("testproj.compute_score", user_id)
    return {"ok": True, "score": score["score"]}


@register.workflow()
def e2e_flow(ctx, value):
    # activity
    res = ctx.run_activity("testproj.echo", value)
    # immediate timer
    ctx.sleep(0)
    # wait for external signal 'go'
    sig = ctx.wait_signal("go")
    return {"res": res["value"], "sig": sig}

@register.workflow()
def complex_flow(ctx, value):
    # chain multiple activities with timers and a signal
    first = ctx.run_activity("testproj.add", value, 5)
    ctx.sleep(0)
    second = ctx.run_activity("testproj.multiply", first["value"], 2)
    ctx.sleep(0)
    sig = ctx.wait_signal("finish")
    ctx.sleep(0)
    final = ctx.run_activity("testproj.add", second["value"], sig["add"])
    return {"result": final["value"], "sig": sig}


@register.workflow()
def sleep_work_loop(ctx, loops: int, sleep: float):
    """Workflow that alternates between sleeping and doing trivial work."""
    for i in range(loops):
        ctx.sleep(sleep)
        ctx.run_activity("testproj.do_work", i)
    return {"done": loops}


@register.workflow()
def activity_timeout_flow(ctx):
    ctx.run_activity("testproj.echo", "hi", schedule_to_close_timeout=0)


@register.workflow()
def retry_flow(ctx, key: str, fail_times: int):
    res = ctx.run_activity("testproj.flaky", key, fail_times)
    return {"attempts": res["attempts"]}


@register.workflow()
def retry_linear_flow(ctx, key: str, fail_times: int):
    res = ctx.run_activity("testproj.flaky_linear", key, fail_times)
    return {"attempts": res["attempts"]}


@register.workflow()
def heartbeat_flow(ctx):
    res = ctx.run_activity("testproj.heartbeat_activity")
    return res


@register.workflow()
def heartbeat_timeout_flow(ctx):
    ctx.run_activity("testproj.no_heartbeat_activity")


@register.workflow()
def add_flow(ctx, a: int, b: int):
    """Simple workflow used for benchmarks."""
    res = ctx.run_activity("testproj.add", a, b)
    return {"value": res["value"]}


@register.workflow()
def child_increment_workflow(ctx, x: int):
    res = ctx.run_activity("testproj.add", x, 1)
    return {"y": res["value"]}


@register.workflow()
def parent_child_workflow(ctx, x: int):
    child = ctx.run_workflow("testproj.child_increment_workflow", x=x)
    return {"child": child}

@register.workflow()
def long_running_step_flow(ctx, loops: int, delay: float):
    """Workflow with long-running steps to test recovery when worker dies mid-execution."""
    for i in range(loops):
        time.sleep(delay)
        ctx.run_activity("testproj.do_work", i)
    return {"done": loops}


@register.workflow()
def long_activity_flow(ctx, loops: int, delay: float):
    """Workflow with slow activities to test recovery when worker dies during an activity."""
    for _ in range(loops):
        ctx.run_activity("testproj.slow_sleep", delay)
    return {"done": loops}


@register.workflow()
def grandchild_sleep_workflow(ctx):
    """Workflow that simply sleeps so it can be canceled later."""
    ctx.sleep(3600)


@register.workflow()
def child_child_workflow(ctx):
    """Child workflow that starts a grandchild workflow and sleeps."""
    ctx.start_workflow("testproj.grandchild_sleep_workflow")
    ctx.sleep(3600)


@register.workflow()
def parent_cascade_workflow(ctx):
    """Parent workflow that starts a child workflow and sleeps."""
    ctx.start_workflow("testproj.child_child_workflow")
    ctx.sleep(3600)
