from django_durable.registry import register

@register.workflow()
def onboard_user(ctx, user_id: int):
    # 1) send email (schedules ActivityTask, then pauses; worker resumes deterministically)
    res = ctx.activity("send_welcome_email", user_id)
    # 2) wait 1 hour without blocking a worker thread
    ctx.sleep(3600)
    # 3) check confirmation
    clicked = ctx.activity("confirm_clicked", user_id)
    if not clicked["clicked"]:
        # try again in a day
        ctx.sleep(24 * 3600)
        ctx.activity("send_welcome_email", user_id)

    # 4) compute score and finish
    score = ctx.activity("compute_score", user_id)
    return {"ok": True, "score": score["score"]}


@register.workflow()
def e2e_flow(ctx, value):
    # activity
    res = ctx.activity("echo", value)
    # immediate timer
    ctx.sleep(0)
    # wait for external signal 'go'
    sig = ctx.wait_signal("go")
    return {"res": res["value"], "sig": sig}


@register.workflow()
def complex_flow(ctx, value):
    # chain multiple activities with timers and a signal
    first = ctx.activity("add", value, 5)
    ctx.sleep(0)
    second = ctx.activity("multiply", first["value"], 2)
    ctx.sleep(0)
    sig = ctx.wait_signal("finish")
    ctx.sleep(0)
    final = ctx.activity("add", second["value"], sig["add"])
    return {"result": final["value"], "sig": sig}
