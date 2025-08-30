from django.contrib import admin

from .models import WorkflowExecution, HistoryEvent, ActivityTask


class HistoryEventInline(admin.TabularInline):
    model = HistoryEvent
    extra = 0
    fields = ("type", "pos", "created_at", "details")
    readonly_fields = ("created_at",)
    ordering = ("id",)
    show_change_link = True


class ActivityTaskInline(admin.TabularInline):
    model = ActivityTask
    extra = 0
    fields = (
        "id",
        "activity_name",
        "status",
        "pos",
        "after_time",
        "attempt",
        "max_attempts",
        "started_at",
        "finished_at",
    )
    readonly_fields = ("id", "started_at", "finished_at")
    ordering = ("-updated_at",)
    show_change_link = True


@admin.register(WorkflowExecution)
class WorkflowExecutionAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "workflow_name",
        "status",
        "started_at",
        "finished_at",
        "updated_at",
    )
    list_filter = ("status", "workflow_name")
    search_fields = ("id", "workflow_name")
    date_hierarchy = "started_at"
    readonly_fields = ("started_at", "finished_at", "updated_at")
    fields = (
        "workflow_name",
        "status",
        "input",
        "result",
        "error",
        "started_at",
        "finished_at",
        "updated_at",
    )
    inlines = [ActivityTaskInline, HistoryEventInline]


@admin.register(ActivityTask)
class ActivityTaskAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "execution",
        "activity_name",
        "status",
        "pos",
        "after_time",
        "attempt",
        "started_at",
        "finished_at",
        "updated_at",
    )
    list_filter = ("status", "activity_name")
    search_fields = ("id", "execution__id", "activity_name")
    date_hierarchy = "after_time"
    readonly_fields = ("started_at", "finished_at", "updated_at")


@admin.register(HistoryEvent)
class HistoryEventAdmin(admin.ModelAdmin):
    list_display = ("id", "execution", "type", "pos", "created_at")
    list_filter = ("type",)
    search_fields = ("id", "execution__id", "type")
    date_hierarchy = "created_at"
    ordering = ("-id",)

