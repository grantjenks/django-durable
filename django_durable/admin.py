from django import forms
from django.contrib import admin
from django.contrib.admin.helpers import ACTION_CHECKBOX_NAME
from django.shortcuts import redirect
from django.template.response import TemplateResponse
from django.urls import reverse

from .api import cancel_workflow, signal_workflow, start_workflow
from .models import ActivityTask, HistoryEvent, WorkflowExecution


class StartWorkflowForm(forms.Form):
    workflow_name = forms.CharField(label="Workflow name")
    params = forms.JSONField(
        required=False, label="Parameters", help_text="JSON payload for workflow input"
    )


class SignalWorkflowForm(forms.Form):
    signal_name = forms.CharField(label="Signal name")
    payload = forms.JSONField(
        required=False, label="Payload", help_text="JSON payload for the signal"
    )


class HistoryEventInline(admin.TabularInline):
    model = HistoryEvent
    extra = 0
    fields = ('type', 'pos', 'created_at', 'details')
    readonly_fields = ('created_at',)
    ordering = ('id',)
    show_change_link = True


class ActivityTaskInline(admin.TabularInline):
    model = ActivityTask
    extra = 0
    fields = (
        'id',
        'activity_name',
        'status',
        'pos',
        'after_time',
        'attempt',
        'max_attempts',
        'started_at',
        'finished_at',
    )
    readonly_fields = ('id', 'started_at', 'finished_at')
    ordering = ('-updated_at',)
    show_change_link = True


@admin.register(WorkflowExecution)
class WorkflowExecutionAdmin(admin.ModelAdmin):
    list_display = (
        'id',
        'workflow_name',
        'status',
        'started_at',
        'finished_at',
        'updated_at',
    )
    list_filter = ('status', 'workflow_name')
    search_fields = ('id', 'workflow_name')
    date_hierarchy = 'started_at'
    readonly_fields = ('started_at', 'finished_at', 'updated_at')
    fields = (
        'workflow_name',
        'status',
        'input',
        'result',
        'error',
        'started_at',
        'finished_at',
        'updated_at',
    )
    inlines = [ActivityTaskInline, HistoryEventInline]

    actions = ["cancel_workflows", "signal_workflows"]

    def add_view(self, request, form_url="", extra_context=None):
        if request.method == "POST":
            form = StartWorkflowForm(request.POST)
            if form.is_valid():
                name = form.cleaned_data["workflow_name"]
                params = form.cleaned_data.get("params") or {}
                exec_id = start_workflow(name, **params)
                self.message_user(request, f"Started workflow {exec_id}")
                url = reverse("admin:django_durable_workflowexecution_change", args=[exec_id])
                return redirect(url)
        else:
            form = StartWorkflowForm()
        context = {
            **self.admin_site.each_context(request),
            "form": form,
            "opts": self.model._meta,
            "title": "Start workflow",
        }
        return TemplateResponse(request, "admin/django_durable/start_workflow.html", context)

    @admin.action(description="Cancel selected workflows")
    def cancel_workflows(self, request, queryset):
        for execution in queryset:
            cancel_workflow(execution)
        self.message_user(request, f"Canceled {queryset.count()} workflow(s)")

    @admin.action(description="Signal selected workflows")
    def signal_workflows(self, request, queryset):
        form = SignalWorkflowForm(request.POST or None)
        if "apply" in request.POST:
            if form.is_valid():
                name = form.cleaned_data["signal_name"]
                payload = form.cleaned_data.get("payload")
                for execution in queryset:
                    signal_workflow(execution, name, payload)
                self.message_user(
                    request, f"Sent signal '{name}' to {queryset.count()} workflow(s)"
                )
                return None
        context = {
            **self.admin_site.each_context(request),
            "opts": self.model._meta,
            "form": form,
            "queryset": queryset,
            "action": "signal_workflows",
            "title": "Signal workflows",
            "action_checkbox_name": ACTION_CHECKBOX_NAME,
        }
        return TemplateResponse(
            request, "admin/django_durable/signal_workflow.html", context
        )


@admin.register(ActivityTask)
class ActivityTaskAdmin(admin.ModelAdmin):
    list_display = (
        'id',
        'execution',
        'activity_name',
        'status',
        'pos',
        'after_time',
        'attempt',
        'started_at',
        'finished_at',
        'updated_at',
    )
    list_filter = ('status', 'activity_name')
    search_fields = ('id', 'execution__id', 'activity_name')
    date_hierarchy = 'after_time'
    readonly_fields = ('started_at', 'finished_at', 'updated_at')


@admin.register(HistoryEvent)
class HistoryEventAdmin(admin.ModelAdmin):
    list_display = ('id', 'execution', 'type', 'pos', 'created_at')
    list_filter = ('type',)
    search_fields = ('id', 'execution__id', 'type')
    date_hierarchy = 'created_at'
    ordering = ('-id',)
