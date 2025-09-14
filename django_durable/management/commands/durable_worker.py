import json
import select
import socket
import subprocess
import sys
import time
from datetime import timedelta
from datetime import timedelta as _td

from django.core.management.base import BaseCommand, CommandError
from django.db import DatabaseError, IntegrityError, close_old_connections
from django.utils import timezone

from django_durable.constants import SPECIAL_EVENT_POS, ErrorCode, HistoryEventType
from django_durable.engine import _notify_parent, execute_activity, step_workflow
from django_durable.models import ActivityTask, HistoryEvent, WorkflowExecution
from django_durable.retry import compute_backoff


class Command(BaseCommand):
    help = 'Run the django-durable worker (workflows + activities).'

    def add_arguments(self, parser):
        parser.add_argument(
            '--tick', type=float, default=0.5, help='Poll interval in seconds.'
        )
        parser.add_argument('--batch', type=int, default=10, help='Max tasks per tick.')
        parser.add_argument(
            '--iterations',
            type=int,
            default=None,
            help='Optional number of loop iterations to run (for testing).',
        )
        parser.add_argument(
            '--procs',
            type=int,
            default=4,
            help='Max subprocesses to manage concurrently.',
        )
        parser.add_argument(
            '--dispatch-mode',
            choices=['parent', 'follower'],
            default='parent',
            help='Internal: run as parent dispatcher or follower worker.',
        )
        parser.add_argument(
            '--max-follower-tasks',
            type=int,
            default=100,
            help='Exit follower after processing this many tasks.',
        )

    def _timeout_activity(self, task):
        task.error = ErrorCode.ACTIVITY_TIMEOUT.value
        policy = task.retry_policy or {}
        max_attempts = policy.get('maximum_attempts', 0)
        curr_attempt = task.attempt or 1
        should_retry = max_attempts == 0 or curr_attempt < max_attempts
        if should_retry:
            interval = compute_backoff(policy, curr_attempt)
            task.status = ActivityTask.Status.QUEUED
            task.after_time = timezone.now() + _td(seconds=interval)
            task.save(update_fields=['status', 'error', 'after_time', 'updated_at'])
        else:
            task.status = ActivityTask.Status.TIMED_OUT
            task.finished_at = timezone.now()
            task.save(update_fields=['status', 'error', 'finished_at', 'updated_at'])
            HistoryEvent.objects.create(
                execution=task.execution,
                type=HistoryEventType.ACTIVITY_TIMED_OUT.value,
                pos=task.pos,
                details={'error': ErrorCode.ACTIVITY_TIMEOUT.value},
            )
            WorkflowExecution.objects.filter(
                pk=task.execution_id,
                status__in=[
                    WorkflowExecution.Status.PENDING,
                    WorkflowExecution.Status.RUNNING,
                ],
            ).update(status=WorkflowExecution.Status.PENDING)

    def _timeout_workflow(self, wf):
        now = timezone.now()
        HistoryEvent.objects.create(
            execution=wf,
            type=HistoryEventType.WORKFLOW_TIMED_OUT.value,
            pos=SPECIAL_EVENT_POS,
            details={'error': ErrorCode.WORKFLOW_TIMEOUT.value},
        )
        wf.status = WorkflowExecution.Status.TIMED_OUT
        wf.error = ErrorCode.WORKFLOW_TIMEOUT.value
        wf.finished_at = now
        wf.save(update_fields=['status', 'error', 'finished_at', 'updated_at'])
        qs = ActivityTask.objects.select_for_update().filter(
            execution=wf, status=ActivityTask.Status.QUEUED
        )
        for t in qs:
            t.status = ActivityTask.Status.FAILED
            t.error = ErrorCode.WORKFLOW_TIMEOUT.value
            t.finished_at = now
            t.save(
                update_fields=[
                    'status',
                    'error',
                    'finished_at',
                    'updated_at',
                ]
            )
            HistoryEvent.objects.create(
                execution=wf,
                type=HistoryEventType.ACTIVITY_FAILED.value,
                pos=t.pos,
                details={'error': ErrorCode.WORKFLOW_TIMEOUT.value},
            )
        _notify_parent(
            wf,
            HistoryEventType.CHILD_WORKFLOW_TIMED_OUT.value,
            {'error': ErrorCode.WORKFLOW_TIMEOUT.value},
        )

    def _cancel_activity(self, task):
        now = timezone.now()
        task.status = ActivityTask.Status.FAILED
        task.error = ErrorCode.WORKFLOW_CANCELED.value
        task.finished_at = now
        task.save(update_fields=['status', 'error', 'finished_at', 'updated_at'])
        HistoryEvent.objects.create(
            execution=task.execution,
            type=HistoryEventType.ACTIVITY_CANCELED.value,
            pos=task.pos,
            details={'error': ErrorCode.WORKFLOW_CANCELED.value},
        )
        WorkflowExecution.objects.filter(
            pk=task.execution_id,
            status__in=[
                WorkflowExecution.Status.PENDING,
                WorkflowExecution.Status.RUNNING,
            ],
        ).update(status=WorkflowExecution.Status.PENDING)

    def _spawn_follower_proc(self, max_tasks):
        cmd = [
            sys.executable,
            sys.argv[0],
            'durable_worker',
            '--dispatch-mode',
            'follower',
            '--max-follower-tasks',
            str(max_tasks),
        ]
        close_old_connections()
        proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=sys.stderr,
            text=True,
        )
        close_old_connections()
        return proc

    def _run_follower(self, max_tasks: int):
        """Run follower mode: execute tasks from stdin and ack on stdout."""
        close_old_connections()
        processed = 0
        try:
            for line in sys.stdin:
                line = line.strip()
                if not line:
                    continue
                msg = json.loads(line)
                cmd = msg.get('cmd')
                if cmd == 'activity':
                    task = ActivityTask.objects.get(id=msg['id'])
                    execute_activity(task)
                elif cmd == 'workflow':
                    wf = WorkflowExecution.objects.get(id=msg['id'])
                    step_workflow(wf)
                elif cmd == 'exit':
                    break
                sys.stdout.write(json.dumps({'ok': True}) + '\n')
                sys.stdout.flush()
                processed += 1
                if max_tasks and processed >= max_tasks:
                    break
        finally:
            close_old_connections()

    def _run_worker_loop(self, tick, batch, iterations, procs, max_tasks):
        close_old_connections()
        try:
            loops = 0
            idle = [self._spawn_follower_proc(max_tasks) for _ in range(procs)]
            running = []
            while True:
                now = timezone.now()
                progressed = False

                for proc in list(idle):
                    if proc.poll() is not None:
                        idle.remove(proc)
                        idle.append(self._spawn_follower_proc(max_tasks))
                        progressed = True

                if running:
                    rlist = [info['proc'].stdout for info in running]
                    ready, _, _ = select.select(rlist, [], [], 0)
                    for r in ready:
                        for info in list(running):
                            if info['proc'].stdout is r:
                                r.readline()
                                running.remove(info)
                                idle.append(info['proc'])
                                progressed = True
                                break

                for info in list(running):
                    proc = info['proc']
                    if proc.poll() is not None:
                        running.remove(info)
                        idle.append(self._spawn_follower_proc(max_tasks))
                        progressed = True
                        continue
                    deadline = info.get('deadline')
                    if deadline is not None and now >= deadline:
                        proc.kill()
                        proc.wait()
                        if info['type'] == 'activity':
                            try:
                                task = ActivityTask.objects.get(id=info['id'])
                                self._timeout_activity(task)
                            except ActivityTask.DoesNotExist:
                                pass
                        else:
                            try:
                                wf = WorkflowExecution.objects.get(id=info['id'])
                                self._timeout_workflow(wf)
                            except WorkflowExecution.DoesNotExist:
                                pass
                        running.remove(info)
                        idle.append(self._spawn_follower_proc(max_tasks))
                        progressed = True
                        continue

                    if info['type'] == 'activity':
                        try:
                            task = ActivityTask.objects.select_related('execution').get(id=info['id'])
                        except ActivityTask.DoesNotExist:
                            running.remove(info)
                            idle.append(self._spawn_follower_proc(max_tasks))
                            progressed = True
                            continue
                        if task.execution.status == WorkflowExecution.Status.CANCELED:
                            proc.kill()
                            proc.wait()
                            self._cancel_activity(task)
                            running.remove(info)
                            idle.append(self._spawn_follower_proc(max_tasks))
                            progressed = True
                    else:
                        try:
                            wf = WorkflowExecution.objects.select_related('parent').get(id=info['id'])
                        except WorkflowExecution.DoesNotExist:
                            running.remove(info)
                            idle.append(self._spawn_follower_proc(max_tasks))
                            progressed = True
                            continue
                        parent_canceled = (
                            wf.parent_id
                            and WorkflowExecution.objects.filter(
                                id=wf.parent_id,
                                status=WorkflowExecution.Status.CANCELED,
                            ).exists()
                        )
                        if wf.status == WorkflowExecution.Status.CANCELED or parent_canceled:
                            proc.kill()
                            proc.wait()
                            running.remove(info)
                            idle.append(self._spawn_follower_proc(max_tasks))
                            progressed = True

                # 0) Timeout queued activities
                timed_ids = list(
                    ActivityTask.objects.filter(
                        status=ActivityTask.Status.QUEUED,
                        expires_at__isnull=False,
                        expires_at__lte=now,
                    ).values_list('id', flat=True)[:batch]
                )
                if timed_ids:
                    progressed = True
                for tid in timed_ids:
                    try:
                        task = ActivityTask.objects.get(id=tid)
                    except ActivityTask.DoesNotExist:
                        continue
                    task.error = ErrorCode.ACTIVITY_TIMEOUT.value
                    policy = task.retry_policy or {}
                    max_attempts = policy.get('maximum_attempts', 0)
                    curr_attempt = task.attempt or 0
                    should_retry = curr_attempt > 0 and (
                        max_attempts == 0 or curr_attempt < max_attempts
                    )
                    if should_retry:
                        interval = compute_backoff(policy, curr_attempt + 1)
                        task.after_time = timezone.now() + _td(seconds=interval)
                        task.save(update_fields=['error', 'after_time', 'updated_at'])
                    else:
                        task.status = ActivityTask.Status.TIMED_OUT
                        task.finished_at = now
                        task.save(
                            update_fields=[
                                'status',
                                'error',
                                'finished_at',
                                'updated_at',
                            ]
                        )
                        HistoryEvent.objects.create(
                            execution=task.execution,
                            type=HistoryEventType.ACTIVITY_TIMED_OUT.value,
                            pos=task.pos,
                            details={'error': ErrorCode.ACTIVITY_TIMEOUT.value},
                        )
                        WorkflowExecution.objects.filter(
                            pk=task.execution_id,
                            status__in=[
                                WorkflowExecution.Status.PENDING,
                                WorkflowExecution.Status.RUNNING,
                            ],
                        ).update(status=WorkflowExecution.Status.PENDING)

                # 0b) Timeout workflows
                wf_timeouts = list(
                    WorkflowExecution.objects.filter(
                        status__in=[
                            WorkflowExecution.Status.PENDING,
                            WorkflowExecution.Status.RUNNING,
                        ],
                        expires_at__isnull=False,
                        expires_at__lte=now,
                    ).values_list('id', flat=True)[:batch]
                )
                if wf_timeouts:
                    progressed = True
                for wid in wf_timeouts:
                    try:
                        wf = WorkflowExecution.objects.get(id=wid)
                    except WorkflowExecution.DoesNotExist:
                        continue
                    self._timeout_workflow(wf)

                # 0c) Heartbeat timeouts for running activities
                hb_ids = list(
                    ActivityTask.objects.filter(
                        status=ActivityTask.Status.RUNNING,
                        heartbeat_timeout__isnull=False,
                    ).values_list('id', flat=True)[:batch]
                )
                if hb_ids:
                    progressed = True
                for tid in hb_ids:
                    try:
                        task = ActivityTask.objects.get(id=tid)
                    except ActivityTask.DoesNotExist:
                        continue
                    hb_at = task.heartbeat_at or task.started_at or now
                    if (
                        task.heartbeat_timeout is not None
                        and hb_at + timedelta(seconds=float(task.heartbeat_timeout))
                        <= now
                    ):
                        task.error = ErrorCode.HEARTBEAT_TIMEOUT.value
                        policy = task.retry_policy or {}
                        max_attempts = policy.get('maximum_attempts', 0)
                        curr_attempt = task.attempt or 1
                        should_retry = max_attempts == 0 or curr_attempt < max_attempts
                        if should_retry:
                            interval = compute_backoff(policy, curr_attempt)
                            task.status = ActivityTask.Status.QUEUED
                            task.after_time = timezone.now() + _td(seconds=interval)
                            task.save(
                                update_fields=[
                                    'status',
                                    'error',
                                    'after_time',
                                    'updated_at',
                                ]
                            )
                        else:
                            task.status = ActivityTask.Status.TIMED_OUT
                            task.finished_at = now
                            task.save(
                                update_fields=[
                                    'status',
                                    'error',
                                    'finished_at',
                                    'updated_at',
                                ]
                            )
                            try:
                                HistoryEvent.objects.create(
                                    execution=task.execution,
                                    type=HistoryEventType.ACTIVITY_TIMED_OUT.value,
                                    pos=task.pos,
                                    details={
                                        'error': ErrorCode.HEARTBEAT_TIMEOUT.value
                                    },
                                )
                            except IntegrityError:
                                # Event already recorded by another worker iteration
                                pass
                            wf = task.execution
                            HistoryEvent.objects.create(
                                execution=wf,
                                type=HistoryEventType.WORKFLOW_FAILED.value,
                                pos=SPECIAL_EVENT_POS,
                                details={'error': ErrorCode.HEARTBEAT_TIMEOUT.value},
                            )
                            wf.status = WorkflowExecution.Status.FAILED
                            wf.error = ErrorCode.HEARTBEAT_TIMEOUT.value
                            wf.finished_at = now
                            wf.save(
                                update_fields=['status', 'error', 'finished_at', 'updated_at']
                            )
                            _notify_parent(
                                wf,
                                HistoryEventType.CHILD_WORKFLOW_FAILED.value,
                                {
                                    'error': ErrorCode.HEARTBEAT_TIMEOUT.value
                                },
                            )

                # 0d) Schedule-to-close timeouts for running activities
                sc_ids = list(
                    ActivityTask.objects.filter(
                        status=ActivityTask.Status.RUNNING,
                        expires_at__isnull=False,
                        expires_at__lte=now,
                    ).values_list('id', flat=True)[:batch]
                )
                if sc_ids:
                    progressed = True
                for tid in sc_ids:
                    try:
                        task = ActivityTask.objects.get(id=tid)
                    except ActivityTask.DoesNotExist:
                        continue
                    task.error = ErrorCode.ACTIVITY_TIMEOUT.value
                    policy = task.retry_policy or {}
                    max_attempts = policy.get('maximum_attempts', 0)
                    curr_attempt = task.attempt or 1
                    should_retry = max_attempts == 0 or curr_attempt < max_attempts
                    if should_retry:
                        interval = compute_backoff(policy, curr_attempt)
                        task.status = ActivityTask.Status.QUEUED
                        task.after_time = timezone.now() + _td(seconds=interval)
                        task.save(
                            update_fields=[
                                'status',
                                'error',
                                'after_time',
                                'updated_at',
                            ]
                        )
                    else:
                        task.status = ActivityTask.Status.TIMED_OUT
                        task.finished_at = now
                        task.save(
                            update_fields=[
                                'status',
                                'error',
                                'finished_at',
                                'updated_at',
                            ]
                        )
                        HistoryEvent.objects.create(
                            execution=task.execution,
                            type=HistoryEventType.ACTIVITY_TIMED_OUT.value,
                            pos=task.pos,
                            details={'error': ErrorCode.ACTIVITY_TIMEOUT.value},
                        )
                        WorkflowExecution.objects.filter(
                            pk=task.execution_id,
                            status__in=[
                                WorkflowExecution.Status.PENDING,
                                WorkflowExecution.Status.RUNNING,
                            ],
                        ).update(status=WorkflowExecution.Status.PENDING)

                # 1) Run due activities
                slots = len(idle)
                if slots > 0:
                    due_ids = list(
                        ActivityTask.objects.filter(
                            status=ActivityTask.Status.QUEUED,
                            after_time__lte=now,
                            execution__status__in=[
                                WorkflowExecution.Status.PENDING,
                                WorkflowExecution.Status.RUNNING,
                            ],
                        )
                        .order_by('updated_at')
                        .values_list('id', flat=True)[: min(batch, slots)]
                    )
                else:
                    due_ids = []
                if due_ids:
                    progressed = True
                for tid in due_ids:
                    if not idle:
                        break
                    proc = idle.pop(0)
                    claimed = ActivityTask.objects.filter(
                        id=tid,
                        status=ActivityTask.Status.QUEUED,
                        after_time__lte=now,
                    ).update(status=ActivityTask.Status.RUNNING)
                    if not claimed:
                        idle.append(proc)
                        continue
                    try:
                        task = ActivityTask.objects.get(id=tid)
                    except DatabaseError:
                        ActivityTask.objects.filter(id=tid).update(
                            status=ActivityTask.Status.QUEUED
                        )
                        idle.append(proc)
                        continue
                    timeout = None
                    if task.expires_at is not None:
                        timeout = max(
                            0.0,
                            (task.expires_at - timezone.now()).total_seconds(),
                        )
                    msg = json.dumps({'cmd': 'activity', 'id': tid}) + '\n'
                    try:
                        proc.stdin.write(msg)
                        proc.stdin.flush()
                    except Exception:
                        ActivityTask.objects.filter(id=tid).update(
                            status=ActivityTask.Status.QUEUED
                        )
                        idle.append(self._spawn_follower_proc(max_tasks))
                        continue
                    deadline = (
                        timezone.now() + _td(seconds=timeout)
                        if timeout is not None
                        else None
                    )
                    running.append(
                        {
                            'type': 'activity',
                            'id': tid,
                            'proc': proc,
                            'deadline': deadline,
                        }
                    )

                # 2) Advance workflows
                if idle:
                    runnable_ids = list(
                        WorkflowExecution.objects.filter(
                            status=WorkflowExecution.Status.PENDING
                        )
                        .order_by('updated_at')
                        .values_list('id', flat=True)[: min(batch, len(idle))]
                    )
                else:
                    runnable_ids = []
                if runnable_ids:
                    progressed = True
                for wid in runnable_ids:
                    if not idle:
                        break
                    proc = idle.pop(0)
                    try:
                        wf = WorkflowExecution.objects.get(id=wid)
                    except DatabaseError:
                        idle.append(proc)
                        continue
                    timeout = None
                    if wf.expires_at is not None:
                        timeout = max(
                            0.0,
                            (wf.expires_at - timezone.now()).total_seconds(),
                        )
                    msg = json.dumps({'cmd': 'workflow', 'id': wid}) + '\n'
                    try:
                        proc.stdin.write(msg)
                        proc.stdin.flush()
                    except Exception:
                        idle.append(self._spawn_follower_proc(max_tasks))
                        continue
                    deadline = (
                        timezone.now() + _td(seconds=timeout)
                        if timeout is not None
                        else None
                    )
                    running.append(
                        {
                            'type': 'workflow',
                            'id': wid,
                            'proc': proc,
                            'deadline': deadline,
                        }
                    )

                loops += 1
                if iterations is not None and loops >= iterations and not running:
                    break
                if not progressed:
                    time.sleep(tick)
        finally:
            close_old_connections()

    def handle(self, *args, **opts):
        mode = opts['dispatch_mode']
        if mode == 'follower':
            self._run_follower(opts['max_follower_tasks'])
            return
        tick = opts['tick']
        batch = opts['batch']
        iterations = opts['iterations']
        procs = opts['procs']
        if procs < 1:
            raise CommandError('--procs must be >= 1')
        hostname = socket.gethostname()
        self.stdout.write(self.style.SUCCESS(f'[durable] worker started on {hostname}'))
        self._run_worker_loop(tick, batch, iterations, procs, opts['max_follower_tasks'])
