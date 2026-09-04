import json
import select
import socket
import subprocess
import sys
import time
import uuid
from contextlib import redirect_stdout
from datetime import timedelta

from django.core.management.base import BaseCommand, CommandError
from django.db import DatabaseError, close_old_connections
from django.db.models import Q
from django.utils import timezone

from django_durable.constants import ErrorCode
from django_durable.engine import execute_activity, step_workflow
from django_durable.models import ActivityTask, WorkflowExecution


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
        return task.mark_timed_out()

    def _timeout_workflow(self, wf):
        return wf.time_out()

    def _cancel_activity(self, task):
        return task.fail_due_to_cancel()

    def _recover_activity(self, info):
        if info['type'] != 'activity':
            return
        task = ActivityTask.objects.filter(pk=info['id']).first()
        if task is not None:
            task.lease_token = uuid.UUID(info['token'])
            task.retry_or_fail('worker_lost')

    @staticmethod
    def _close_process(proc):
        if proc.poll() is None:
            proc.kill()
        proc.wait()
        proc.stdin.close()
        proc.stdout.close()

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

    def _respawn_follower(self, idle, max_tasks):
        proc = self._spawn_follower_proc(max_tasks)
        idle.append(proc)
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
                if cmd == 'exit':
                    break
                # User prints/logging must not be interpreted as an ACK.
                with redirect_stdout(sys.stderr):
                    if cmd == 'activity':
                        task = ActivityTask.objects.get(id=msg['id'])
                        if str(task.lease_token) == msg.get('token'):
                            execute_activity(task, claimed=True)
                    elif cmd == 'workflow':
                        wf = WorkflowExecution.objects.get(id=msg['id'])
                        step_workflow(wf)
                sys.stdout.write(
                    json.dumps({'ok': True, 'id': msg['id'], 'token': msg.get('token')})
                    + '\n'
                )
                sys.stdout.flush()
                processed += 1
                if max_tasks and processed >= max_tasks:
                    break
        finally:
            close_old_connections()

    def _run_worker_loop(self, tick, batch, iterations, procs, max_tasks):
        close_old_connections()
        idle = []
        running = []
        try:
            loops = 0
            idle = [self._spawn_follower_proc(max_tasks) for _ in range(procs)]
            running = []
            while True:
                now = timezone.now()
                progressed = False

                progressed |= self._refresh_idle_processes(idle, running, max_tasks)
                progressed |= self._handle_running_processes(
                    running, idle, max_tasks, now
                )
                progressed |= self._process_timeouts(now, batch)
                progressed |= self._dispatch_due_activities(
                    now, batch, idle, running, max_tasks
                )
                progressed |= self._dispatch_runnable_workflows(
                    now, batch, idle, running, max_tasks
                )

                loops += 1
                if iterations is not None and loops >= iterations and not running:
                    break
                if not progressed:
                    time.sleep(tick)
        finally:
            for info in running:
                self._close_process(info['proc'])
                self._recover_activity(info)
            for proc in idle:
                self._close_process(proc)
            close_old_connections()

    def _refresh_idle_processes(self, idle, running, max_tasks):
        progressed = False
        for proc in list(idle):
            if proc.poll() is not None:
                idle.remove(proc)
                self._close_process(proc)
                self._respawn_follower(idle, max_tasks)
                progressed = True

        if running:
            rlist = [info['proc'].stdout for info in running]
            ready, _, _ = select.select(rlist, [], [], 0)
            for r in ready:
                for info in list(running):
                    if info['proc'].stdout is r:
                        line = r.readline()
                        if not line:
                            self._close_process(info['proc'])
                            self._recover_activity(info)
                            running.remove(info)
                            self._respawn_follower(idle, max_tasks)
                            progressed = True
                            break
                        try:
                            ack = json.loads(line)
                        except ValueError:
                            break
                        if ack != {
                            'ok': True,
                            'id': info['id'],
                            'token': info.get('token'),
                        }:
                            break
                        running.remove(info)
                        idle.append(info['proc'])
                        progressed = True
                        break
        return progressed

    def _handle_running_processes(self, running, idle, max_tasks, now):
        progressed = False
        for info in list(running):
            proc = info['proc']
            if proc.poll() is not None:
                self._close_process(proc)
                self._recover_activity(info)
                running.remove(info)
                self._respawn_follower(idle, max_tasks)
                progressed = True
                continue
            deadline = info.get('deadline')
            if deadline is not None and now >= deadline:
                self._terminate_timed_out_process(proc, info)
                running.remove(info)
                self._respawn_follower(idle, max_tasks)
                progressed = True
                continue

            if info['type'] == 'activity':
                progressed |= self._check_running_activity(
                    proc, info, running, idle, max_tasks
                )
            else:
                progressed |= self._check_running_workflow(
                    proc, info, running, idle, max_tasks
                )
        return progressed

    def _terminate_timed_out_process(self, proc, info):
        self._close_process(proc)
        if info['type'] == 'activity':
            try:
                task = ActivityTask.objects.get(id=info['id'])
                task.lease_token = uuid.UUID(info['token'])
                self._timeout_activity(task)
            except ActivityTask.DoesNotExist:
                pass
        else:
            try:
                wf = WorkflowExecution.objects.get(id=info['id'])
                self._timeout_workflow(wf)
            except WorkflowExecution.DoesNotExist:
                pass

    def _check_running_activity(self, proc, info, running, idle, max_tasks):
        task = (
            ActivityTask.objects.select_related('execution')
            .filter(id=info['id'])
            .first()
        )
        if (
            task is None
            or str(task.lease_token) != info['token']
            or task.status
            in (ActivityTask.Status.QUEUED, ActivityTask.Status.TIMED_OUT)
            or task.execution.is_terminal()
        ):
            self._close_process(proc)
            running.remove(info)
            self._respawn_follower(idle, max_tasks)
            return True
        if task.status != ActivityTask.Status.RUNNING:
            return False  # The committed outcome is awaiting its ACK.
        if not task.renew_lease():
            self._close_process(proc)
            self._recover_activity(info)
            running.remove(info)
            self._respawn_follower(idle, max_tasks)
            return True
        return False

    def _check_running_workflow(self, proc, info, running, idle, max_tasks):
        try:
            wf = WorkflowExecution.objects.select_related('parent').get(id=info['id'])
        except WorkflowExecution.DoesNotExist:
            self._close_process(proc)
            running.remove(info)
            self._respawn_follower(idle, max_tasks)
            return True
        parent_canceled = (
            wf.parent_id
            and WorkflowExecution.objects.filter(
                id=wf.parent_id,
                status=WorkflowExecution.Status.CANCELED,
            ).exists()
        )
        if wf.is_terminal() or parent_canceled:
            self._close_process(proc)
            running.remove(info)
            self._respawn_follower(idle, max_tasks)
            return True
        return False

    def _process_timeouts(self, now, batch):
        progressed = False
        progressed |= self._timeout_workflows(now, batch)
        progressed |= self._timeout_queued_activities(now, batch)
        progressed |= self._heartbeat_timeouts(now, batch)
        progressed |= self._schedule_to_close_timeouts(now, batch)
        expired = (
            ActivityTask.objects.filter(status=ActivityTask.Status.RUNNING)
            .filter(
                Q(lease_expires_at__lte=now)
                | Q(
                    lease_expires_at=None,
                    updated_at__lte=now - ActivityTask.lease_duration(),
                )
            )
            .order_by('updated_at')[:batch]
        )
        for task in expired:
            progressed |= task.retry_or_fail('worker_lost', lease_before=now)
        woke = WorkflowExecution.objects.filter(
            status=WorkflowExecution.Status.RUNNING,
            wake_at__lte=now,
        ).update(status=WorkflowExecution.Status.PENDING, wake_at=None)
        return progressed or bool(woke)

    def _timeout_queued_activities(self, now, batch):
        progressed = False
        for task in ActivityTask.objects.filter(
            status=ActivityTask.Status.QUEUED, expires_at__lte=now
        )[:batch]:
            progressed |= task.mark_timed_out()
        return progressed

    def _timeout_workflows(self, now, batch):
        progressed = False
        for wf in WorkflowExecution.objects.filter(
            status__in=[
                WorkflowExecution.Status.PENDING,
                WorkflowExecution.Status.RUNNING,
            ],
            expires_at__lte=now,
        )[:batch]:
            progressed |= wf.time_out()
        return progressed

    def _heartbeat_timeouts(self, now, batch):
        progressed = False
        expired = 0
        # Limit expired tasks, not healthy tasks; otherwise the first batch of
        # live heartbeats can hide every stale heartbeat behind it indefinitely.
        for task in ActivityTask.objects.filter(
            status=ActivityTask.Status.RUNNING,
            heartbeat_timeout__isnull=False,
            heartbeat_at__isnull=False,
        ).iterator():
            hb_at = task.heartbeat_at or task.started_at or task.updated_at
            if hb_at + timedelta(seconds=task.heartbeat_timeout) <= now:
                progressed |= self._handle_heartbeat_timeout(task, now)
                expired += 1
                if expired >= batch:
                    break
        return progressed

    def _handle_heartbeat_timeout(self, task, now):
        return task.retry_or_fail(
            ErrorCode.HEARTBEAT_TIMEOUT.value, timed_out=True, heartbeat_before=now
        )

    def _schedule_to_close_timeouts(self, now, batch):
        progressed = False
        for task in ActivityTask.objects.filter(
            status=ActivityTask.Status.RUNNING, expires_at__lte=now
        )[:batch]:
            progressed |= self._handle_schedule_to_close_timeout(task, now)
        return progressed

    def _handle_schedule_to_close_timeout(self, task, now):
        return task.mark_timed_out()

    def _dispatch_due_activities(self, now, batch, idle, running, max_tasks):
        slots = len(idle)
        if slots <= 0:
            return False
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
        if not due_ids:
            return False
        progressed = False
        for tid in due_ids:
            if not idle:
                break
            proc = idle.pop(0)
            try:
                task = ActivityTask.objects.get(id=tid)
                claimed = task.start()
            except (DatabaseError, ActivityTask.DoesNotExist):
                idle.append(proc)
                continue
            if not claimed:
                idle.append(proc)
                continue
            timeout = None
            if task.expires_at is not None:
                timeout = max(0.0, (task.expires_at - timezone.now()).total_seconds())
            msg = (
                json.dumps(
                    {'cmd': 'activity', 'id': tid, 'token': str(task.lease_token)}
                )
                + '\n'
            )
            try:
                proc.stdin.write(msg)
                proc.stdin.flush()
            except Exception:
                self._close_process(proc)
                task.retry_or_fail('worker_lost')
                self._respawn_follower(idle, max_tasks)
                continue
            deadline = (
                timezone.now() + timedelta(seconds=timeout)
                if timeout is not None
                else None
            )
            running.append(
                {
                    'type': 'activity',
                    'id': tid,
                    'token': str(task.lease_token),
                    'proc': proc,
                    'deadline': deadline,
                }
            )
            progressed = True
        return progressed

    def _dispatch_runnable_workflows(self, now, batch, idle, running, max_tasks):
        if not idle:
            return False
        runnable_ids = list(
            WorkflowExecution.objects.filter(status=WorkflowExecution.Status.PENDING)
            .exclude(
                pk__in=[info['id'] for info in running if info['type'] == 'workflow']
            )
            .order_by('updated_at')
            .values_list('id', flat=True)[: min(batch, len(idle))]
        )
        if not runnable_ids:
            return False
        progressed = False
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
                timeout = max(0.0, (wf.expires_at - timezone.now()).total_seconds())
            msg = json.dumps({'cmd': 'workflow', 'id': wid}) + '\n'
            try:
                proc.stdin.write(msg)
                proc.stdin.flush()
            except Exception:
                self._close_process(proc)
                self._respawn_follower(idle, max_tasks)
                continue
            deadline = (
                timezone.now() + timedelta(seconds=timeout)
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
            progressed = True
        return progressed

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
        self._run_worker_loop(
            tick, batch, iterations, procs, opts['max_follower_tasks']
        )
