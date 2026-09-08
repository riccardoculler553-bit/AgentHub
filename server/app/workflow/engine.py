"""WorkflowEngine: deterministic orchestration (V1.3 §26-§30/§123).

Owns: which step is next, when it is READY, what Task to create, when the
run succeeds/fails/cancels. Never dispatches over WebSockets itself - the
Task Engine owns Attempt/Dispatch/Retry/Timeout (§5/§20/§203); the engine
only calls TaskService.create / request_retry / (cancel via service).

Advancement is event-driven (§27/§28): the task-terminal observer and the
WorkflowMonitor sweep both funnel into handle_task_result/advance. All
transitions are state-machine-legal AND DB CAS (§89/§90/§128) so concurrent
advancers (observer thread vs monitor sweep) can never double-create Tasks.
"""

import logging
import uuid

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.models import utcnow
from app.task import models as task_schemas
from app.task.db_models import Task, TaskAttempt, TaskEvent
from app.task.errors import InvalidTaskState, TaskError, TaskNotFound
from app.task.service import TaskService
from app.workflow.context import build_context, record_step_result
from app.workflow.db_models import Workflow, WorkflowEvent, WorkflowRun, WorkflowStep, WorkflowStepRun
from app.workflow.errors import (
    InvalidWorkflowState,
    WorkflowAlreadyRunning,
    WorkflowParamResolutionFailed,
    WorkflowRunNotFound,
)
from app.workflow.resolver import resolve_params
from app.workflow.state import (
    can_step_run_transition,
    can_workflow_transition,
    is_workflow_terminal,
)
from app.workflow.waiters import workflow_waiters

logger = logging.getLogger(__name__)


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


class WorkflowEngine:
    def __init__(self, db: Session) -> None:
        self.db = db

    # ------------------------------------------------------------------ helpers

    def _record(
        self,
        run_id: str,
        event_type: str,
        step_run_id: str | None = None,
        payload: dict | None = None,
    ) -> None:
        self.db.add(
            WorkflowEvent(run_id=run_id, step_run_id=step_run_id, event_type=event_type, payload=payload or {})
        )

    def get_run(self, run_id: str) -> WorkflowRun:
        row = self.db.scalars(select(WorkflowRun).where(WorkflowRun.run_id == run_id)).first()
        if row is None:
            raise WorkflowRunNotFound(run_id)
        return row

    def get_step_runs(self, run_id: str) -> list[WorkflowStepRun]:
        return list(
            self.db.scalars(
                select(WorkflowStepRun)
                .where(WorkflowStepRun.run_id == run_id)
                .order_by(WorkflowStepRun.order_no)
            )
        )

    def get_steps(self, workflow_id: str) -> list[WorkflowStep]:
        return list(
            self.db.scalars(
                select(WorkflowStep)
                .where(WorkflowStep.workflow_id == workflow_id)
                .order_by(WorkflowStep.order_no)
            )
        )

    # ----------------------------------------------------------------- create

    def create_run(
        self,
        workflow: Workflow,
        variables: dict | None,
        trigger_type: str,
        created_by: str,
    ) -> WorkflowRun:
        """WorkflowRun + all StepRuns upfront (§85/§86): Step1 READY, the rest
        PENDING - the dashboard sees the whole plan immediately."""
        if workflow.active_singleton:
            active = self.db.scalars(
                select(WorkflowRun).where(
                    WorkflowRun.workflow_name == workflow.name,
                    WorkflowRun.status.in_(["PENDING", "RUNNING"]),
                )
            ).first()
            if active is not None:
                raise WorkflowAlreadyRunning(workflow.name, active.run_id)

        run = WorkflowRun(
            run_id=_new_id("wrun"),
            workflow_id=workflow.workflow_id,
            workflow_name=workflow.name,
            workflow_version=workflow.version,
            status="PENDING",
            trigger_type=trigger_type,
            created_by=created_by,
            context_json=build_context(variables),
        )
        self.db.add(run)
        for order_no, step in enumerate(self.get_steps(workflow.workflow_id), start=1):
            self.db.add(
                WorkflowStepRun(
                    step_run_id=_new_id("wstep"),
                    run_id=run.run_id,
                    workflow_step_id=step.step_id,
                    name=step.name,
                    order_no=order_no,
                    command=step.command,
                    status="READY" if order_no == 1 else "PENDING",
                )
            )
        self._record(run.run_id, "workflow.created", payload={"workflow": workflow.name, "version": workflow.version})
        self.db.commit()
        return run

    # ------------------------------------------------------------------ start

    def start_run(self, run_id: str) -> list[str]:
        """PENDING -> RUNNING (CAS) then advance into the first step.
        Returns task ids that need dispatching."""
        claimed = (
            self.db.query(WorkflowRun)
            .filter(WorkflowRun.run_id == run_id, WorkflowRun.status == "PENDING")
            .update({"status": "RUNNING", "started_at": utcnow()}, synchronize_session=False)
        )
        self.db.commit()
        if claimed != 1:
            return []  # already started / cancelled
        run = self.get_run(run_id)
        self._record(run_id, "workflow.started")
        self.db.commit()
        return self.advance(run)

    # ---------------------------------------------------------------- advance

    def advance(self, run: WorkflowRun) -> list[str]:
        """Progress the run: start the next READY step, promote the next
        PENDING one, or complete the workflow (§24/§26/§87)."""
        if is_workflow_terminal(run.status):
            return []
        step_runs = self.get_step_runs(run.run_id)
        for step_run in step_runs:
            if step_run.status == "READY":
                task_id = self.start_step(run, step_run)
                return [task_id] if task_id else []
            if step_run.status == "PENDING":
                promoted = (
                    self.db.query(WorkflowStepRun)
                    .filter(
                        WorkflowStepRun.step_run_id == step_run.step_run_id,
                        WorkflowStepRun.status == "PENDING",
                    )
                    .update({"status": "READY"}, synchronize_session=False)
                )
                if promoted:
                    self._record(run.run_id, "workflow.step_ready", step_run.step_run_id, {"step": step_run.name})
                    self.db.commit()
                task_id = self.start_step(run, step_run)
                return [task_id] if task_id else []

        # no READY/PENDING step left: complete when everything succeeded (§129)
        if step_runs and all(sr.status == "SUCCESS" for sr in step_runs):
            completed = (
                self.db.query(WorkflowRun)
                .filter(WorkflowRun.run_id == run.run_id, WorkflowRun.status == "RUNNING")
                .update(
                    {"status": "SUCCESS", "finished_at": utcnow(), "current_step_run_id": None},
                    synchronize_session=False,
                )
            )
            self.db.commit()
            if completed:
                self._record(run.run_id, "workflow.completed")
                self.db.commit()
                workflow_waiters.notify(run.run_id)
        return []

    # -------------------------------------------------------------- start step

    def start_step(self, run: WorkflowRun, step_run: WorkflowStepRun) -> str | None:
        """READY -> RUNNING (CAS, §90) -> resolve params -> create Task.
        Returns the created task id, or None (lost the race / failed)."""
        now = utcnow()
        claimed = (
            self.db.query(WorkflowStepRun)
            .filter(
                WorkflowStepRun.step_run_id == step_run.step_run_id,
                WorkflowStepRun.status == "READY",
            )
            .update({"status": "RUNNING", "started_at": now}, synchronize_session=False)
        )
        if claimed != 1:
            return None
        self.db.query(WorkflowRun).filter(WorkflowRun.run_id == run.run_id).update(
            {"current_step_run_id": step_run.step_run_id}, synchronize_session=False
        )
        self._record(run.run_id, "workflow.step_started", step_run.step_run_id, {"step": step_run.name})
        self.db.commit()
        self.db.expire(step_run)
        run = self.get_run(run.run_id)

        step = self.db.scalars(
            select(WorkflowStep).where(WorkflowStep.step_id == step_run.workflow_step_id)
        ).first()
        if step is None or not step.enabled:
            return self._fail_step(run, step_run, "WORKFLOW_STEP_INVALID", "step definition missing or disabled")

        # --- params: Template -> Context Resolver -> real params (§36/§80)
        valid_steps = {s.name for s in self.get_steps(run.workflow_id)}
        try:
            params = resolve_params(step.params or {}, run.context_json or {}, valid_steps)
        except WorkflowParamResolutionFailed as exc:
            return self._fail_step(run, step_run, "WORKFLOW_PARAM_RESOLUTION_FAILED", str(exc))

        # --- device: fixed, else pick an online device with the capability
        device_id = step.device_id or self._pick_device(step.command)
        if device_id is None:
            return self._fail_step(
                run, step_run, "WORKFLOW_TASK_CREATE_FAILED",
                f"no online device reports capability: {step.command}",
            )

        # --- create the Task (Task Engine owns it from here, §30-§33)
        try:
            task = TaskService(self.db).create(
                task_schemas.TaskCreateIn(
                    name=f"[WF {run.workflow_name}] {step_run.name}"[:200],
                    target_device_id=device_id,
                    steps=[task_schemas.StepIn(command=step_run.command, params=params)],
                    source_type="WORKFLOW",
                    workflow_run_id=run.run_id,
                    workflow_step_run_id=step_run.step_run_id,
                ),
                created_by=f"workflow:{run.run_id}",
            )
        except TaskError as exc:
            return self._fail_step(run, step_run, "WORKFLOW_TASK_CREATE_FAILED", str(exc))

        self.db.query(WorkflowStepRun).filter(
            WorkflowStepRun.step_run_id == step_run.step_run_id
        ).update({"task_id": task.task_id}, synchronize_session=False)
        self.db.commit()
        logger.info(
            "workflow %s step %s created task %s (%s)",
            run.run_id, step_run.name, task.task_id, step_run.command,
        )
        return task.task_id

    def _pick_device(self, command: str) -> str | None:
        """Deterministic minimal selector (§113): DB-online device reporting
        the command capability; the Task Engine re-validates everything."""
        from app.capability.db_models import DeviceCapability
        from app.db.models import Device

        rows = self.db.execute(
            select(Device.device_id)
            .join(DeviceCapability, DeviceCapability.device_id == Device.device_id)
            .where(
                DeviceCapability.command_name == command,
                DeviceCapability.enabled.is_(True),
                Device.status == "online",
                Device.revoked_at.is_(None),
            )
            .order_by(Device.name)
            .limit(1)
        ).first()
        return rows[0] if rows else None

    def _fail_step(self, run: WorkflowRun, step_run: WorkflowStepRun, error_code: str, message: str) -> None:
        self.db.query(WorkflowStepRun).filter(
            WorkflowStepRun.step_run_id == step_run.step_run_id,
            WorkflowStepRun.status.in_(["READY", "RUNNING"]),
        ).update(
            {"status": "FAILED", "error_code": error_code, "error_message": message[:500],
             "finished_at": utcnow()},
            synchronize_session=False,
        )
        self.db.commit()
        self._record(run.run_id, "workflow.step_failed", step_run.step_run_id,
                     {"error_code": error_code, "error_message": message[:500]})
        self.db.commit()
        self.fail_workflow(run, "WORKFLOW_STEP_FAILED", f"step {step_run.name}: {error_code}")

    # ------------------------------------------------------- task integration

    def handle_task_result(self, task_id: str) -> list[str]:
        """A workflow-owned Task reached a terminal state: sync the StepRun
        from the DB facts (Task is the truth, §57/§58) and advance. Returns
        task ids that need dispatching (retried task or the next step's)."""
        step_run = self.db.scalars(
            select(WorkflowStepRun).where(
                WorkflowStepRun.task_id == task_id, WorkflowStepRun.status == "RUNNING"
            )
        ).first()
        if step_run is None:
            return []  # not workflow-owned / already synced (late duplicate)
        run = self.get_run(step_run.run_id)
        if is_workflow_terminal(run.status):
            return []

        try:
            task = TaskService(self.db).get(task_id)
        except TaskNotFound:
            return []

        if task.status == "SUCCESS":
            return self._complete_step(run, step_run, task.task_id)
        if task.status in ("FAILED", "TIMEOUT"):
            return self._handle_step_failure(run, step_run, task)
        if task.status == "CANCELLED":
            return self._cancel_from_task(run, step_run)
        return []  # PENDING/live again after a retry: nothing to sync

    def _final_result_payload(self, task_id: str) -> dict | None:
        row = self.db.scalars(
            select(TaskEvent)
            .where(TaskEvent.task_id == task_id, TaskEvent.event_type.in_(["task.success", "task.failed", "task.timeout", "task.cancelled"]))
            .order_by(TaskEvent.id.desc())
        ).first()
        if row is None:
            return None
        payload = dict(row.payload or {})
        if row.event_type == "task.success":
            return dict(payload.get("result") or {})
        return None

    def _complete_step(self, run: WorkflowRun, step_run: WorkflowStepRun, task_id: str) -> list[str]:
        result = self._final_result_payload(task_id)
        updated = (
            self.db.query(WorkflowStepRun)
            .filter(WorkflowStepRun.step_run_id == step_run.step_run_id, WorkflowStepRun.status == "RUNNING")
            .update(
                {"status": "SUCCESS", "result": result, "finished_at": utcnow()},
                synchronize_session=False,
            )
        )
        self.db.commit()
        if not updated:
            return []  # lost a cancel race (§51): the cancel path owns the step
        record_step_result(
            self._reload_context(run.run_id), step_run.name, task_id, "SUCCESS", result
        )
        self._persist_context(run.run_id)
        self._record(run.run_id, "workflow.step_success", step_run.step_run_id,
                     {"step": step_run.name, "task_id": task_id})
        self.db.commit()
        run = self.get_run(run.run_id)
        return self.advance(run)

    def _reload_context(self, run_id: str) -> dict:
        run = self.get_run(run_id)
        context = run.context_json or build_context({})
        context.setdefault("variables", {})
        context.setdefault("steps", {})
        return context

    def _persist_context(self, run_id: str) -> None:
        run = self.get_run(run_id)
        self.db.query(WorkflowRun).filter(WorkflowRun.run_id == run_id).update(
            {"context_json": run.context_json}, synchronize_session=False
        )

    # ------------------------------------------------- failure / retry / cancel

    def _handle_step_failure(self, run: WorkflowRun, step_run: WorkflowStepRun, task: Task) -> list[str]:
        error_code = self._task_error_code(task.task_id)
        step = self.db.scalars(
            select(WorkflowStep).where(WorkflowStep.step_id == step_run.workflow_step_id)
        ).first()
        policy = dict((step.retry_policy if step else None) or {})
        max_attempts = int(policy.get("max_attempts", 1) or 1)
        retry_on = list(policy.get("retry_on") or [])
        allowed = (
            (step.on_failure if step else "stop") == "retry"
            and step_run.retry_count < max_attempts
            and (not retry_on or error_code in retry_on)
        )
        if allowed:
            try:
                TaskService(self.db).request_retry(task.task_id)
            except InvalidTaskState:
                allowed = False  # Task Engine budget exhausted -> fall through to stop
        if allowed:
            self.db.query(WorkflowStepRun).filter(
                WorkflowStepRun.step_run_id == step_run.step_run_id
            ).update({"retry_count": step_run.retry_count + 1}, synchronize_session=False)
            self.db.commit()
            self._record(
                run.run_id, "workflow.step_retry", step_run.step_run_id,
                {"step": step_run.name, "task_id": task.task_id, "error_code": error_code,
                 "attempt": step_run.retry_count + 1, "max_attempts": max_attempts},
            )
            self.db.commit()
            return [task.task_id]  # Task stays PENDING -> dispatcher re-fires it

        # stop (or retry budget exhausted): fail the run (§44/§130)
        self.db.query(WorkflowStepRun).filter(
            WorkflowStepRun.step_run_id == step_run.step_run_id, WorkflowStepRun.status == "RUNNING"
        ).update(
            {"status": "FAILED", "error_code": error_code, "finished_at": utcnow()},
            synchronize_session=False,
        )
        self.db.commit()
        self._record(run.run_id, "workflow.step_failed", step_run.step_run_id,
                     {"step": step_run.name, "task_id": task.task_id, "error_code": error_code})
        self.db.commit()
        self.fail_workflow(run, "WORKFLOW_FAILED", f"step {step_run.name} failed ({error_code})")
        return []

    def _task_error_code(self, task_id: str) -> str:
        attempt = self.db.scalars(
            select(TaskAttempt)
            .where(TaskAttempt.task_id == task_id)
            .order_by(TaskAttempt.id.desc())
        ).first()
        if attempt is not None and attempt.error_code:
            return attempt.error_code
        return "EXECUTOR_FAILED"

    def fail_workflow(self, run: WorkflowRun, error_code: str, message: str) -> None:
        """Remaining steps SKIPPED, run FAILED via CAS (§44/§130)."""
        for step_run in self.get_step_runs(run.run_id):
            if step_run.status in ("PENDING", "READY"):
                self.db.query(WorkflowStepRun).filter(
                    WorkflowStepRun.step_run_id == step_run.step_run_id,
                    WorkflowStepRun.status.in_(["PENDING", "READY"]),
                ).update(
                    {"status": "SKIPPED", "finished_at": utcnow()},
                    synchronize_session=False,
                )
        updated = (
            self.db.query(WorkflowRun)
            .filter(WorkflowRun.run_id == run.run_id, WorkflowRun.status == "RUNNING")
            .update(
                {"status": "FAILED", "error_code": error_code, "error_message": message[:500],
                 "finished_at": utcnow(), "current_step_run_id": None},
                synchronize_session=False,
            )
        )
        self.db.commit()
        if not updated:
            return  # a cancel won the CAS (§51)
        self._record(run.run_id, "workflow.failed", payload={"error_code": error_code, "error_message": message[:500]})
        self.db.commit()
        workflow_waiters.notify(run.run_id)

    def _cancel_from_task(self, run: WorkflowRun, step_run: WorkflowStepRun) -> list[str]:
        """The current task was cancelled (worker/admin): run follows (§49)."""
        self.db.query(WorkflowStepRun).filter(
            WorkflowStepRun.step_run_id == step_run.step_run_id, WorkflowStepRun.status == "RUNNING"
        ).update({"status": "CANCELLED", "finished_at": utcnow()}, synchronize_session=False)
        for other in self.get_step_runs(run.run_id):
            if other.status in ("PENDING", "READY"):
                self.db.query(WorkflowStepRun).filter(
                    WorkflowStepRun.step_run_id == other.step_run_id,
                    WorkflowStepRun.status.in_(["PENDING", "READY"]),
                ).update({"status": "CANCELLED", "finished_at": utcnow()}, synchronize_session=False)
        updated = (
            self.db.query(WorkflowRun)
            .filter(WorkflowRun.run_id == run.run_id, WorkflowRun.status == "RUNNING")
            .update(
                {"status": "CANCELLED", "finished_at": utcnow(), "current_step_run_id": None},
                synchronize_session=False,
            )
        )
        self.db.commit()
        if not updated:
            return []
        self._record(run.run_id, "workflow.cancelled", payload={"step": step_run.name})
        self.db.commit()
        workflow_waiters.notify(run.run_id)
        return []

    def cancel_run(self, run_id: str) -> dict:
        """User-initiated cancel (§49-§51): CAS the run first; the current
        task cancel goes through TaskService; remaining steps CANCELLED (§131).
        Returns {"notify_task_id": ...} for the caller's device notification."""
        run = self.get_run(run_id)
        if is_workflow_terminal(run.status):
            raise InvalidWorkflowState(run_id, run.status, "cancel")
        started = run.status == "RUNNING"
        target_from, target_to = ("RUNNING", "CANCELLED") if started else ("PENDING", "CANCELLED")
        if not can_workflow_transition(run.status, "CANCELLED"):
            raise InvalidWorkflowState(run_id, run.status, "cancel")

        updated = (
            self.db.query(WorkflowRun)
            .filter(WorkflowRun.run_id == run_id, WorkflowRun.status == run.status)
            .update(
                {"status": "CANCELLED", "finished_at": utcnow(), "current_step_run_id": None},
                synchronize_session=False,
            )
        )
        if updated != 1:
            raise InvalidWorkflowState(run_id, run.status, "cancel")

        notify_task_id = None
        for step_run in self.get_step_runs(run_id):
            if step_run.status in ("RUNNING", "READY"):
                self.db.query(WorkflowStepRun).filter(
                    WorkflowStepRun.step_run_id == step_run.step_run_id,
                    WorkflowStepRun.status.in_(["RUNNING", "READY"]),
                ).update(
                    {"status": "CANCELLED", "finished_at": utcnow()},
                    synchronize_session=False,
                )
                if step_run.task_id and started:
                    try:
                        task = TaskService(self.db).get(step_run.task_id)
                        if task.status not in ("SUCCESS", "FAILED", "TIMEOUT", "CANCELLED"):
                            TaskService(self.db).request_cancel(step_run.task_id)
                            notify_task_id = step_run.task_id
                    except TaskNotFound:
                        pass
            elif step_run.status == "PENDING":
                self.db.query(WorkflowStepRun).filter(
                    WorkflowStepRun.step_run_id == step_run.step_run_id,
                    WorkflowStepRun.status == "PENDING",
                ).update({"status": "CANCELLED", "finished_at": utcnow()}, synchronize_session=False)

        self._record(run_id, "workflow.cancelled", payload={"by": "user"})
        self.db.commit()
        workflow_waiters.notify(run_id)
        return {"run_id": run_id, "status": "CANCELLED", "notify_task_id": notify_task_id}

    # ---------------------------------------------------------------- recovery

    def recover_run(self, run: WorkflowRun) -> list[str]:
        """Server-restart / sweep repair for one RUNNING run (§54-§56/§93):
        re-sync from Task facts, never recreate Tasks. Returns dispatch ids."""
        if run.status == "PENDING":
            return self.start_run(run.run_id)  # crashed between create and start
        if run.status != "RUNNING":
            return []

        step_runs = self.get_step_runs(run.run_id)
        active = [sr for sr in step_runs if sr.status == "RUNNING"]
        if active:
            step_run = active[0]
            if step_run.task_id is None:
                # orphan: RUNNING without a task id (§56) - not guessable
                return self._fail_step(run, step_run, "WORKFLOW_ORPHAN_STEP", "RUNNING step has no task") or []
            try:
                task = TaskService(self.db).get(step_run.task_id)
            except TaskNotFound:
                return self._fail_step(
                    run, step_run, "WORKFLOW_ORPHAN_STEP", f"task {step_run.task_id} not found"
                ) or []
            if task.status in ("SUCCESS", "FAILED", "TIMEOUT", "CANCELLED"):
                return self.handle_task_result(step_run.task_id)
            return []  # live task: Task Engine still owns it (§54)
        return self.advance(run)
