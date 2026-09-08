"""V1.3 Workflow Engine integration tests (§166/§179 scenarios 1-8).

Real chain: API -> WorkflowService -> WorkflowEngine -> TaskService ->
TaskDispatcher -> FakeWorker -> task.result -> task-terminal observer ->
WorkflowEngine advance -> next step. DB state is asserted as the truth.
"""

import pytest

from app.core.config import settings
from app.db.database import SessionLocal
from app.task.db_models import Task, TaskEvent
from app.workflow.db_models import WorkflowRun, WorkflowStepRun
from app.workflow.monitor import WorkflowMonitor

from ._worker import FakeWorker, register_device, wait_until

ECHO_CAPS = ("echo",)


@pytest.fixture()
def device(client):
    """Registered device with a committed echo capability (DB-seeded, like
    the V1.2 tool tests - no WS caps race)."""
    payload = register_device(client, "流程测试机")
    from app.capability.service import CapabilityService

    with SessionLocal() as db:
        CapabilityService(db).replace_device_capabilities(
            payload["device_id"], [{"name": "echo", "version": "1.0"}]
        )
    return payload


def _make_workflow(client, device, steps, name="daily_report", enabled=True, **kwargs) -> str:
    payload = {
        "name": name,
        "version": "1.0.0",
        "description": "测试流程",
        "steps": steps,
        **kwargs,
    }
    res = client.post("/api/workflows", json=payload)
    assert res.status_code == 201, res.text
    workflow_id = res.json()["workflow_id"]
    if enabled:
        res = client.post(f"/api/workflows/{workflow_id}/enable")
        assert res.status_code == 200, res.text
    return workflow_id


def _start_run(client, workflow_id, variables=None) -> str:
    res = client.post(f"/api/workflows/{workflow_id}/runs", json={"variables": variables or {}})
    assert res.status_code == 201, res.text
    return res.json()["run_id"]


def _run(client, run_id) -> dict:
    return client.get(f"/api/workflow-runs/{run_id}").json()


def _wait_terminal(client, run_id, timeout=20) -> dict:
    assert wait_until(lambda: _run(client, run_id)["status"] in ("SUCCESS", "FAILED", "CANCELLED"), timeout), (
        f"run {run_id} did not reach a terminal state"
    )
    return _run(client, run_id)


# -------------------------------------------------------- scenario 1: three steps


def test_three_step_workflow_success(client, device):
    worker = FakeWorker(client, device["device_token"], capabilities=ECHO_CAPS, behaviour="success")
    worker.start()
    try:
        assert wait_for_caps(client, device["device_id"])
        workflow_id = _make_workflow(
            client,
            device,
            steps=[
                {"name": "step_one", "command": "echo", "params": {"message": "one"}},
                {"name": "step_two", "command": "echo", "params": {"message": "two"}},
                {"name": "step_three", "command": "echo", "params": {"message": "three"}},
            ],
        )
        run_id = _start_run(client, workflow_id)
        run = _wait_terminal(client, run_id)
        assert run["status"] == "SUCCESS"
        assert [s["status"] for s in run["steps"]] == ["SUCCESS", "SUCCESS", "SUCCESS"]
        # every step mapped to its own Task with WORKFLOW provenance (§31-§33)
        with SessionLocal() as db:
            task_ids = [s["task_id"] for s in run["steps"]]
            assert len(set(task_ids)) == 3
            for task_id in task_ids:
                task = db.scalars(select_task(task_id)).first()
                assert task is not None
                assert task.source_type == "WORKFLOW"
                assert task.workflow_run_id == run_id
                assert task.workflow_step_run_id in {s["step_run_id"] for s in run["steps"]}
    finally:
        worker.stop()


# ------------------------------------------------- scenario 2: params step1 -> step2


def test_parameter_passing_between_steps(client, device):
    worker = FakeWorker(client, device["device_token"], capabilities=ECHO_CAPS, behaviour="success")
    worker.start()
    try:
        assert wait_for_caps(client, device["device_id"])
        workflow_id = _make_workflow(
            client,
            device,
            steps=[
                {
                    "name": "download",
                    "command": "echo",
                    "params": {"message": "file-{{ variables.tag }}"},
                },
                {
                    "name": "process",
                    "command": "echo",
                    "params": {"message": "{{ steps.download.result.echo }}"},
                },
            ],
        )
        run_id = _start_run(client, workflow_id, variables={"tag": "v13"})
        run = _wait_terminal(client, run_id)
        assert run["status"] == "SUCCESS"
        step1, step2 = run["steps"]
        assert step1["result"] == {"echo": "file-v13"}
        # step2's Task received the resolved param from step1's result (§36/§142)
        with SessionLocal() as db:
            task = db.scalars(select_task(step2["task_id"])).first()
            from app.task.db_models import TaskStep

            step_row = db.scalars(
                __import__("sqlalchemy").select(TaskStep).where(TaskStep.task_id == step2["task_id"])
            ).first()
            assert step_row.params == {"message": "file-v13"}
        # context persisted for restart-safety (§94/§95)
        assert run["context"]["steps"]["download"]["result"] == {"echo": "file-v13"}
    finally:
        worker.stop()


# ------------------------------------------------ scenarios 3/4: retry then success


class FlakyWorker(FakeWorker):
    """Fails the first `fail_first` dispatches, then succeeds."""

    def __init__(self, *args, fail_first: int = 1, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.fail_first = fail_first
        self._answered = 0

    def _answer(self, session, data: dict) -> None:
        self._answered += 1
        self.behaviour = "fail" if self._answered <= self.fail_first else "success"
        super()._answer(session, data)


def test_step_retry_then_success(client, device):
    worker = FlakyWorker(client, device["device_token"], capabilities=ECHO_CAPS, fail_first=1)
    worker.start()
    try:
        assert wait_for_caps(client, device["device_id"])
        workflow_id = _make_workflow(
            client,
            device,
            steps=[
                {"name": "flaky", "command": "echo", "params": {"message": "x"},
                 "on_failure": "retry", "retry_policy": {"max_attempts": 2}},
                {"name": "after", "command": "echo", "params": {"message": "y"}},
            ],
        )
        run_id = _start_run(client, workflow_id)
        run = _wait_terminal(client, run_id)
        # §45: the retry is a Task Engine retry; the run still completes (§172)
        assert run["status"] == "SUCCESS"
        flaky = run["steps"][0]
        assert flaky["status"] == "SUCCESS"
        assert flaky["retry_count"] == 1
        after = run["steps"][1]
        assert after["status"] == "SUCCESS"
        with SessionLocal() as db:
            attempts = db.scalars(
                __import__("sqlalchemy").select(TaskAttempt).where(TaskAttempt.task_id == flaky["task_id"])
            ).all()
            assert len(attempts) == 2  # one workflow-level retry, two task attempts
    finally:
        worker.stop()


def test_retry_budget_exhausted_stops_workflow(client, device):
    worker = FakeWorker(client, device["device_token"], capabilities=ECHO_CAPS, behaviour="fail")
    worker.start()
    try:
        assert wait_for_caps(client, device["device_id"])
        workflow_id = _make_workflow(
            client,
            device,
            steps=[
                {"name": "doomed", "command": "echo", "params": {"message": "x"},
                 "on_failure": "retry", "retry_policy": {"max_attempts": 2}},
                {"name": "never", "command": "echo", "params": {"message": "y"}},
            ],
        )
        run_id = _start_run(client, workflow_id)
        run = _wait_terminal(client, run_id)
        # §183: permanent failure stops the workflow, remaining steps SKIPPED
        assert run["status"] == "FAILED"
        assert run["error_code"] == "WORKFLOW_FAILED"
        steps = {s["name"]: s for s in run["steps"]}
        assert steps["doomed"]["status"] == "FAILED"
        assert steps["doomed"]["retry_count"] == 2
        assert steps["never"]["status"] == "SKIPPED"
        assert steps["never"]["task_id"] is None  # never created a Task
    finally:
        worker.stop()


def test_on_failure_stop_uses_task_retry_policy_not_llm(client, device):
    """§77: Workflow is deterministic - stop policy fails immediately, no
    retry, even though the Task Engine itself could retry."""
    worker = FakeWorker(client, device["device_token"], capabilities=ECHO_CAPS, behaviour="fail")
    worker.start()
    try:
        assert wait_for_caps(client, device["device_id"])
        workflow_id = _make_workflow(
            client,
            device,
            steps=[
                {"name": "boom", "command": "echo", "params": {"message": "x"}},
                {"name": "never", "command": "echo", "params": {"message": "y"}},
            ],
        )
        run_id = _start_run(client, workflow_id)
        run = _wait_terminal(client, run_id)
        assert run["status"] == "FAILED"
        steps = {s["name"]: s for s in run["steps"]}
        assert steps["boom"]["retry_count"] == 0
        assert steps["never"]["status"] == "SKIPPED"
    finally:
        worker.stop()


# ----------------------------------------------------- scenario 5: cancel mid-run


def test_cancel_running_workflow(client, device):
    worker = FakeWorker(client, device["device_token"], capabilities=ECHO_CAPS, behaviour="silent")
    worker.start()
    try:
        assert wait_for_caps(client, device["device_id"])
        workflow_id = _make_workflow(
            client,
            device,
            steps=[
                {"name": "long_one", "command": "echo", "params": {"message": "x"}},
                {"name": "long_two", "command": "echo", "params": {"message": "y"}},
            ],
        )
        run_id = _start_run(client, workflow_id)
        # wait until step1's task is dispatched (SENT)
        assert wait_until(
            lambda: _run(client, run_id)["steps"][0]["task_id"]
            and _task_status(_run(client, run_id)["steps"][0]["task_id"]) == "SENT"
        )
        res = client.post(f"/api/workflow-runs/{run_id}/cancel")
        assert res.status_code == 200, res.text
        run = _wait_terminal(client, run_id)
        # §177: run CANCELLED, current step + task CANCELLED, next step never runs
        assert run["status"] == "CANCELLED"
        steps = {s["name"]: s for s in run["steps"]}
        assert steps["long_one"]["status"] == "CANCELLED"
        assert _task_status(steps["long_one"]["task_id"]) == "CANCELLED"
        assert steps["long_two"]["status"] == "CANCELLED"
        assert steps["long_two"]["task_id"] is None
    finally:
        worker.stop()


# --------------------------------------------------- scenario 7: singleton duplicate


def test_active_singleton_blocks_duplicate_run(client, device):
    worker = FakeWorker(client, device["device_token"], capabilities=ECHO_CAPS, behaviour="silent")
    worker.start()
    try:
        assert wait_for_caps(client, device["device_id"])
        workflow_id = _make_workflow(
            client,
            device,
            steps=[{"name": "only", "command": "echo", "params": {"message": "x"}}],
            active_singleton=True,
        )
        first = _start_run(client, workflow_id)
        assert _run(client, first)["status"] in ("RUNNING", "PENDING")
        res = client.post(f"/api/workflows/{workflow_id}/runs", json={})
        assert res.status_code == 409
        assert res.json()["detail"]["code"] == "workflow_already_running"
        client.post(f"/api/workflow-runs/{first}/cancel")
    finally:
        worker.stop()


# ---------------------------------------------- scenario 6: restart recovery sweep


def test_monitor_sweep_recovers_missed_task_result(client, device):
    """Server-restart simulation (§54/§55): the task reached SUCCESS but the
    observer notification was lost - the sweep syncs the run from DB facts
    without recreating tasks."""
    worker = FakeWorker(client, device["device_token"], capabilities=ECHO_CAPS, behaviour="silent")
    worker.start()
    try:
        assert wait_for_caps(client, device["device_id"])
        workflow_id = _make_workflow(
            client,
            device,
            steps=[
                {"name": "missed", "command": "echo", "params": {"message": "x"}},
                {"name": "next", "command": "echo", "params": {"message": "y"}},
            ],
        )
        run_id = _start_run(client, workflow_id)
        assert wait_until(lambda: _run(client, run_id)["steps"][0]["task_id"])
        task_id = _run(client, run_id)["steps"][0]["task_id"]
        # simulate the lost notification: task terminal in DB, run still RUNNING
        with SessionLocal() as db:
            task = db.scalars(select_task(task_id)).first()
            task.status = "SUCCESS"
            task.finished_at = task.created_at
            db.add(TaskEvent(task_id=task_id, event_type="task.success",
                             payload={"result": {"file_path": "D:/orders.xlsx"}}))
            db.commit()
        dispatch_ids = WorkflowMonitor(None).sweep()
        # sweep re-synced step1, advanced to step2 and created its Task (§87)
        run = _run(client, run_id)
        steps = {s["name"]: s for s in run["steps"]}
        assert steps["missed"]["status"] == "SUCCESS"
        assert run["context"]["steps"]["missed"]["result"] == {"file_path": "D:/orders.xlsx"}
        assert steps["next"]["status"] in ("READY", "RUNNING")
        assert steps["next"]["task_id"] is not None
        assert steps["next"]["task_id"] != task_id  # new step -> new task, not recreated
        assert task_id in dispatch_ids or steps["next"]["task_id"] in dispatch_ids
    finally:
        worker.stop()


def test_orphan_step_detection(client, device):
    """§56: a RUNNING step without a task is not guessable -> run FAILED."""
    from app.workflow.engine import WorkflowEngine

    with SessionLocal() as db:
        engine = WorkflowEngine(db)
        # 'echo' is a seed command (main.py lifespan) - no manual insert here
        from app.workflow.registry import WorkflowRegistry
        from app.workflow.schemas import WorkflowDefinitionIn, WorkflowStepDefinition

        definition = WorkflowDefinitionIn(
            name="orphan_case",
            version="1.0.0",
            steps=[WorkflowStepDefinition(name="ghost_step", command="echo")],
        )
        workflow = WorkflowRegistry(db).create(definition)
        run = engine.create_run(workflow, {}, "api", "test")
        step_run = engine.get_step_runs(run.run_id)[0]
        # surgery: pretend the step started but its task vanished (§56)
        db.query(WorkflowStepRun).filter(WorkflowStepRun.step_run_id == step_run.step_run_id).update(
            {"status": "RUNNING", "task_id": None}, synchronize_session=False
        )
        db.query(WorkflowRun).filter(WorkflowRun.run_id == run.run_id).update(
            {"status": "RUNNING"}, synchronize_session=False
        )
        db.commit()
        run_id = run.run_id
        workflow_name = "orphan_case"

    dispatch_ids = WorkflowMonitor(None).sweep()
    with SessionLocal() as db:
        from sqlalchemy import select

        run = db.scalars(select(WorkflowRun).where(WorkflowRun.run_id == run_id)).first()
        assert run.status == "FAILED"
        # run-level code is the generic stop policy code; the orphan detail
        # lives on the step (fail_workflow is called with WORKFLOW_STEP_FAILED)
        assert run.error_code == "WORKFLOW_STEP_FAILED"
        step = db.scalars(
            select(WorkflowStepRun).where(WorkflowStepRun.run_id == run_id)
        ).first()
        assert step.status == "FAILED"
        assert step.error_code == "WORKFLOW_ORPHAN_STEP"


# --------------------------------------------------------------- engine CAS / races


def test_start_step_cas_prevents_double_task(client, device):
    """§89/§90: a second advancer on an already-RUNNING step must lose the
    CAS and never create a second Task."""
    from app.workflow.engine import WorkflowEngine
    from app.workflow.registry import WorkflowRegistry
    from app.workflow.schemas import WorkflowDefinitionIn, WorkflowStepDefinition

    with SessionLocal() as db:
        definition = WorkflowDefinitionIn(
            name="cas_case",
            version="1.0.0",
            steps=[
                WorkflowStepDefinition(
                    name="s1", command="echo", params={"message": "x"}, device_id=device["device_id"]
                ),
                WorkflowStepDefinition(
                    name="s2", command="echo", params={"message": "y"}, device_id=device["device_id"]
                ),
            ],
        )
        workflow = WorkflowRegistry(db).create(definition)
        engine = WorkflowEngine(db)
        run = engine.create_run(workflow, {}, "api", "test")
        engine.start_run(run.run_id)
        step_run = engine.get_step_runs(run.run_id)[0]
        assert step_run.status == "RUNNING"
        # second advancer: the READY->RUNNING CAS has already been taken
        assert engine.start_step(engine.get_run(run.run_id), step_run) is None
        with SessionLocal() as db2:
            from sqlalchemy import select

            tasks = db2.scalars(
                select(Task).where(Task.workflow_run_id == run.run_id)
            ).all()
            assert len(tasks) == 1  # exactly one Task for one READY->RUNNING race


# ----------------------------------------------------------------- shared helpers


def select_task(task_id: str):
    from sqlalchemy import select

    return select(Task).where(Task.task_id == task_id)


def _task_status(task_id: str) -> str | None:
    with SessionLocal() as db:
        task = db.scalars(select_task(task_id)).first()
        return task.status if task else None


def wait_for_caps(client, device_id: str, timeout: float = 10) -> bool:
    return True  # capabilities are DB-seeded in the device fixture


from app.task.db_models import TaskAttempt  # noqa: E402  (used in retry assertions)
