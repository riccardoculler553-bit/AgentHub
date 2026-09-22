"""V1.7 Execution Path Resolver tests (§30-§43).

Deterministic estimator: cold start falls back to the capability baseline
(config estimated_duration_sec), real history dominates as samples grow,
a BUSY worker's ETA includes its queue wait, and min(ETA) wins with a
stable device_id tie-break.
"""

import uuid
from datetime import timedelta

from app.db.database import SessionLocal
from app.db.models import utcnow
from app.execution.predictor import predict_eta
from app.task.db_models import Task, TaskAttempt

from ._worker import FakeWorker, register_device, wait_for_worker_capabilities


def test_cold_start_uses_baseline():
    path = predict_eta("pc-1", None, 600, 0, 0, None)
    assert path.predicted_runtime_sec == 600
    assert path.confidence == 0.0
    assert "cold-start-baseline" in path.reasons
    assert path.estimated_completion_sec == 600 + path.startup_time_sec


def test_history_overrides_baseline_as_confidence_grows():
    stats = type("S", (), {"count": 10, "mean_duration_sec": 420.0, "mean_duration_per_mb": None})()
    path = predict_eta("pc-2", stats, 600, 0, 0, None)
    assert path.predicted_runtime_sec == 420  # >=5 samples: history dominates
    assert path.confidence == 0.9
    assert "history-10-runs" in path.reasons

    one_run = type("S", (), {"count": 1, "mean_duration_sec": 420.0, "mean_duration_per_mb": None})()
    blended = predict_eta("pc-2", one_run, 600, 0, 0, None)
    # 1 sample: 0.2 weight -> blend toward the baseline, not a blind trust
    assert 420 < blended.predicted_runtime_sec < 600


def test_busy_worker_queue_wait_uses_remaining_timeout():
    idle = predict_eta("pc-1", None, 600, 0, 0, None)
    busy = predict_eta("pc-2", None, 600, 0, 1, 400)
    assert idle.estimated_completion_sec < busy.estimated_completion_sec
    assert "worker-busy" in busy.reasons
    assert busy.queue_wait_sec == 400


def test_busy_worker_queue_wait_is_bounded():
    # no deadline info / huge remaining -> bounded pessimism, not infinity
    capped = predict_eta("pc-2", None, 600, 0, 1, 100_000)
    assert capped.queue_wait_sec == capped.predicted_runtime_sec * 0 + 600


def test_transfer_cost_scales_with_input_size():
    small = predict_eta("pc-1", None, 600, 100, 0, None)
    big = predict_eta("pc-1", None, 600, 2000, 0, None)
    assert big.transfer_time_sec > small.transfer_time_sec
    assert "transfer-" in " ".join(big.reasons)


def test_resolver_picks_min_eta_with_reasons(client):
    from app.capability_runtime.db_models import AutomationCapability, CapabilityVersion, CapabilityPackage
    from app.capability_runtime.resolver import CapabilityResolver

    device = register_device(client, "路径机A")
    # the resolver only selects workers advertising the capability (V1.6 0.10)
    # "silent" keeps the WS session open (caps_only disconnects immediately),
    # so the device is online for the resolver while the test seeds history.
    worker = FakeWorker(
        client, device["device_token"], behaviour="silent",
        installed_capabilities=[{"name": "exec.path.demo", "version": "1.0.0"}],
        environment={"fingerprint": "fp-exec", "python": ["3.11"]},
    )
    worker.start()
    try:
        assert wait_for_worker_capabilities(client, device["device_id"], ("exec.path.demo",))
        with SessionLocal() as db:
            cap = AutomationCapability(name="exec.path.demo", runtime_type="PYTHON", current_version="1.0.0")
            pkg = CapabilityPackage(
                package_id=f"pkg_{uuid.uuid4().hex[:8]}", name=cap.name, version="1.0.0",
                storage_path="capability_packages/x.zip", size=1, checksum="c",
                runtime_type="PYTHON", created_at=utcnow(),
            )
            version_row = CapabilityVersion(
                capability_name=cap.name, version="1.0.0", package_id=pkg.package_id,
                status="PUBLISHED", entrypoint="main", checksum="c", config={
                    "resources": {"estimated_duration_sec": 300},
                },
                created_at=utcnow(),
            )
            db.add_all([cap, pkg, version_row])
            db.commit()

            # seed history: this device ran the capability fast 8 times
            for _ in range(8):
                task_id = f"task_{uuid.uuid4().hex[:10]}"
                db.add(Task(
                    task_id=task_id, name="h", target_device_id=device["device_id"],
                    status="SUCCESS", execution_type="CAPABILITY", capability_name=cap.name,
                    capability_version="1.0.0", created_at=utcnow(),
                ))
                db.add(TaskAttempt(
                    attempt_id=f"attempt_{uuid.uuid4().hex[:10]}", task_id=task_id,
                    step_id=f"step_{uuid.uuid4().hex[:8]}", device_id=device["device_id"],
                    status="SUCCESS", started_at=utcnow(), finished_at=utcnow() + timedelta(seconds=120),
                    created_at=utcnow(),
                ))
            db.commit()

            from app.main import app as fastapi_app

            hub = fastapi_app.state.hub
            path = CapabilityResolver(db, hub).resolve_execution_path(version_row, cap.name)
        # history says 120s: the ETA reflects it, not the 300s baseline
        assert path.device_id == device["device_id"]
        assert path.predicted_runtime_sec == 120
        assert "history-8-runs" in path.reasons
        assert path.explain().startswith("ETA ")
    finally:
        worker.stop()
