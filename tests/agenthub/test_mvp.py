"""AgentHub MVP acceptance (PDF §112-§121):

钉钉消息(模拟) -> MVP Agent -> Task(yingdao.audit) -> Worker -> 回复。
Covers: success path with attempt_id, unknown device, offline device,
busy device, unsupported intent.
"""

try:
    from agenthub._worker import FakeWorker, register_device, wait_for_capabilities, wait_until
except ImportError:  # pragma: no cover - depends on pytest import mode
    from _worker import FakeWorker, register_device, wait_for_capabilities, wait_until


def _send_message(client, text: str) -> str:
    res = client.post("/api/agent/message", json={"text": text, "channel": "api"})
    assert res.status_code == 200, res.text
    return res.json()["run_id"]


def _get_run(client, run_id: str) -> dict:
    res = client.get(f"/api/agent/runs/{run_id}")
    assert res.status_code == 200, res.text
    return res.json()


def _await_run(client, run_id: str, timeout: float = 30) -> dict:
    assert wait_until(
        lambda: _get_run(client, run_id)["status"] != "RUNNING", timeout=timeout
    ), f"run {run_id} did not finish in time: {_get_run(client, run_id)}"
    return _get_run(client, run_id)


def test_mvp_success_flow_with_attempt_id(client):
    """§112-§115: message -> task -> worker executes -> success reply."""
    device = register_device(client, "办公室电脑02")
    worker = FakeWorker(client, device["device_token"], capabilities=("yingdao.audit",), max_dispatches=1)
    worker.start()
    try:
        assert wait_for_capabilities(client, device["device_id"], names=("yingdao.audit",))

        run_id = _send_message(client, "运行办公室电脑02的审单")
        run = _await_run(client, run_id)

        assert run["status"] == "SUCCESS"
        assert run["ack_reply"].startswith("收到，正在启动办公室电脑02的审单任务")
        assert "已完成" in run["final_reply"]
        assert run["task_id"]

        # §35/§36: dispatch carries the full task_id/step_id/attempt_id triple
        assert len(worker.dispatches) == 1
        data = worker.dispatches[0]["data"]
        assert data["command"] == "yingdao.audit"
        assert data["attempt_id"].startswith("attempt_")

        # attempt bookkeeping on the server
        detail = client.get(f"/api/tasks/{run['task_id']}").json()
        assert detail["status"] == "SUCCESS"
        assert detail["attempts"][0]["attempt_id"] == data["attempt_id"]
        assert detail["attempts"][0]["status"] == "SUCCESS"
    finally:
        worker.stop()


def test_mvp_unknown_device(client):
    """§26: device name not registered -> friendly reply, no task created."""
    run_id = _send_message(client, "运行办公室电脑02的审单")
    run = _await_run(client, run_id)
    assert run["status"] == "FAILED"
    assert "没有找到" in run["final_reply"]
    assert run["task_id"] is None


def test_mvp_device_offline(client):
    """§27: registered but not connected -> offline reply, no task."""
    register_device(client, "办公室电脑02")
    run_id = _send_message(client, "运行办公室电脑02的审单")
    run = _await_run(client, run_id)
    assert run["status"] == "FAILED"
    assert "离线" in run["final_reply"]
    assert run["task_id"] is None


def test_mvp_device_busy(client):
    """§80/§84: another live task on the device -> busy reply, no new task."""
    device = register_device(client, "办公室电脑02")
    worker = FakeWorker(client, device["device_token"], capabilities=("yingdao.audit",), behaviour="silent")
    worker.start()
    try:
        assert wait_for_capabilities(client, device["device_id"], names=("yingdao.audit",))
        # Occupy the device with a task the worker never acknowledges.
        res = client.post(
            "/api/tasks",
            json={
                "name": "occupy",
                "target_device_id": device["device_id"],
                "steps": [{"command": "yingdao.audit", "params": {}}],
            },
        )
        assert res.status_code == 201, res.text
        occupier = res.json()["task_id"]
        assert wait_until(lambda: client.get(f"/api/tasks/{occupier}").json()["status"] == "SENT")

        run_id = _send_message(client, "运行办公室电脑02的审单")
        run = _await_run(client, run_id)
        assert run["status"] == "FAILED"
        assert "稍后再试" in run["final_reply"]
        assert run["task_id"] is None
    finally:
        worker.stop()


def test_mvp_unsupported_intent(client):
    """§21/§23: non-audit requests are refused with a hint."""
    run_id = _send_message(client, "今天天气怎么样")
    run = _await_run(client, run_id)
    assert run["status"] == "FAILED"
    assert "审单" in run["final_reply"]
    assert run["task_id"] is None
