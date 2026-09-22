"""ETA predictor (V1.7 §32/§40-§42).

    ETA = queue_wait + transfer_time + startup_time + predicted_runtime

Priority order for the runtime estimate (§33/§38):
    real history > environment/capability baseline > static estimate
History weight grows with sample count (§36: confidence blending).
"""

from dataclasses import dataclass, field

from app.core.config import settings
from app.execution.history import DeviceRuntimeStats


@dataclass
class ExecutionPath:
    device_id: str
    predicted_runtime_sec: float
    queue_wait_sec: float
    transfer_time_sec: float
    startup_time_sec: float
    estimated_completion_sec: float
    confidence: float
    reasons: list[str] = field(default_factory=list)

    def explain(self) -> str:
        return (
            f"ETA {self.estimated_completion_sec:.0f}s = queue {self.queue_wait_sec:.0f}s"
            f" + transfer {self.transfer_time_sec:.0f}s + startup {self.startup_time_sec:.0f}s"
            f" + runtime {self.predicted_runtime_sec:.0f}s (confidence {self.confidence:.2f})"
        )


def predicted_runtime(
    stats: DeviceRuntimeStats | None,
    baseline_sec: float | None,
    input_mb: float,
) -> tuple[float, float, list[str]]:
    """Blend history with the capability baseline by sample confidence.
    Returns (predicted_runtime_sec, confidence, reasons)."""
    reasons: list[str] = []
    baseline = float(baseline_sec) if baseline_sec and baseline_sec > 0 else 600.0
    if stats is None or stats.count == 0:
        reasons.append("cold-start-baseline")
        return baseline, 0.0, reasons

    weight = min(1.0, stats.count / 5.0)
    reasons.append(f"history-{stats.count}-runs")
    estimate = stats.mean_duration_sec
    # per-MB scaling when both sides know the input size (§35)
    if stats.mean_duration_per_mb and input_mb >= 1:
        scaled = stats.mean_duration_per_mb * input_mb
        reasons.append(f"per-mb-rate-{stats.mean_duration_per_mb:.1f}s/MB")
        estimate = scaled if stats.count >= 3 else (estimate + scaled) / 2
    blended = weight * estimate + (1.0 - weight) * baseline
    return blended, 0.9 * weight, reasons


def queue_wait_sec(live_tasks: int, remaining_timeout_sec: float | None) -> tuple[float, list[str]]:
    """§39: a BUSY worker's ETA must include its current work. max_concurrency
    is 1 (GUI/RPA sessions), so one live task = the queue depth."""
    reasons: list[str] = []
    if live_tasks <= 0:
        return 0.0, reasons
    reasons.append("worker-busy")
    if remaining_timeout_sec and remaining_timeout_sec > 0:
        reasons.append("queue-wait-from-timeout")
        return min(remaining_timeout_sec, settings.task_offline_max_wait), reasons
    return 300.0, reasons  # no deadline info: bounded pessimism


def transfer_time_sec(input_mb: float) -> tuple[float, list[str]]:
    """§41: data movement cost. Throughput is a conservative fleet constant in
    V1 (per-device measured throughput arrives with execution metrics later)."""
    if input_mb <= 0:
        return 0.0, []
    mbps = max(1.0, float(settings.execution_transfer_mbps))
    return input_mb / mbps, [f"transfer-{input_mb:.0f}MB@{mbps:.0f}MB/s"]


def predict_eta(
    device_id: str,
    stats: DeviceRuntimeStats | None,
    baseline_sec: float | None,
    input_mb: float,
    live_tasks: int,
    remaining_timeout_sec: float | None,
) -> ExecutionPath:
    runtime, confidence, reasons = predicted_runtime(stats, baseline_sec, input_mb)
    wait, wait_reasons = queue_wait_sec(live_tasks, remaining_timeout_sec)
    transfer, transfer_reasons = transfer_time_sec(input_mb)
    startup = float(settings.execution_startup_sec)
    reasons.extend(wait_reasons)
    reasons.extend(transfer_reasons)
    if stats is None or stats.count == 0:
        confidence = 0.0
    reasons.append(f"startup-{startup:.0f}s")
    total = wait + transfer + startup + runtime
    return ExecutionPath(
        device_id=device_id,
        predicted_runtime_sec=runtime,
        queue_wait_sec=wait,
        transfer_time_sec=transfer,
        startup_time_sec=startup,
        estimated_completion_sec=total,
        confidence=confidence,
        reasons=reasons,
    )
