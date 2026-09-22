"""Execution Plane (V1.7 §30-§43): deterministic execution path selection.

NOT a new Task Engine and NOT an "AI scheduler" - a deterministic estimator
plus resolver that answers "which READY worker should run THIS task?" from:

    hard filters -> historical runtime -> config baseline fallback ->
    transfer cost -> current worker state -> ETA -> min(ETA) -> reasons
"""

from app.execution.history import ExecutionHistory
from app.execution.predictor import ExecutionPath, predict_eta

__all__ = ["ExecutionHistory", "ExecutionPath", "predict_eta"]
