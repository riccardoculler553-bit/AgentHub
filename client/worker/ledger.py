"""ExecutionLedger: local SQLite journal of task attempts (PDF §76-§77).

Why: if the RPA ran to completion but the WSS died before task.result left
the machine, the server must never re-run the job. The ledger answers two
questions:

- idempotency: "have I seen (task_id, attempt_id) before?" (PDF §75)
- recovery:   "which finished results were never acknowledged?" -> re-report
              on reconnect (PDF §110)

DB file: ~/.devicelink/worker.db (respects DEVICELINK_HOME).
"""

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any

import storage

_SCHEMA = """
CREATE TABLE IF NOT EXISTS executions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL,
    step_id TEXT NOT NULL DEFAULT '',
    attempt_id TEXT NOT NULL UNIQUE,
    command TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL,
    result TEXT,
    error_code TEXT,
    error_message TEXT,
    reported INTEGER NOT NULL DEFAULT 0,
    started_at TEXT,
    finished_at TEXT
);
CREATE INDEX IF NOT EXISTS ix_executions_reported ON executions(reported, status);
"""

TERMINAL_STATUSES = {"SUCCESS", "FAILED", "TIMEOUT", "CANCELLED"}


class ExecutionLedger:
    def __init__(self, path: Path | None = None) -> None:
        if path is None:
            path = storage.identity_dir() / "worker.db"
        path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        # check_same_thread=False: the consumer loop and connect callbacks may
        # touch the ledger from different threads (tests use portals).
        self._conn = sqlite3.connect(str(path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.executescript(_SCHEMA)
            self._conn.commit()

    # ------------------------------------------------------------------ claims

    def claim(self, task_id: str, step_id: str, attempt_id: str, command: str) -> str | None:
        """Reserve the attempt. Returns the EXISTING status when this
        attempt_id was already claimed (duplicate dispatch / late retry);
        returns None when the claim is fresh and execution may start."""
        now = _now()
        with self._lock:
            row = self._conn.execute(
                "SELECT status FROM executions WHERE attempt_id = ?", (attempt_id,)
            ).fetchone()
            if row is not None:
                return str(row["status"])
            self._conn.execute(
                "INSERT INTO executions (task_id, step_id, attempt_id, command, status, started_at)"
                " VALUES (?, ?, ?, ?, 'ACCEPTED', ?)",
                (task_id, step_id, attempt_id, command, now),
            )
            self._conn.commit()
            return None

    # ----------------------------------------------------------------- updates

    def mark_running(self, attempt_id: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE executions SET status = 'RUNNING' WHERE attempt_id = ?", (attempt_id,)
            )
            self._conn.commit()

    def mark_finished(
        self,
        attempt_id: str,
        status: str,
        *,
        result: dict[str, Any] | None = None,
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> None:
        if status not in TERMINAL_STATUSES:
            raise ValueError(f"not a terminal status: {status}")
        with self._lock:
            self._conn.execute(
                "UPDATE executions SET status = ?, result = ?, error_code = ?, error_message = ?,"
                " finished_at = ? WHERE attempt_id = ?",
                (
                    status,
                    json.dumps(result, ensure_ascii=False) if result is not None else None,
                    error_code,
                    error_message,
                    _now(),
                    attempt_id,
                ),
            )
            self._conn.commit()

    def mark_reported(self, attempt_id: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE executions SET reported = 1 WHERE attempt_id = ?", (attempt_id,)
            )
            self._conn.commit()

    # ---------------------------------------------------------------- recovery

    def get(self, attempt_id: str) -> dict | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM executions WHERE attempt_id = ?", (attempt_id,)
            ).fetchone()
        return dict(row) if row is not None else None

    def unreported_results(self) -> list[dict]:
        """Terminal results that never got a transport ACK - re-report on
        reconnect (PDF §110)."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM executions WHERE reported = 0 AND status IN ('SUCCESS','FAILED','TIMEOUT','CANCELLED')"
            ).fetchall()
        return [dict(r) for r in rows]

    def fail_running(self, reason: str = "worker_restarted") -> int:
        """Process startup: attempts left RUNNING by a previous process are
        dead (their executors are gone); park them as FAILED so they are
        reported instead of blocking future claims."""
        with self._lock:
            cur = self._conn.execute(
                "UPDATE executions SET status = 'FAILED', error_code = 'WORKER_RESTARTED',"
                " error_message = ?, finished_at = ? WHERE status IN ('ACCEPTED','RUNNING')",
                (reason, _now()),
            )
            self._conn.commit()
            return cur.rowcount

    def close(self) -> None:
        with self._lock:
            self._conn.close()


def _now() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat(timespec="seconds")
