"""ScheduledTaskStore — persists scheduled tasks and execution records.

Two backends:
  - PostgresScheduledTaskStore: PostgreSQL-backed (recommended for production)
  - JsonScheduledTaskStore: JSON-file-backed (fallback / local dev)

The factory function `create_store()` picks the backend based on config.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
import threading
import time
import uuid
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def _generate_id() -> str:
    return str(uuid.uuid4())


def create_store() -> PostgresScheduledTaskStore | JsonScheduledTaskStore:
    """Create the appropriate store backend based on configuration.

    If `scheduler.connection_string` is set (or DEER_FLOW_POSTGRES_DSN is
    available), use PostgreSQL.  Otherwise fall back to JSON file.
    """
    from deerflow.config.app_config import get_app_config

    config = get_app_config()
    extra = config.model_extra or {}
    scheduler_config = extra.get("scheduler", {})
    connection_string = scheduler_config.get("connection_string", "")

    if not connection_string:
        connection_string = os.getenv("DEER_FLOW_POSTGRES_DSN", "")

    if connection_string:
        logger.info("Scheduler store: using PostgreSQL backend")
        return PostgresScheduledTaskStore(connection_string)

    logger.info("Scheduler store: using JSON file backend (no connection_string configured)")
    return JsonScheduledTaskStore()


# ---------------------------------------------------------------------------
# JSON file backend (original implementation)
# ---------------------------------------------------------------------------


class JsonScheduledTaskStore:
    """JSON-file-backed store for scheduled tasks and execution records."""

    def __init__(self, path: str | Path | None = None) -> None:
        if path is None:
            from deerflow.config.paths import get_paths

            path = Path(get_paths().base_dir) / "scheduler" / "store.json"
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._data: dict[str, dict[str, Any]] = self._load()
        self._lock = threading.Lock()

    def _load(self) -> dict[str, dict[str, Any]]:
        if self._path.exists():
            try:
                data = json.loads(self._path.read_text(encoding="utf-8"))
                if isinstance(data, dict):
                    data.setdefault("tasks", {})
                    data.setdefault("executions", {})
                    return data
            except (json.JSONDecodeError, OSError):
                logger.warning("Corrupt scheduler store at %s, starting fresh", self._path)
        return {"tasks": {}, "executions": {}}

    def _save(self) -> None:
        fd = tempfile.NamedTemporaryFile(
            mode="w",
            dir=self._path.parent,
            suffix=".tmp",
            delete=False,
        )
        try:
            json.dump(self._data, fd, indent=2, ensure_ascii=False, default=str)
            fd.close()
            Path(fd.name).replace(self._path)
        except BaseException:
            fd.close()
            Path(fd.name).unlink(missing_ok=True)
            raise

    def create_task(
        self,
        *,
        user_id: str,
        task_name: str,
        task_type: str,
        task_prompt: str,
        schedule_type: str,
        schedule_config: dict[str, Any] | None = None,
        timezone: str = "Asia/Shanghai",
        notify_channels: list[str] | None = None,
        notify_config: dict[str, Any] | None = None,
        thread_id: str | None = None,
    ) -> dict[str, Any]:
        now = time.time()
        task_id = _generate_id()
        task: dict[str, Any] = {
            "id": task_id,
            "user_id": user_id,
            "thread_id": thread_id,
            "task_name": task_name,
            "task_type": task_type,
            "task_prompt": task_prompt,
            "schedule_type": schedule_type,
            "schedule_config": schedule_config or {},
            "timezone": timezone,
            "notify_channels": notify_channels or [],
            "notify_config": notify_config or {},
            "status": "active",
            "last_run_at": None,
            "next_run_at": None,
            "created_at": now,
            "updated_at": now,
        }
        with self._lock:
            self._data["tasks"][task_id] = task
            self._save()
        logger.info("Created scheduled task %s: %s", task_id, task_name)
        return dict(task)

    def get_task(self, task_id: str) -> dict[str, Any] | None:
        with self._lock:
            task = self._data["tasks"].get(task_id)
            return dict(task) if task else None

    def list_tasks(
        self,
        *,
        user_id: str | None = None,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        with self._lock:
            tasks = list(self._data["tasks"].values())
        results = []
        for t in tasks:
            if user_id and t.get("user_id") != user_id:
                continue
            if status and t.get("status") != status:
                continue
            results.append(dict(t))
        results.sort(key=lambda x: x.get("created_at", 0), reverse=True)
        return results

    def update_task(self, task_id: str, **updates: Any) -> dict[str, Any] | None:
        with self._lock:
            task = self._data["tasks"].get(task_id)
            if task is None:
                return None
            for key, value in updates.items():
                if key in ("id", "created_at"):
                    continue
                task[key] = value
            task["updated_at"] = time.time()
            self._save()
        return dict(task)

    def delete_task(self, task_id: str) -> bool:
        with self._lock:
            task = self._data["tasks"].get(task_id)
            if task is None:
                return False
            task["status"] = "deleted"
            task["updated_at"] = time.time()
            self._save()
        return True

    def get_due_tasks(self) -> list[dict[str, Any]]:
        now = time.time()
        with self._lock:
            tasks = list(self._data["tasks"].values())
        return [dict(t) for t in tasks if t.get("status") == "active" and t.get("next_run_at") is not None and t["next_run_at"] <= now]

    def create_execution(self, task_id: str) -> dict[str, Any]:
        now = time.time()
        execution_id = _generate_id()
        execution: dict[str, Any] = {
            "id": execution_id,
            "task_id": task_id,
            "started_at": now,
            "finished_at": None,
            "status": "running",
            "error_message": None,
            "result_content": None,
            "notify_status": None,
            "created_at": now,
        }
        with self._lock:
            self._data["executions"][execution_id] = execution
            self._save()
        return dict(execution)

    def update_execution(self, execution_id: str, **updates: Any) -> dict[str, Any] | None:
        with self._lock:
            execution = self._data["executions"].get(execution_id)
            if execution is None:
                return None
            for key, value in updates.items():
                if key in ("id", "created_at"):
                    continue
                execution[key] = value
            self._save()
        return dict(execution)

    def list_executions(
        self,
        task_id: str,
        *,
        limit: int = 20,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        with self._lock:
            executions = list(self._data["executions"].values())
        results = [dict(e) for e in executions if e.get("task_id") == task_id]
        results.sort(key=lambda x: x.get("created_at", 0), reverse=True)
        return results[offset : offset + limit]

    def cleanup_stale_executions(self, max_age_seconds: int = 3600) -> int:
        now = time.time()
        cleaned = 0
        with self._lock:
            for execution in self._data["executions"].values():
                if execution.get("status") != "running":
                    continue
                started = execution.get("started_at", 0)
                if now - started > max_age_seconds:
                    execution["status"] = "failed"
                    execution["finished_at"] = now
                    execution["error_message"] = "Execution timed out (stale running record cleaned up)"
                    cleaned += 1
            if cleaned:
                self._save()
        return cleaned


# ---------------------------------------------------------------------------
# PostgreSQL backend
# ---------------------------------------------------------------------------


class PostgresScheduledTaskStore:
    """PostgreSQL-backed store for scheduled tasks and execution records.

    Follows the same pattern as PostgresMemoryStorage: raw psycopg SQL,
    auto-creates tables on first use, thread-safe via connection-per-call.
    """

    _TASKS_TABLE = "scheduler_tasks"
    _EXECUTIONS_TABLE = "scheduler_executions"

    def __init__(self, connection_string: str) -> None:
        self._connection_string = connection_string
        try:
            import psycopg
        except ImportError as exc:
            raise ImportError("psycopg is required for PostgresScheduledTaskStore. Install it with: uv add psycopg[binary]") from exc
        self._psycopg = psycopg
        self._init_lock = threading.Lock()
        self._initialized = False

    def _connect(self):
        return self._psycopg.connect(self._connection_string, autocommit=True)

    def _ensure_schema(self, conn) -> None:
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._TASKS_TABLE} (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL DEFAULT 'default',
                thread_id TEXT,
                task_name TEXT NOT NULL,
                task_type TEXT NOT NULL DEFAULT 'custom_query',
                task_prompt TEXT NOT NULL,
                schedule_type TEXT NOT NULL,
                schedule_config JSONB NOT NULL DEFAULT '{{}}',
                timezone TEXT NOT NULL DEFAULT 'Asia/Shanghai',
                notify_channels JSONB NOT NULL DEFAULT '[]',
                notify_config JSONB NOT NULL DEFAULT '{{}}',
                status TEXT NOT NULL DEFAULT 'active',
                last_run_at DOUBLE PRECISION,
                next_run_at DOUBLE PRECISION,
                created_at DOUBLE PRECISION NOT NULL,
                updated_at DOUBLE PRECISION NOT NULL
            )
            """
        )
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{self._TASKS_TABLE}_status ON {self._TASKS_TABLE} (status)")
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{self._TASKS_TABLE}_next_run ON {self._TASKS_TABLE} (next_run_at) WHERE status = 'active'")
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._EXECUTIONS_TABLE} (
                id TEXT PRIMARY KEY,
                task_id TEXT NOT NULL REFERENCES {self._TASKS_TABLE}(id) ON DELETE CASCADE,
                started_at DOUBLE PRECISION NOT NULL,
                finished_at DOUBLE PRECISION,
                status TEXT NOT NULL DEFAULT 'running',
                error_message TEXT,
                result_content TEXT,
                notify_status JSONB,
                created_at DOUBLE PRECISION NOT NULL
            )
            """
        )
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{self._EXECUTIONS_TABLE}_task_id ON {self._EXECUTIONS_TABLE} (task_id)")
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{self._EXECUTIONS_TABLE}_status ON {self._EXECUTIONS_TABLE} (status)")

    def _ensure_initialized(self) -> None:
        if self._initialized:
            return
        with self._init_lock:
            if self._initialized:
                return
            with self._connect() as conn:
                self._ensure_schema(conn)
            self._initialized = True
            logger.info("PostgresScheduledTaskStore schema initialized")

    @staticmethod
    def _row_to_task(row) -> dict[str, Any]:
        return {
            "id": row[0],
            "user_id": row[1],
            "thread_id": row[2],
            "task_name": row[3],
            "task_type": row[4],
            "task_prompt": row[5],
            "schedule_type": row[6],
            "schedule_config": row[7] if isinstance(row[7], dict) else (json.loads(row[7]) if row[7] else {}),
            "timezone": row[8],
            "notify_channels": row[9] if isinstance(row[9], list) else (json.loads(row[9]) if row[9] else []),
            "notify_config": row[10] if isinstance(row[10], dict) else (json.loads(row[10]) if row[10] else {}),
            "status": row[11],
            "last_run_at": row[12],
            "next_run_at": row[13],
            "created_at": row[14],
            "updated_at": row[15],
        }

    @staticmethod
    def _row_to_execution(row) -> dict[str, Any]:
        return {
            "id": row[0],
            "task_id": row[1],
            "started_at": row[2],
            "finished_at": row[3],
            "status": row[4],
            "error_message": row[5],
            "result_content": row[6],
            "notify_status": row[7] if isinstance(row[7], dict) else (json.loads(row[7]) if row[7] else None),
            "created_at": row[8],
        }

    # -- task CRUD ----------------------------------------------------------

    def create_task(
        self,
        *,
        user_id: str,
        task_name: str,
        task_type: str,
        task_prompt: str,
        schedule_type: str,
        schedule_config: dict[str, Any] | None = None,
        timezone: str = "Asia/Shanghai",
        notify_channels: list[str] | None = None,
        notify_config: dict[str, Any] | None = None,
        thread_id: str | None = None,
    ) -> dict[str, Any]:
        self._ensure_initialized()
        now = time.time()
        task_id = _generate_id()
        with self._connect() as conn:
            conn.execute(
                f"""
                INSERT INTO {self._TASKS_TABLE}
                    (id, user_id, thread_id, task_name, task_type, task_prompt,
                     schedule_type, schedule_config, timezone, notify_channels,
                     notify_config, status, last_run_at, next_run_at, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s::jsonb, %s::jsonb, %s, NULL, NULL, %s, %s)
                """,
                (
                    task_id,
                    user_id,
                    thread_id,
                    task_name,
                    task_type,
                    task_prompt,
                    schedule_type,
                    json.dumps(schedule_config or {}),
                    timezone,
                    json.dumps(notify_channels or []),
                    json.dumps(notify_config or {}),
                    "active",
                    now,
                    now,
                ),
            )
        logger.info("Created scheduled task %s: %s", task_id, task_name)
        return self.get_task(task_id)  # type: ignore[return-value]

    def get_task(self, task_id: str) -> dict[str, Any] | None:
        self._ensure_initialized()
        with self._connect() as conn:
            row = conn.execute(
                f"SELECT * FROM {self._TASKS_TABLE} WHERE id = %s",
                (task_id,),
            ).fetchone()
        return self._row_to_task(row) if row else None

    def list_tasks(
        self,
        *,
        user_id: str | None = None,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        self._ensure_initialized()
        conditions = []
        params: list[Any] = []
        if user_id:
            conditions.append("user_id = %s")
            params.append(user_id)
        if status:
            conditions.append("status = %s")
            params.append(status)
        where = f" WHERE {' AND '.join(conditions)}" if conditions else ""
        with self._connect() as conn:
            rows = conn.execute(
                f"SELECT * FROM {self._TASKS_TABLE}{where} ORDER BY created_at DESC",
                params,
            ).fetchall()
        return [self._row_to_task(r) for r in rows]

    def update_task(self, task_id: str, **updates: Any) -> dict[str, Any] | None:
        self._ensure_initialized()
        if not updates:
            return self.get_task(task_id)
        updates["updated_at"] = time.time()
        set_parts = []
        params: list[Any] = []
        json_fields = {"schedule_config", "notify_channels", "notify_config"}
        for key, value in updates.items():
            if key in ("id", "created_at"):
                continue
            if key in json_fields:
                set_parts.append(f"{key} = %s::jsonb")
                params.append(json.dumps(value))
            else:
                set_parts.append(f"{key} = %s")
                params.append(value)
        if not set_parts:
            return self.get_task(task_id)
        params.append(task_id)
        with self._connect() as conn:
            conn.execute(
                f"UPDATE {self._TASKS_TABLE} SET {', '.join(set_parts)} WHERE id = %s",
                params,
            )
        return self.get_task(task_id)

    def delete_task(self, task_id: str) -> bool:
        self._ensure_initialized()
        with self._connect() as conn:
            n = conn.execute(
                f"UPDATE {self._TASKS_TABLE} SET status = 'deleted', updated_at = %s WHERE id = %s",
                (time.time(), task_id),
            ).rowcount
        return n > 0

    def get_due_tasks(self) -> list[dict[str, Any]]:
        self._ensure_initialized()
        now = time.time()
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT * FROM {self._TASKS_TABLE}
                WHERE status = 'active' AND next_run_at IS NOT NULL AND next_run_at <= %s
                """,
                (now,),
            ).fetchall()
        return [self._row_to_task(r) for r in rows]

    # -- execution CRUD -----------------------------------------------------

    def create_execution(self, task_id: str) -> dict[str, Any]:
        self._ensure_initialized()
        now = time.time()
        execution_id = _generate_id()
        with self._connect() as conn:
            conn.execute(
                f"""
                INSERT INTO {self._EXECUTIONS_TABLE}
                    (id, task_id, started_at, finished_at, status, error_message,
                     result_content, notify_status, created_at)
                VALUES (%s, %s, %s, NULL, 'running', NULL, NULL, NULL, %s)
                """,
                (execution_id, task_id, now, now),
            )
        return dict(
            id=execution_id,
            task_id=task_id,
            started_at=now,
            finished_at=None,
            status="running",
            error_message=None,
            result_content=None,
            notify_status=None,
            created_at=now,
        )

    def update_execution(self, execution_id: str, **updates: Any) -> dict[str, Any] | None:
        self._ensure_initialized()
        if not updates:
            return self._get_execution_by_id(execution_id)
        set_parts = []
        params: list[Any] = []
        json_fields = {"notify_status"}
        for key, value in updates.items():
            if key in ("id", "created_at"):
                continue
            if key in json_fields:
                set_parts.append(f"{key} = %s::jsonb")
                params.append(json.dumps(value) if value is not None else None)
            else:
                set_parts.append(f"{key} = %s")
                params.append(value)
        if not set_parts:
            return self._get_execution_by_id(execution_id)
        params.append(execution_id)
        with self._connect() as conn:
            conn.execute(
                f"UPDATE {self._EXECUTIONS_TABLE} SET {', '.join(set_parts)} WHERE id = %s",
                params,
            )
        return self._get_execution_by_id(execution_id)

    def _get_execution_by_id(self, execution_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                f"SELECT * FROM {self._EXECUTIONS_TABLE} WHERE id = %s",
                (execution_id,),
            ).fetchone()
        return self._row_to_execution(row) if row else None

    def list_executions(
        self,
        task_id: str,
        *,
        limit: int = 20,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        self._ensure_initialized()
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT * FROM {self._EXECUTIONS_TABLE}
                WHERE task_id = %s
                ORDER BY created_at DESC
                LIMIT %s OFFSET %s
                """,
                (task_id, limit, offset),
            ).fetchall()
        return [self._row_to_execution(r) for r in rows]

    def cleanup_stale_executions(self, max_age_seconds: int = 3600) -> int:
        self._ensure_initialized()
        now = time.time()
        cutoff = now - max_age_seconds
        with self._connect() as conn:
            n = conn.execute(
                f"""
                UPDATE {self._EXECUTIONS_TABLE}
                SET status = 'failed',
                    finished_at = %s,
                    error_message = 'Execution timed out (stale running record cleaned up)'
                WHERE status = 'running' AND started_at < %s
                """,
                (now, cutoff),
            ).rowcount
        if n:
            logger.info("Cleaned up %d stale execution(s)", n)
        return n


# Backward compatibility alias
ScheduledTaskStore = JsonScheduledTaskStore
