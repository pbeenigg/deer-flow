"""ScheduledTaskStore — persists scheduled tasks and execution records.

Uses the same JSON-file-backed pattern as ChannelStore for consistency
with the existing codebase. For production workloads with high concurrency,
this can be swapped for a proper database backend.
"""

from __future__ import annotations

import json
import logging
import tempfile
import threading
import time
import uuid
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def _generate_id() -> str:
    return str(uuid.uuid4())


class ScheduledTaskStore:
    """JSON-file-backed store for scheduled tasks and execution records.

    Data layout (on disk)::

        {
            "tasks": {
                "<task_id>": {
                    "id": "<uuid>",
                    "user_id": "...",
                    "thread_id": "...",
                    "task_name": "...",
                    "task_type": "news_digest",
                    "task_prompt": "...",
                    "schedule_type": "daily",
                    "schedule_config": {...},
                    "timezone": "Asia/Shanghai",
                    "notify_channels": ["email"],
                    "notify_config": {...},
                    "status": "active",
                    "last_run_at": null,
                    "next_run_at": null,
                    "created_at": 1700000000.0,
                    "updated_at": 1700000000.0
                }
            },
            "executions": {
                "<execution_id>": {
                    "id": "<uuid>",
                    "task_id": "<uuid>",
                    "started_at": 1700000000.0,
                    "finished_at": null,
                    "status": "running",
                    "error_message": null,
                    "result_content": null,
                    "notify_status": null,
                    "created_at": 1700000000.0
                }
            }
        }
    """

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
        """Create a new scheduled task and return its record."""
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
        """Get a task by ID. Returns a copy or None."""
        with self._lock:
            task = self._data["tasks"].get(task_id)
            return dict(task) if task else None

    def list_tasks(
        self,
        *,
        user_id: str | None = None,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        """List tasks, optionally filtered by user_id and/or status."""
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
        """Update a task's fields. Returns the updated task or None."""
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
        """Soft-delete a task by setting status to 'deleted'."""
        with self._lock:
            task = self._data["tasks"].get(task_id)
            if task is None:
                return False
            task["status"] = "deleted"
            task["updated_at"] = time.time()
            self._save()
        return True

    def get_due_tasks(self) -> list[dict[str, Any]]:
        """Get all active tasks whose next_run_at <= now."""
        now = time.time()
        with self._lock:
            tasks = list(self._data["tasks"].values())
        return [dict(t) for t in tasks if t.get("status") == "active" and t.get("next_run_at") is not None and t["next_run_at"] <= now]

    # -- execution CRUD -----------------------------------------------------

    def create_execution(self, task_id: str) -> dict[str, Any]:
        """Create a new execution record for a task."""
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
        """Update an execution record. Returns the updated record or None."""
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
        """List execution records for a task, sorted by creation time desc."""
        with self._lock:
            executions = list(self._data["executions"].values())
        results = [dict(e) for e in executions if e.get("task_id") == task_id]
        results.sort(key=lambda x: x.get("created_at", 0), reverse=True)
        return results[offset : offset + limit]
