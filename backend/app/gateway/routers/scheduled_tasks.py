"""Gateway router for scheduled task management."""

from __future__ import annotations

import logging
import time
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from app.scheduler.scheduler import compute_next_run_time
from app.scheduler.store import ScheduledTaskStore

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/scheduled-tasks", tags=["scheduled-tasks"])


def _get_store(request: Request) -> ScheduledTaskStore:
    """Get the ScheduledTaskStore from the scheduler service."""
    from app.scheduler.service import get_scheduler_service

    service = get_scheduler_service()
    if service is None:
        raise HTTPException(status_code=503, detail="Scheduler service is not available")
    return service.store


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------


class ScheduleConfig(BaseModel):
    type: str = Field(description="Schedule type: once, daily, weekly, interval, cron")
    hour: int | None = Field(default=None, description="Hour (0-23)")
    minute: int | None = Field(default=None, description="Minute (0-59)")
    day_of_week: str | None = Field(default=None, description="Day of week (for weekly)")
    minutes: int | None = Field(default=None, description="Interval in minutes (for interval)")
    expression: str | None = Field(default=None, description="Cron expression (for cron)")


class TaskCreateRequest(BaseModel):
    task_name: str = Field(description="Task name")
    task_type: str = Field(default="custom_query", description="Task type: news_digest, custom_query")
    task_prompt: str = Field(description="Prompt to execute")
    schedule: ScheduleConfig = Field(description="Schedule configuration")
    timezone: str = Field(default="Asia/Shanghai", description="Timezone")
    notify_channels: list[str] = Field(default_factory=lambda: ["email"], description="Notification channels")
    notify_config: dict[str, Any] = Field(default_factory=dict, description="Channel-specific config")
    user_id: str = Field(default="default", description="User ID")
    thread_id: str | None = Field(default=None, description="Existing thread ID to reuse")


class TaskUpdateRequest(BaseModel):
    task_name: str | None = None
    task_prompt: str | None = None
    schedule: ScheduleConfig | None = None
    timezone: str | None = None
    notify_channels: list[str] | None = None
    notify_config: dict[str, Any] | None = None


class TaskStatusRequest(BaseModel):
    status: str = Field(description="New status: active, paused")


class TaskResponse(BaseModel):
    task_id: str
    task_name: str
    task_type: str
    task_prompt: str
    schedule_type: str
    schedule_config: dict[str, Any]
    timezone: str
    notify_channels: list[str]
    notify_config: dict[str, Any]
    status: str
    last_run_at: float | None = None
    next_run_at: float | None = None
    thread_id: str | None = None
    created_at: float
    updated_at: float


class ExecutionResponse(BaseModel):
    id: str
    task_id: str
    started_at: float
    finished_at: float | None = None
    status: str
    error_message: str | None = None
    result_content: str | None = None
    notify_status: dict[str, Any] | None = None
    created_at: float


class NotifyTestRequest(BaseModel):
    channel: str = Field(description="Channel to test")
    config: dict[str, Any] = Field(description="Channel config")
    test_message: str = Field(default="This is a test message from DeerFlow", description="Test message content")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _task_to_response(task: dict[str, Any]) -> TaskResponse:
    return TaskResponse(
        task_id=task["id"],
        task_name=task.get("task_name", ""),
        task_type=task.get("task_type", ""),
        task_prompt=task.get("task_prompt", ""),
        schedule_type=task.get("schedule_type", ""),
        schedule_config=task.get("schedule_config", {}),
        timezone=task.get("timezone", "Asia/Shanghai"),
        notify_channels=task.get("notify_channels", []),
        notify_config=task.get("notify_config", {}),
        status=task.get("status", ""),
        last_run_at=task.get("last_run_at"),
        next_run_at=task.get("next_run_at"),
        thread_id=task.get("thread_id"),
        created_at=task.get("created_at", 0),
        updated_at=task.get("updated_at", 0),
    )


def _execution_to_response(execution: dict[str, Any]) -> ExecutionResponse:
    return ExecutionResponse(
        id=execution["id"],
        task_id=execution.get("task_id", ""),
        started_at=execution.get("started_at", 0),
        finished_at=execution.get("finished_at"),
        status=execution.get("status", ""),
        error_message=execution.get("error_message"),
        result_content=execution.get("result_content"),
        notify_status=execution.get("notify_status"),
        created_at=execution.get("created_at", 0),
    )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post("", response_model=TaskResponse)
async def create_task(body: TaskCreateRequest, request: Request) -> TaskResponse:
    """Create a new scheduled task."""
    store = _get_store(request)

    schedule_dict = body.schedule.model_dump(exclude_none=True)
    schedule_type = schedule_dict.pop("type")

    task = store.create_task(
        user_id=body.user_id,
        task_name=body.task_name,
        task_type=body.task_type,
        task_prompt=body.task_prompt,
        schedule_type=schedule_type,
        schedule_config=schedule_dict,
        timezone=body.timezone,
        notify_channels=body.notify_channels,
        notify_config=body.notify_config,
        thread_id=body.thread_id,
    )

    next_run = compute_next_run_time(
        schedule_type,
        schedule_dict,
        body.timezone,
    )
    if next_run is not None:
        store.update_task(task["id"], next_run_at=next_run)
        task["next_run_at"] = next_run
    else:
        store.update_task(task["id"], status="completed")
        task["status"] = "completed"

    return _task_to_response(store.get_task(task["id"]) or task)


@router.get("", response_model=list[TaskResponse])
async def list_tasks(
    request: Request,
    user_id: str | None = None,
    status: str | None = None,
) -> list[TaskResponse]:
    """List scheduled tasks, optionally filtered."""
    store = _get_store(request)
    tasks = store.list_tasks(user_id=user_id, status=status)
    return [_task_to_response(t) for t in tasks]


@router.get("/status")
async def get_scheduler_status(request: Request) -> dict:
    """Get the scheduler service status."""
    from app.scheduler.service import get_scheduler_service

    service = get_scheduler_service()
    if service is None:
        return {"enabled": False, "running": False, "active_tasks": 0}
    return service.get_status()


@router.get("/channels")
async def get_available_channels() -> dict:
    """Get available notification channels."""
    from app.scheduler.notify import NotifyService

    return {"channels": NotifyService.get_available_channels()}


@router.get("/{task_id}", response_model=TaskResponse)
async def get_task(task_id: str, request: Request) -> TaskResponse:
    """Get a scheduled task by ID."""
    store = _get_store(request)
    task = store.get_task(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
    return _task_to_response(task)


@router.put("/{task_id}", response_model=TaskResponse)
async def update_task(task_id: str, body: TaskUpdateRequest, request: Request) -> TaskResponse:
    """Update a scheduled task."""
    store = _get_store(request)
    task = store.get_task(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail=f"Task {task_id} not found")

    updates: dict[str, Any] = {}
    if body.task_name is not None:
        updates["task_name"] = body.task_name
    if body.task_prompt is not None:
        updates["task_prompt"] = body.task_prompt
    if body.timezone is not None:
        updates["timezone"] = body.timezone
    if body.notify_channels is not None:
        updates["notify_channels"] = body.notify_channels
    if body.notify_config is not None:
        updates["notify_config"] = body.notify_config

    if body.schedule is not None:
        schedule_dict = body.schedule.model_dump(exclude_none=True)
        schedule_type = schedule_dict.pop("type")
        updates["schedule_type"] = schedule_type
        updates["schedule_config"] = schedule_dict

        next_run = compute_next_run_time(
            schedule_type,
            schedule_dict,
            body.timezone or task.get("timezone", "Asia/Shanghai"),
        )
        updates["next_run_at"] = next_run
        if next_run is None and task.get("status") == "active":
            updates["status"] = "completed"

    if updates:
        store.update_task(task_id, **updates)

    return _task_to_response(store.get_task(task_id) or task)


@router.patch("/{task_id}/status", response_model=TaskResponse)
async def update_task_status(task_id: str, body: TaskStatusRequest, request: Request) -> TaskResponse:
    """Pause or resume a scheduled task."""
    store = _get_store(request)
    task = store.get_task(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail=f"Task {task_id} not found")

    if body.status not in ("active", "paused"):
        raise HTTPException(status_code=400, detail="Status must be 'active' or 'paused'")

    updates: dict[str, Any] = {"status": body.status}

    if body.status == "active" and task.get("next_run_at") is None:
        next_run = compute_next_run_time(
            task.get("schedule_type", ""),
            task.get("schedule_config", {}),
            task.get("timezone", "Asia/Shanghai"),
        )
        updates["next_run_at"] = next_run
        if next_run is None:
            updates["status"] = "completed"

    store.update_task(task_id, **updates)
    return _task_to_response(store.get_task(task_id) or task)


@router.delete("/{task_id}")
async def delete_task(task_id: str, request: Request) -> dict:
    """Delete a scheduled task (soft delete)."""
    store = _get_store(request)
    success = store.delete_task(task_id)
    if not success:
        raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
    return {"success": True, "message": f"Task {task_id} deleted"}


@router.get("/{task_id}/executions", response_model=list[ExecutionResponse])
async def list_executions(
    task_id: str,
    request: Request,
    limit: int = 20,
    offset: int = 0,
) -> list[ExecutionResponse]:
    """Get execution history for a scheduled task."""
    store = _get_store(request)
    task = store.get_task(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
    executions = store.list_executions(task_id, limit=limit, offset=offset)
    return [_execution_to_response(e) for e in executions]


@router.post("/notify/test")
async def test_notify(body: NotifyTestRequest) -> dict:
    """Test a notification channel configuration."""
    from app.scheduler.notify import NotifyService

    service = NotifyService()
    results = await service.send(
        channels=[body.channel],
        content=body.test_message,
        config={body.channel: body.config},
    )
    return results
