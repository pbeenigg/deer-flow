"""SchedulerService — manages the lifecycle of the scheduled task system.

Singleton pattern matching ChannelService for consistency.
Started/stopped during the FastAPI lifespan.
"""

from __future__ import annotations

import logging
import os
from typing import Any

from app.scheduler.scheduler import TaskScheduler
from app.scheduler.store import create_store
from app.scheduler.worker import TaskWorker

logger = logging.getLogger(__name__)

_SCHEDULER_LANGGRAPH_URL_ENV = "DEER_FLOW_SCHEDULER_LANGGRAPH_URL"
_DEFAULT_LANGGRAPH_URL = "http://localhost:2024"


class SchedulerService:
    """Manages the lifecycle of the scheduled task system.

    Coordinates the TaskStore, TaskScheduler, and TaskWorker.
    """

    def __init__(
        self,
        *,
        langgraph_url: str = _DEFAULT_LANGGRAPH_URL,
    ) -> None:
        self.store = create_store()
        self.worker = TaskWorker(self.store, langgraph_url=langgraph_url)
        self.scheduler = TaskScheduler(self.store)
        self._running = False

    @classmethod
    def from_app_config(cls) -> SchedulerService:
        """Create a SchedulerService from the application config."""
        from deerflow.config.app_config import get_app_config

        config = get_app_config()
        extra = config.model_extra or {}
        scheduler_config = extra.get("scheduler", {})

        langgraph_url = scheduler_config.get("langgraph_url", "")
        if not langgraph_url:
            langgraph_url = os.getenv(_SCHEDULER_LANGGRAPH_URL_ENV, _DEFAULT_LANGGRAPH_URL)

        enabled = scheduler_config.get("enabled", False)

        service = cls(langgraph_url=langgraph_url)
        service._enabled = enabled
        return service

    async def start(self) -> None:
        """Start the scheduler and worker."""
        if self._running:
            return

        if not getattr(self, "_enabled", False):
            logger.info("Scheduler service is disabled, skipping start")
            return

        await self.worker.start()

        self.scheduler.set_callback(self._on_due_task)
        self.scheduler.initialize_next_run_times()
        await self.scheduler.start()

        self._running = True
        logger.info("SchedulerService started")

    async def stop(self) -> None:
        """Stop the scheduler and worker."""
        await self.scheduler.stop()
        await self.worker.stop()
        self._running = False
        logger.info("SchedulerService stopped")

    async def _on_due_task(self, task: dict[str, Any]) -> None:
        """Callback invoked by the scheduler for each due task."""
        await self.worker.execute_task(task)

    def get_status(self) -> dict[str, Any]:
        """Return status information."""
        return {
            "enabled": getattr(self, "_enabled", False),
            "running": self._running,
            "active_tasks": len(self.store.list_tasks(status="active")),
        }


_scheduler_service: SchedulerService | None = None


def get_scheduler_service() -> SchedulerService | None:
    """Get the singleton SchedulerService instance (if started)."""
    return _scheduler_service


async def start_scheduler_service() -> SchedulerService:
    """Create and start the global SchedulerService from app config."""
    global _scheduler_service
    if _scheduler_service is not None:
        return _scheduler_service
    _scheduler_service = SchedulerService.from_app_config()
    await _scheduler_service.start()
    return _scheduler_service


async def stop_scheduler_service() -> None:
    """Stop the global SchedulerService."""
    global _scheduler_service
    if _scheduler_service is not None:
        await _scheduler_service.stop()
        _scheduler_service = None
