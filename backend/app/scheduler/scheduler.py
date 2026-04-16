"""TaskScheduler — scans due tasks and dispatches them for execution.

Uses APScheduler for reliable interval-based scanning. The scheduler
computes next_run_at for each task based on its schedule_config.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timedelta
from typing import Any

from app.scheduler.store import ScheduledTaskStore

logger = logging.getLogger(__name__)


def compute_next_run_time(
    schedule_type: str,
    schedule_config: dict[str, Any],
    timezone: str = "Asia/Shanghai",
    *,
    from_time: datetime | None = None,
) -> float | None:
    """Compute the next run timestamp for a schedule.

    Returns a Unix timestamp (float) or None if the schedule type
    is 'once' and has already fired.
    """
    try:
        from zoneinfo import ZoneInfo
    except ImportError:
        from backports.zoneinfo import ZoneInfo  # type: ignore[no-redef]

    tz = ZoneInfo(timezone)
    now = from_time or datetime.now(tz)
    now_ts = now.timestamp()

    if schedule_type == "once":
        hour = schedule_config.get("hour", 0)
        minute = schedule_config.get("minute", 0)
        target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if target.timestamp() > now_ts:
            return target.timestamp()
        return None

    if schedule_type == "daily":
        hour = schedule_config.get("hour", 0)
        minute = schedule_config.get("minute", 0)
        target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if target.timestamp() <= now_ts:
            target += timedelta(days=1)
        return target.timestamp()

    if schedule_type == "weekly":
        day_map = {
            "monday": 0,
            "tuesday": 1,
            "wednesday": 2,
            "thursday": 3,
            "friday": 4,
            "saturday": 5,
            "sunday": 6,
        }
        target_day = day_map.get(schedule_config.get("day_of_week", "monday").lower(), 0)
        hour = schedule_config.get("hour", 0)
        minute = schedule_config.get("minute", 0)
        target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        days_ahead = (target_day - target.weekday()) % 7
        if days_ahead == 0 and target.timestamp() <= now_ts:
            days_ahead = 7
        target += timedelta(days=days_ahead)
        return target.timestamp()

    if schedule_type == "interval":
        minutes = schedule_config.get("minutes", 60)
        return now_ts + minutes * 60

    if schedule_type == "cron":
        expression = schedule_config.get("expression", "")
        return _compute_cron_next(expression, now, tz)

    logger.warning("Unknown schedule_type: %s", schedule_type)
    return None


def _compute_cron_next(expression: str, now: datetime, tz: Any) -> float | None:
    """Simple cron expression parser for 'min hour day month dow' format."""
    try:
        parts = expression.strip().split()
        if len(parts) != 5:
            logger.warning("Invalid cron expression (expected 5 fields): %s", expression)
            return None

        cron_minute = _parse_cron_field(parts[0], 0, 59)
        cron_hour = _parse_cron_field(parts[1], 0, 23)
        cron_day = _parse_cron_field(parts[2], 1, 31)
        cron_month = _parse_cron_field(parts[3], 1, 12)
        cron_dow = _parse_cron_field(parts[4], 0, 6)

        target = now.replace(second=0, microsecond=0)
        for _ in range(366 * 24 * 60):
            target += timedelta(minutes=1)
            if target.minute in cron_minute and target.hour in cron_hour and target.day in cron_day and target.month in cron_month and target.weekday() in cron_dow:
                return target.timestamp()

        return None
    except Exception:
        logger.exception("Failed to compute cron next run: %s", expression)
        return None


def _parse_cron_field(field: str, min_val: int, max_val: int) -> set[int]:
    """Parse a single cron field into a set of valid integer values."""
    result: set[int] = set()
    for part in field.split(","):
        if part == "*":
            result.update(range(min_val, max_val + 1))
        elif "-" in part:
            start, end = part.split("-", 1)
            step = 1
            if "/" in end:
                end, step_str = end.split("/", 1)
                step = int(step_str)
            result.update(range(int(start), int(end) + 1, step))
        elif "/" in part:
            base, step_str = part.split("/", 1)
            step = int(step_str)
            start = min_val if base == "*" else int(base)
            result.update(range(start, max_val + 1, step))
        else:
            result.add(int(part))
    return result


class TaskScheduler:
    """Scans for due scheduled tasks and dispatches them for execution.

    Integrates with the FastAPI lifespan via start()/stop() methods.
    """

    def __init__(self, store: ScheduledTaskStore) -> None:
        self._store = store
        self._running = False
        self._task: asyncio.Task | None = None
        self._on_due_task = None

    def set_callback(self, callback) -> None:
        """Set the callback invoked for each due task.

        The callback receives the task dict and should be an async function.
        """
        self._on_due_task = callback

    async def start(self) -> None:
        """Start the scheduler scan loop."""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._scan_loop())
        logger.info("TaskScheduler started")

    async def stop(self) -> None:
        """Stop the scheduler scan loop."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        logger.info("TaskScheduler stopped")

    async def _scan_loop(self) -> None:
        """Periodically scan for due tasks."""
        while self._running:
            try:
                await self._scan_due_tasks()
            except Exception:
                logger.exception("Error scanning due tasks")
            await asyncio.sleep(60)

    async def _scan_due_tasks(self) -> None:
        """Scan for due tasks and dispatch them."""
        due_tasks = self._store.get_due_tasks()
        if not due_tasks:
            return

        logger.info("Found %d due task(s)", len(due_tasks))
        for task in due_tasks:
            try:
                if self._on_due_task:
                    await self._on_due_task(task)
                self._store.update_task(
                    task["id"],
                    last_run_at=time.time(),
                )
                next_run = compute_next_run_time(
                    task["schedule_type"],
                    task.get("schedule_config", {}),
                    task.get("timezone", "Asia/Shanghai"),
                )
                self._store.update_task(
                    task["id"],
                    next_run_at=next_run,
                )
                if next_run is None:
                    self._store.update_task(task["id"], status="completed")
            except Exception:
                logger.exception("Error dispatching task %s", task.get("id"))

    def initialize_next_run_times(self) -> None:
        """Compute next_run_at for all active tasks that don't have one yet.

        Called once at startup to bootstrap the scheduler.
        """
        tasks = self._store.list_tasks(status="active")
        for task in tasks:
            if task.get("next_run_at") is not None:
                continue
            next_run = compute_next_run_time(
                task["schedule_type"],
                task.get("schedule_config", {}),
                task.get("timezone", "Asia/Shanghai"),
            )
            if next_run is not None:
                self._store.update_task(task["id"], next_run_at=next_run)
            else:
                self._store.update_task(task["id"], status="completed")
