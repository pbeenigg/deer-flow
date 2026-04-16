"""Quick test script for the scheduler module."""

from app.scheduler.store import ScheduledTaskStore
from app.scheduler.scheduler import TaskScheduler, compute_next_run_time
from app.scheduler.worker import TaskWorker
from app.scheduler.notify import NotifyService
from app.scheduler.service import SchedulerService

print("All scheduler modules imported successfully!")
print("Available channels:", NotifyService.get_available_channels())

next_run = compute_next_run_time("daily", {"hour": 9, "minute": 0}, "Asia/Shanghai")
print(f"Next daily 9:00 run: {next_run}")

next_weekly = compute_next_run_time("weekly", {"day_of_week": "monday", "hour": 9, "minute": 0}, "Asia/Shanghai")
print(f"Next Monday 9:00 run: {next_weekly}")

next_interval = compute_next_run_time("interval", {"minutes": 30}, "Asia/Shanghai")
print(f"Next 30min interval run: {next_interval}")

next_cron = compute_next_run_time("cron", {"expression": "0 9 * * 1-5"}, "Asia/Shanghai")
print(f"Next weekday 9:00 cron run: {next_cron}")

import tempfile, os

tmpdir = tempfile.mkdtemp()
store = ScheduledTaskStore(path=os.path.join(tmpdir, "test.json"))
task = store.create_task(
    user_id="test_user",
    task_name="Test Task",
    task_type="news_digest",
    task_prompt="Test prompt",
    schedule_type="daily",
    schedule_config={"hour": 9, "minute": 0},
    notify_channels=["email"],
    notify_config={"email": {"to_email": "test@example.com"}},
)
print(f"Created task: {task['id']}")

tasks = store.list_tasks(user_id="test_user")
print(f"Listed {len(tasks)} task(s)")

retrieved = store.get_task(task["id"])
print(f"Retrieved task: {retrieved['task_name']}")

store.update_task(task["id"], status="paused")
updated = store.get_task(task["id"])
print(f"Updated status: {updated['status']}")

execution = store.create_execution(task["id"])
print(f"Created execution: {execution['id']}")

store.update_execution(execution["id"], status="success", result_content="Test result")
exec_updated = store.get_task(task["id"])
executions = store.list_executions(task["id"])
print(f"Listed {len(executions)} execution(s)")

import shutil

shutil.rmtree(tmpdir)
print("All tests passed!")
