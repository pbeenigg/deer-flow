"""Comprehensive test for the scheduler system."""
import asyncio
import tempfile
import os
import shutil

from app.scheduler.store import ScheduledTaskStore
from app.scheduler.scheduler import TaskScheduler, compute_next_run_time
from app.scheduler.worker import TaskWorker
from app.scheduler.notify import NotifyService
from app.scheduler.service import SchedulerService


def test_store():
    tmpdir = tempfile.mkdtemp()
    try:
        store = ScheduledTaskStore(path=os.path.join(tmpdir, "test.json"))

        task = store.create_task(
            user_id="test_user",
            task_name="Daily Tech News",
            task_type="news_digest",
            task_prompt="Summarize today's tech news",
            schedule_type="daily",
            schedule_config={"hour": 9, "minute": 0},
            notify_channels=["email"],
            notify_config={"email": {"to_email": "test@example.com"}},
        )
        assert task["id"] is not None
        assert task["status"] == "active"
        print(f"  [PASS] create_task: {task['id']}")

        retrieved = store.get_task(task["id"])
        assert retrieved is not None
        assert retrieved["task_name"] == "Daily Tech News"
        print(f"  [PASS] get_task")

        tasks = store.list_tasks(user_id="test_user")
        assert len(tasks) == 1
        print(f"  [PASS] list_tasks: {len(tasks)} task(s)")

        store.update_task(task["id"], status="paused")
        updated = store.get_task(task["id"])
        assert updated["status"] == "paused"
        print(f"  [PASS] update_task: status={updated['status']}")

        store.delete_task(task["id"])
        deleted = store.get_task(task["id"])
        assert deleted["status"] == "deleted"
        print(f"  [PASS] delete_task: status={deleted['status']}")

        execution = store.create_execution(task["id"])
        assert execution["status"] == "running"
        print(f"  [PASS] create_execution: {execution['id']}")

        store.update_execution(execution["id"], status="success", result_content="Test result")
        exec_updated = store.update_execution(execution["id"], status="success")
        assert exec_updated["status"] == "success"
        print(f"  [PASS] update_execution")

        executions = store.list_executions(task["id"])
        assert len(executions) == 1
        print(f"  [PASS] list_executions: {len(executions)} execution(s)")
    finally:
        shutil.rmtree(tmpdir)


def test_compute_next_run_time():
    next_daily = compute_next_run_time("daily", {"hour": 9, "minute": 0}, "Asia/Shanghai")
    assert next_daily is not None
    print(f"  [PASS] daily: next_run={next_daily}")

    next_weekly = compute_next_run_time("weekly", {"day_of_week": "monday", "hour": 9, "minute": 0}, "Asia/Shanghai")
    assert next_weekly is not None
    print(f"  [PASS] weekly: next_run={next_weekly}")

    next_interval = compute_next_run_time("interval", {"minutes": 30}, "Asia/Shanghai")
    assert next_interval is not None
    print(f"  [PASS] interval: next_run={next_interval}")

    next_cron = compute_next_run_time("cron", {"expression": "0 9 * * 1-5"}, "Asia/Shanghai")
    assert next_cron is not None
    print(f"  [PASS] cron: next_run={next_cron}")

    next_once = compute_next_run_time("once", {"hour": 23, "minute": 59}, "Asia/Shanghai")
    print(f"  [PASS] once (future): next_run={next_once}")

    next_once_past = compute_next_run_time("once", {"hour": 0, "minute": 0}, "Asia/Shanghai")
    print(f"  [PASS] once (past): next_run={next_once_past}")


def test_notify_service():
    channels = NotifyService.get_available_channels()
    assert "email" in channels
    assert "wechat" in channels
    assert "telegram" in channels
    assert "dingtalk" in channels
    print(f"  [PASS] available channels: {channels}")


def test_scheduler():
    tmpdir = tempfile.mkdtemp()
    try:
        store = ScheduledTaskStore(path=os.path.join(tmpdir, "test.json"))
        scheduler = TaskScheduler(store)

        store.create_task(
            user_id="test_user",
            task_name="Test Task",
            task_type="custom_query",
            task_prompt="Test",
            schedule_type="daily",
            schedule_config={"hour": 9, "minute": 0},
        )

        scheduler.initialize_next_run_times()
        tasks = store.list_tasks(status="active")
        assert len(tasks) == 1
        assert tasks[0]["next_run_at"] is not None
        print(f"  [PASS] initialize_next_run_times: next_run_at={tasks[0]['next_run_at']}")
    finally:
        shutil.rmtree(tmpdir)


def test_service():
    service = SchedulerService(langgraph_url="http://localhost:2024")
    assert service.store is not None
    assert service.worker is not None
    assert service.scheduler is not None
    print(f"  [PASS] SchedulerService created")

    status = service.get_status()
    assert "enabled" in status
    assert "running" in status
    print(f"  [PASS] get_status: {status}")


if __name__ == "__main__":
    print("Testing ScheduledTaskStore...")
    test_store()

    print("\nTesting compute_next_run_time...")
    test_compute_next_run_time()

    print("\nTesting NotifyService...")
    test_notify_service()

    print("\nTesting TaskScheduler...")
    test_scheduler()

    print("\nTesting SchedulerService...")
    test_service()

    print("\n=== All tests passed! ===")
