"""Migrate scheduler data from JSON file to PostgreSQL.

Usage:
    python scripts/migrate_scheduler_json_to_postgres.py [--dsn DSN] [--json-path PATH]
"""

from __future__ import annotations

import argparse
import json
import os
import sys

def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate scheduler JSON data to PostgreSQL")
    parser.add_argument(
        "--dsn",
        default=os.getenv("DEER_FLOW_POSTGRES_DSN", "postgresql://deerflow:deerflow@localhost:5432/deerflow"),
        help="PostgreSQL DSN",
    )
    parser.add_argument(
        "--json-path",
        default=None,
        help="Path to store.json (auto-detected if not specified)",
    )
    args = parser.parse_args()

    if args.json_path is None:
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        json_path = os.path.join(base_dir, "backend", ".deer-flow", "scheduler", "store.json")
    else:
        json_path = args.json_path

    if not os.path.exists(json_path):
        print(f"JSON file not found: {json_path}")
        sys.exit(1)

    with open(json_path, encoding="utf-8") as f:
        data = json.load(f)

    tasks = data.get("tasks", {})
    executions = data.get("executions", {})

    if not tasks and not executions:
        print("No data to migrate.")
        return

    try:
        import psycopg
    except ImportError:
        print("psycopg is required. Install with: pip install psycopg[binary]")
        sys.exit(1)

    with psycopg.connect(args.dsn, autocommit=False) as conn:
        with conn.cursor() as cur:
            task_count = 0
            for task_id, task in tasks.items():
                cur.execute(
                    """
                    INSERT INTO scheduler_tasks
                        (id, user_id, thread_id, task_name, task_type, task_prompt,
                         schedule_type, schedule_config, timezone, notify_channels,
                         notify_config, status, last_run_at, next_run_at, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s::jsonb, %s::jsonb, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        user_id = EXCLUDED.user_id,
                        thread_id = EXCLUDED.thread_id,
                        task_name = EXCLUDED.task_name,
                        task_type = EXCLUDED.task_type,
                        task_prompt = EXCLUDED.task_prompt,
                        schedule_type = EXCLUDED.schedule_type,
                        schedule_config = EXCLUDED.schedule_config,
                        timezone = EXCLUDED.timezone,
                        notify_channels = EXCLUDED.notify_channels,
                        notify_config = EXCLUDED.notify_config,
                        status = EXCLUDED.status,
                        last_run_at = EXCLUDED.last_run_at,
                        next_run_at = EXCLUDED.next_run_at,
                        created_at = EXCLUDED.created_at,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (
                        task["id"],
                        task.get("user_id", "default"),
                        task.get("thread_id"),
                        task.get("task_name", ""),
                        task.get("task_type", "custom_query"),
                        task.get("task_prompt", ""),
                        task.get("schedule_type", ""),
                        json.dumps(task.get("schedule_config", {})),
                        task.get("timezone", "Asia/Shanghai"),
                        json.dumps(task.get("notify_channels", [])),
                        json.dumps(task.get("notify_config", {})),
                        task.get("status", "active"),
                        task.get("last_run_at"),
                        task.get("next_run_at"),
                        task.get("created_at", 0),
                        task.get("updated_at", 0),
                    ),
                )
                task_count += 1

            exec_count = 0
            for exec_id, execution in executions.items():
                cur.execute(
                    """
                    INSERT INTO scheduler_executions
                        (id, task_id, started_at, finished_at, status, error_message,
                         result_content, notify_status, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        task_id = EXCLUDED.task_id,
                        started_at = EXCLUDED.started_at,
                        finished_at = EXCLUDED.finished_at,
                        status = EXCLUDED.status,
                        error_message = EXCLUDED.error_message,
                        result_content = EXCLUDED.result_content,
                        notify_status = EXCLUDED.notify_status,
                        created_at = EXCLUDED.created_at
                    """,
                    (
                        execution["id"],
                        execution.get("task_id", ""),
                        execution.get("started_at", 0),
                        execution.get("finished_at"),
                        execution.get("status", "running"),
                        execution.get("error_message"),
                        execution.get("result_content"),
                        json.dumps(execution.get("notify_status")) if execution.get("notify_status") else None,
                        execution.get("created_at", 0),
                    ),
                )
                exec_count += 1

        conn.commit()
        print(f"Migration complete: {task_count} tasks, {exec_count} executions migrated.")


if __name__ == "__main__":
    main()
