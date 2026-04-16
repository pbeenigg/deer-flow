"""Migrate DeerFlow local memory/checkpoint state into PostgreSQL.

Defaults:
- memory: backend/.deer-flow/memory.json
- checkpointer: backend/.deer-flow/checkpoints.db
- postgres DSN: DEER_FLOW_POSTGRES_DSN or postgresql://deerflow:deerflow@localhost:5432/deerflow

This script is intended to run in an environment that has the LangGraph
checkpoint packages installed, such as the DeerFlow gateway container.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from collections import defaultdict
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Migrate local DeerFlow state into PostgreSQL")
    parser.add_argument(
        "--dsn",
        default=os.getenv("DEER_FLOW_POSTGRES_DSN", "postgresql://deerflow:deerflow@localhost:5432/deerflow"),
        help="PostgreSQL DSN",
    )
    parser.add_argument(
        "--memory-path",
        default=Path("backend/.deer-flow/memory.json"),
        type=Path,
        help="Path to the source memory.json file",
    )
    parser.add_argument(
        "--checkpoints-db",
        default=Path("backend/.deer-flow/checkpoints.db"),
        type=Path,
        help="Path to the source SQLite checkpoints.db file",
    )
    return parser.parse_args()


def ensure_memory_schema(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS deerflow_memory (
            scope_key text PRIMARY KEY,
            agent_name text,
            memory_data jsonb NOT NULL,
            last_updated text NOT NULL
        )
        """
    )


def migrate_memory(conn, memory_path: Path) -> None:
    if not memory_path.exists():
        print(f"skip memory: source file not found at {memory_path}")
        return

    memory_data = json.loads(memory_path.read_text(encoding="utf-8"))
    last_updated = memory_data.get("lastUpdated") or memory_data.get("updatedAt") or ""
    conn.execute(
        """
        INSERT INTO deerflow_memory (scope_key, agent_name, memory_data, last_updated)
        VALUES (%s, %s, %s::jsonb, %s)
        ON CONFLICT (scope_key)
        DO UPDATE SET
            agent_name = EXCLUDED.agent_name,
            memory_data = EXCLUDED.memory_data,
            last_updated = EXCLUDED.last_updated
        """,
        ("global", None, json.dumps(memory_data, ensure_ascii=False), last_updated),
    )
    print(f"migrated memory: {memory_path}")


def reset_checkpoint_schema(conn) -> None:
    conn.execute(
        """
        DROP TABLE IF EXISTS checkpoint_writes CASCADE;
        DROP TABLE IF EXISTS checkpoint_blobs CASCADE;
        DROP TABLE IF EXISTS checkpoints CASCADE;
        DROP TABLE IF EXISTS checkpoint_migrations CASCADE;
        """
    )


async def migrate_checkpoints(checkpoints_db: Path, dsn: str) -> None:
    if not checkpoints_db.exists():
        print(f"skip checkpoints: source db not found at {checkpoints_db}")
        return

    try:
        from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
        from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
    except ImportError as exc:  # pragma: no cover - runtime dependency guard
        raise SystemExit(
            "langgraph checkpoint packages are required. Run this inside the DeerFlow gateway container."
        ) from exc

    source_uri = checkpoints_db.resolve().as_posix()

    async with AsyncSqliteSaver.from_conn_string(source_uri) as source, AsyncPostgresSaver.from_conn_string(dsn) as target:
        await target.setup()

        checkpoint_count = 0
        pending_write_count = 0
        async for checkpoint_tuple in source.alist(None):
            thread_id = checkpoint_tuple.config["configurable"]["thread_id"]
            checkpoint_ns = checkpoint_tuple.config["configurable"].get("checkpoint_ns", "")
            parent_config = checkpoint_tuple.parent_config
            if parent_config is None:
                write_config = {"configurable": {"thread_id": thread_id, "checkpoint_ns": checkpoint_ns}}
            else:
                write_config = {"configurable": dict(parent_config["configurable"])}
                write_config["configurable"].setdefault("checkpoint_ns", checkpoint_ns)

            next_config = await target.aput(
                write_config,
                checkpoint_tuple.checkpoint,
                checkpoint_tuple.metadata,
                checkpoint_tuple.checkpoint.get("channel_versions", {}),
            )
            checkpoint_count += 1

            if checkpoint_tuple.pending_writes:
                grouped_writes: dict[str, list[tuple[str, Any]]] = defaultdict(list)
                for item in checkpoint_tuple.pending_writes:
                    if len(item) == 3:
                        task_id, channel, value = item
                    elif len(item) == 4:
                        task_id, channel, value, _ = item
                    else:
                        raise ValueError(f"Unexpected pending write tuple shape: {item!r}")
                    grouped_writes[task_id].append((channel, value))

                for task_id, writes in grouped_writes.items():
                    await target.aput_writes(next_config, writes, task_id)
                    pending_write_count += len(writes)

        print(f"migrated checkpoints: {checkpoint_count}")
        print(f"migrated pending writes: {pending_write_count}")


def main() -> None:
    args = parse_args()

    try:
        import psycopg
    except ImportError as exc:
        raise SystemExit("psycopg is required. Install psycopg[binary] first.") from exc

    with psycopg.connect(args.dsn) as conn:
        ensure_memory_schema(conn)
        migrate_memory(conn, args.memory_path)
        reset_checkpoint_schema(conn)
        conn.commit()

    asyncio.run(migrate_checkpoints(args.checkpoints_db, args.dsn))


if __name__ == "__main__":
    main()
