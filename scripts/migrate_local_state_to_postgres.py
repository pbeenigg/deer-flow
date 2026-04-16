"""Migrate DeerFlow local memory/checkpoint state into PostgreSQL.

Defaults:
- memory: backend/.deer-flow/memory.json
- checkpointer: backend/.deer-flow/checkpoints.db
- postgres DSN: DEER_FLOW_POSTGRES_DSN or postgresql://deerflow:deerflow@localhost:5432/deerflow

This script is intentionally one-way and idempotent for a fresh target database.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
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


def ensure_checkpoint_schema(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS checkpoints (
            thread_id text NOT NULL,
            checkpoint_ns text NOT NULL DEFAULT '',
            checkpoint_id text NOT NULL,
            parent_checkpoint_id text,
            type text,
            checkpoint bytea,
            metadata bytea,
            PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS writes (
            thread_id text NOT NULL,
            checkpoint_ns text NOT NULL DEFAULT '',
            checkpoint_id text NOT NULL,
            task_id text NOT NULL,
            idx integer NOT NULL,
            channel text NOT NULL,
            type text,
            value bytea,
            PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id, task_id, idx)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS store (
            prefix text NOT NULL,
            key text NOT NULL,
            value text NOT NULL,
            created_at timestamp DEFAULT CURRENT_TIMESTAMP,
            updated_at timestamp DEFAULT CURRENT_TIMESTAMP,
            expires_at timestamp,
            ttl_minutes real,
            PRIMARY KEY (prefix, key)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS store_migrations (
            v integer PRIMARY KEY
        )
        """
    )


def reset_target_tables(conn) -> None:
    conn.execute("TRUNCATE TABLE deerflow_memory, writes, checkpoints, store, store_migrations RESTART IDENTITY")


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


def copy_table(
    source_conn: sqlite3.Connection,
    target_conn,
    table_name: str,
    columns: list[str],
    transform: dict[str, Any] | None = None,
) -> int:
    transform = transform or {}
    source_conn.row_factory = sqlite3.Row
    cursor = source_conn.execute(f"SELECT {', '.join(columns)} FROM {table_name}")
    rows = cursor.fetchall()
    if not rows:
        return 0

    placeholders = ", ".join(["%s"] * len(columns))
    column_list = ", ".join(columns)
    sql = f"INSERT INTO {table_name} ({column_list}) VALUES ({placeholders})"
    values: list[tuple[Any, ...]] = []
    for row in rows:
        values.append(tuple(transform.get(column, lambda value: value)(row[column]) for column in columns))

    with target_conn.cursor() as cursor2:
        cursor2.executemany(sql, values)
    return len(values)


def migrate_checkpoints(conn, checkpoints_db: Path) -> None:
    if not checkpoints_db.exists():
        print(f"skip checkpoints: source db not found at {checkpoints_db}")
        return

    source_uri = f"file:{checkpoints_db.as_posix()}?mode=ro&immutable=1"
    source_conn = sqlite3.connect(source_uri, uri=True)
    source_conn.row_factory = sqlite3.Row
    try:
        checkpoint_count = copy_table(
            source_conn,
            conn,
            "checkpoints",
            ["thread_id", "checkpoint_ns", "checkpoint_id", "parent_checkpoint_id", "type", "checkpoint", "metadata"],
        )
        write_count = copy_table(
            source_conn,
            conn,
            "writes",
            ["thread_id", "checkpoint_ns", "checkpoint_id", "task_id", "idx", "channel", "type", "value"],
        )
        store_count = copy_table(
            source_conn,
            conn,
            "store",
            ["prefix", "key", "value", "created_at", "updated_at", "expires_at", "ttl_minutes"],
        )
        migration_count = copy_table(source_conn, conn, "store_migrations", ["v"])
        print(f"migrated checkpoints: {checkpoint_count}")
        print(f"migrated writes: {write_count}")
        print(f"migrated store rows: {store_count}")
        print(f"migrated store_migrations rows: {migration_count}")
    finally:
        source_conn.close()


def main() -> None:
    args = parse_args()

    try:
        import psycopg
    except ImportError as exc:
        raise SystemExit("psycopg is required. Install psycopg[binary] first.") from exc

    with psycopg.connect(args.dsn) as conn:
        ensure_memory_schema(conn)
        ensure_checkpoint_schema(conn)
        reset_target_tables(conn)
        migrate_memory(conn, args.memory_path)
        migrate_checkpoints(conn, args.checkpoints_db)
        conn.commit()


if __name__ == "__main__":
    main()