"""Memory storage providers."""

import abc
import json
import logging
import threading
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from deerflow.config.agents_config import AGENT_NAME_PATTERN
from deerflow.config.memory_config import get_memory_config
from deerflow.config.paths import get_paths

logger = logging.getLogger(__name__)


def utc_now_iso_z() -> str:
    """Current UTC time as ISO-8601 with ``Z`` suffix (matches prior naive-UTC output)."""
    return datetime.now(UTC).isoformat().removesuffix("+00:00") + "Z"


def create_empty_memory() -> dict[str, Any]:
    """Create an empty memory structure."""
    return {
        "version": "1.0",
        "lastUpdated": utc_now_iso_z(),
        "user": {
            "workContext": {"summary": "", "updatedAt": ""},
            "personalContext": {"summary": "", "updatedAt": ""},
            "topOfMind": {"summary": "", "updatedAt": ""},
        },
        "history": {
            "recentMonths": {"summary": "", "updatedAt": ""},
            "earlierContext": {"summary": "", "updatedAt": ""},
            "longTermBackground": {"summary": "", "updatedAt": ""},
        },
        "facts": [],
    }


class MemoryStorage(abc.ABC):
    """Abstract base class for memory storage providers."""

    @abc.abstractmethod
    def load(self, agent_name: str | None = None) -> dict[str, Any]:
        """Load memory data for the given agent."""
        pass

    @abc.abstractmethod
    def reload(self, agent_name: str | None = None) -> dict[str, Any]:
        """Force reload memory data for the given agent."""
        pass

    @abc.abstractmethod
    def save(self, memory_data: dict[str, Any], agent_name: str | None = None) -> bool:
        """Save memory data for the given agent."""
        pass


class FileMemoryStorage(MemoryStorage):
    """File-based memory storage provider."""

    def __init__(self):
        """Initialize the file memory storage."""
        # Per-agent memory cache: keyed by agent_name (None = global)
        # Value: (memory_data, file_mtime)
        self._memory_cache: dict[str | None, tuple[dict[str, Any], float | None]] = {}

    def _validate_agent_name(self, agent_name: str) -> None:
        """Validate that the agent name is safe to use in filesystem paths.

        Uses the repository's established AGENT_NAME_PATTERN to ensure consistency
        across the codebase and prevent path traversal or other problematic characters.
        """
        if not agent_name:
            raise ValueError("Agent name must be a non-empty string.")
        if not AGENT_NAME_PATTERN.match(agent_name):
            raise ValueError(f"Invalid agent name {agent_name!r}: names must match {AGENT_NAME_PATTERN.pattern}")

    def _get_memory_file_path(self, agent_name: str | None = None) -> Path:
        """Get the path to the memory file."""
        if agent_name is not None:
            self._validate_agent_name(agent_name)
            return get_paths().agent_memory_file(agent_name)

        config = get_memory_config()
        if config.storage_path:
            p = Path(config.storage_path)
            return p if p.is_absolute() else get_paths().base_dir / p
        return get_paths().memory_file

    def _load_memory_from_file(self, agent_name: str | None = None) -> dict[str, Any]:
        """Load memory data from file."""
        file_path = self._get_memory_file_path(agent_name)

        if not file_path.exists():
            return create_empty_memory()

        try:
            with open(file_path, encoding="utf-8") as f:
                data = json.load(f)
            return data
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("Failed to load memory file: %s", e)
            return create_empty_memory()

    def load(self, agent_name: str | None = None) -> dict[str, Any]:
        """Load memory data (cached with file modification time check)."""
        file_path = self._get_memory_file_path(agent_name)

        try:
            current_mtime = file_path.stat().st_mtime if file_path.exists() else None
        except OSError:
            current_mtime = None

        cached = self._memory_cache.get(agent_name)

        if cached is None or cached[1] != current_mtime:
            memory_data = self._load_memory_from_file(agent_name)
            self._memory_cache[agent_name] = (memory_data, current_mtime)
            return memory_data

        return cached[0]

    def reload(self, agent_name: str | None = None) -> dict[str, Any]:
        """Reload memory data from file, forcing cache invalidation."""
        file_path = self._get_memory_file_path(agent_name)
        memory_data = self._load_memory_from_file(agent_name)

        try:
            mtime = file_path.stat().st_mtime if file_path.exists() else None
        except OSError:
            mtime = None

        self._memory_cache[agent_name] = (memory_data, mtime)
        return memory_data

    def save(self, memory_data: dict[str, Any], agent_name: str | None = None) -> bool:
        """Save memory data to file and update cache."""
        file_path = self._get_memory_file_path(agent_name)

        try:
            file_path.parent.mkdir(parents=True, exist_ok=True)
            memory_data["lastUpdated"] = utc_now_iso_z()

            temp_path = file_path.with_suffix(f".{uuid.uuid4().hex}.tmp")
            with open(temp_path, "w", encoding="utf-8") as f:
                json.dump(memory_data, f, indent=2, ensure_ascii=False)

            temp_path.replace(file_path)

            try:
                mtime = file_path.stat().st_mtime
            except OSError:
                mtime = None

            self._memory_cache[agent_name] = (memory_data, mtime)
            logger.info("Memory saved to %s", file_path)
            return True
        except OSError as e:
            logger.error("Failed to save memory file: %s", e)
            return False


class PostgresMemoryStorage(MemoryStorage):
    """PostgreSQL-backed memory storage provider."""

    _table_name = "deerflow_memory"

    def __init__(self, connection_string: str | None = None):
        config = get_memory_config()
        self._connection_string = connection_string or config.connection_string
        if not self._connection_string:
            raise ValueError("memory.connection_string is required for PostgresMemoryStorage")
        try:
            import psycopg
        except ImportError as exc:
            raise ImportError(
                "psycopg is required for PostgresMemoryStorage. Install it with: uv add psycopg[binary]"
            ) from exc

        self._psycopg = psycopg
        self._memory_cache: dict[str | None, tuple[dict[str, Any], str | None]] = {}
        self._init_lock = threading.Lock()
        self._initialized = False

    def _validate_agent_name(self, agent_name: str) -> None:
        if not agent_name:
            raise ValueError("Agent name must be a non-empty string.")
        if not AGENT_NAME_PATTERN.match(agent_name):
            raise ValueError(f"Invalid agent name {agent_name!r}: names must match {AGENT_NAME_PATTERN.pattern}")

    def _scope_key(self, agent_name: str | None = None) -> str:
        if agent_name is None:
            return "global"
        self._validate_agent_name(agent_name)
        return f"agent:{agent_name}"

    def _connect(self):
        return self._psycopg.connect(self._connection_string)

    def _ensure_schema(self, conn) -> None:
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._table_name} (
                scope_key text PRIMARY KEY,
                agent_name text,
                memory_data jsonb NOT NULL,
                last_updated text NOT NULL
            )
            """
        )
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{self._table_name}_agent_name ON {self._table_name} (agent_name)"
        )

    def _ensure_initialized(self) -> None:
        if self._initialized:
            return
        with self._init_lock:
            if self._initialized:
                return
            with self._connect() as conn:
                self._ensure_schema(conn)
            self._initialized = True

    def _load_from_db(self, agent_name: str | None = None) -> tuple[dict[str, Any], str | None]:
        self._ensure_initialized()
        scope_key = self._scope_key(agent_name)

        with self._connect() as conn:
            self._ensure_schema(conn)
            row = conn.execute(
                f"SELECT memory_data::text, last_updated FROM {self._table_name} WHERE scope_key = %s",
                (scope_key,),
            ).fetchone()

        if row is None:
            return create_empty_memory(), None

        memory_json, last_updated = row
        cached = self._memory_cache.get(agent_name)
        if cached is not None and cached[1] == last_updated:
            return cached[0], cached[1]

        try:
            memory_data = json.loads(memory_json) if memory_json else create_empty_memory()
        except (TypeError, json.JSONDecodeError) as e:
            logger.warning("Failed to load memory row from PostgreSQL: %s", e)
            return create_empty_memory(), None

        if not isinstance(memory_data, dict):
            logger.warning("PostgreSQL memory row did not contain an object; resetting to empty memory")
            return create_empty_memory(), None

        return memory_data, last_updated

    def load(self, agent_name: str | None = None) -> dict[str, Any]:
        memory_data, last_updated = self._load_from_db(agent_name)
        self._memory_cache[agent_name] = (memory_data, last_updated)
        return memory_data

    def reload(self, agent_name: str | None = None) -> dict[str, Any]:
        memory_data, last_updated = self._load_from_db(agent_name)
        self._memory_cache[agent_name] = (memory_data, last_updated)
        return memory_data

    def save(self, memory_data: dict[str, Any], agent_name: str | None = None) -> bool:
        scope_key = self._scope_key(agent_name)
        memory_to_save = dict(memory_data)
        memory_to_save["lastUpdated"] = utc_now_iso_z()
        payload = json.dumps(memory_to_save, ensure_ascii=False)

        try:
            self._ensure_initialized()
            with self._connect() as conn:
                self._ensure_schema(conn)
                conn.execute(
                    f"""
                    INSERT INTO {self._table_name} (scope_key, agent_name, memory_data, last_updated)
                    VALUES (%s, %s, %s::jsonb, %s)
                    ON CONFLICT (scope_key)
                    DO UPDATE SET
                        agent_name = EXCLUDED.agent_name,
                        memory_data = EXCLUDED.memory_data,
                        last_updated = EXCLUDED.last_updated
                    """,
                    (scope_key, agent_name, payload, memory_to_save["lastUpdated"]),
                )

            self._memory_cache[agent_name] = (memory_to_save, memory_to_save["lastUpdated"])
            logger.info("Memory saved to PostgreSQL scope %s", scope_key)
            return True
        except Exception as e:
            logger.error("Failed to save memory to PostgreSQL: %s", e)
            return False


_storage_instance: MemoryStorage | None = None
_storage_lock = threading.Lock()


def get_memory_storage() -> MemoryStorage:
    """Get the configured memory storage instance."""
    global _storage_instance
    if _storage_instance is not None:
        return _storage_instance

    with _storage_lock:
        if _storage_instance is not None:
            return _storage_instance

        config = get_memory_config()
        storage_class_path = config.storage_class

        try:
            module_path, class_name = storage_class_path.rsplit(".", 1)
            import importlib

            module = importlib.import_module(module_path)
            storage_class = getattr(module, class_name)

            # Validate that the configured storage is a MemoryStorage implementation
            if not isinstance(storage_class, type):
                raise TypeError(f"Configured memory storage '{storage_class_path}' is not a class: {storage_class!r}")
            if not issubclass(storage_class, MemoryStorage):
                raise TypeError(f"Configured memory storage '{storage_class_path}' is not a subclass of MemoryStorage")

            _storage_instance = storage_class()
        except Exception as e:
            if "PostgresMemoryStorage" in storage_class_path:
                logger.error("Failed to load configured PostgreSQL memory storage %s: %s", storage_class_path, e)
                raise
            logger.error(
                "Failed to load memory storage %s, falling back to FileMemoryStorage: %s",
                storage_class_path,
                e,
            )
            _storage_instance = FileMemoryStorage()

    return _storage_instance
