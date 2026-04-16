"""Tests for memory storage providers."""

import threading
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from deerflow.agents.memory.storage import (
    FileMemoryStorage,
    MemoryStorage,
    create_empty_memory,
    get_memory_storage,
    PostgresMemoryStorage,
)
from deerflow.config.memory_config import MemoryConfig


class TestCreateEmptyMemory:
    """Test create_empty_memory function."""

    def test_returns_valid_structure(self):
        """Should return a valid empty memory structure."""
        memory = create_empty_memory()
        assert isinstance(memory, dict)
        assert memory["version"] == "1.0"
        assert "lastUpdated" in memory
        assert isinstance(memory["user"], dict)
        assert isinstance(memory["history"], dict)
        assert isinstance(memory["facts"], list)


class TestMemoryStorageInterface:
    """Test MemoryStorage abstract base class."""

    def test_abstract_methods(self):
        """Should raise TypeError when trying to instantiate abstract class."""

        class TestStorage(MemoryStorage):
            pass

        with pytest.raises(TypeError):
            TestStorage()


class TestFileMemoryStorage:
    """Test FileMemoryStorage implementation."""

    def test_get_memory_file_path_global(self, tmp_path):
        """Should return global memory file path when agent_name is None."""

        def mock_get_paths():
            mock_paths = MagicMock()
            mock_paths.memory_file = tmp_path / "memory.json"
            return mock_paths

        with patch("deerflow.agents.memory.storage.get_paths", side_effect=mock_get_paths):
            with patch("deerflow.agents.memory.storage.get_memory_config", return_value=MemoryConfig(storage_path="")):
                storage = FileMemoryStorage()
                path = storage._get_memory_file_path(None)
                assert path == tmp_path / "memory.json"

    def test_get_memory_file_path_agent(self, tmp_path):
        """Should return per-agent memory file path when agent_name is provided."""

        def mock_get_paths():
            mock_paths = MagicMock()
            mock_paths.agent_memory_file.return_value = tmp_path / "agents" / "test-agent" / "memory.json"
            return mock_paths

        with patch("deerflow.agents.memory.storage.get_paths", side_effect=mock_get_paths):
            storage = FileMemoryStorage()
            path = storage._get_memory_file_path("test-agent")
            assert path == tmp_path / "agents" / "test-agent" / "memory.json"

    @pytest.mark.parametrize("invalid_name", ["", "../etc/passwd", "agent/name", "agent\\name", "agent name", "agent@123", "agent_name"])
    def test_validate_agent_name_invalid(self, invalid_name):
        """Should raise ValueError for invalid agent names that don't match the pattern."""
        storage = FileMemoryStorage()
        with pytest.raises(ValueError, match="Invalid agent name|Agent name must be a non-empty string"):
            storage._validate_agent_name(invalid_name)

    def test_load_creates_empty_memory(self, tmp_path):
        """Should create empty memory when file doesn't exist."""

        def mock_get_paths():
            mock_paths = MagicMock()
            mock_paths.memory_file = tmp_path / "non_existent_memory.json"
            return mock_paths

        with patch("deerflow.agents.memory.storage.get_paths", side_effect=mock_get_paths):
            with patch("deerflow.agents.memory.storage.get_memory_config", return_value=MemoryConfig(storage_path="")):
                storage = FileMemoryStorage()
                memory = storage.load()
                assert isinstance(memory, dict)
                assert memory["version"] == "1.0"

    def test_save_writes_to_file(self, tmp_path):
        """Should save memory data to file."""
        memory_file = tmp_path / "memory.json"

        def mock_get_paths():
            mock_paths = MagicMock()
            mock_paths.memory_file = memory_file
            return mock_paths

        with patch("deerflow.agents.memory.storage.get_paths", side_effect=mock_get_paths):
            with patch("deerflow.agents.memory.storage.get_memory_config", return_value=MemoryConfig(storage_path="")):
                storage = FileMemoryStorage()
                test_memory = {"version": "1.0", "facts": [{"content": "test fact"}]}
                result = storage.save(test_memory)
                assert result is True
                assert memory_file.exists()

    def test_reload_forces_cache_invalidation(self, tmp_path):
        """Should force reload from file and invalidate cache."""
        memory_file = tmp_path / "memory.json"
        memory_file.parent.mkdir(parents=True, exist_ok=True)
        memory_file.write_text('{"version": "1.0", "facts": [{"content": "initial fact"}]}')

        def mock_get_paths():
            mock_paths = MagicMock()
            mock_paths.memory_file = memory_file
            return mock_paths

        with patch("deerflow.agents.memory.storage.get_paths", side_effect=mock_get_paths):
            with patch("deerflow.agents.memory.storage.get_memory_config", return_value=MemoryConfig(storage_path="")):
                storage = FileMemoryStorage()
                # First load
                memory1 = storage.load()
                assert memory1["facts"][0]["content"] == "initial fact"

                # Update file directly
                memory_file.write_text('{"version": "1.0", "facts": [{"content": "updated fact"}]}')

                # Reload should get updated data
                memory2 = storage.reload()
                assert memory2["facts"][0]["content"] == "updated fact"


class TestPostgresMemoryStorage:
    class _FakeResult:
        def __init__(self, row):
            self._row = row

        def fetchone(self):
            return self._row

    class _FakeConnection:
        def __init__(self, db):
            self.db = db
            self.executed = []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params=None):
            params = params or ()
            self.executed.append((sql, params))
            normalized = sql.strip().upper()

            if normalized.startswith("SELECT"):
                scope_key = params[0]
                row = self.db.get(scope_key)
                if row is None:
                    return TestPostgresMemoryStorage._FakeResult(None)
                return TestPostgresMemoryStorage._FakeResult((row["memory_data"], row["last_updated"]))

            if normalized.startswith("INSERT INTO"):
                scope_key, agent_name, memory_json, last_updated = params
                self.db[scope_key] = {
                    "agent_name": agent_name,
                    "memory_data": memory_json,
                    "last_updated": last_updated,
                }
                return TestPostgresMemoryStorage._FakeResult(None)

            if normalized.startswith("CREATE TABLE") or normalized.startswith("CREATE INDEX"):
                return TestPostgresMemoryStorage._FakeResult(None)

            raise AssertionError(f"Unexpected SQL: {sql}")

    @pytest.fixture(autouse=True)
    def reset_env(self):
        import deerflow.agents.memory.storage as storage_mod

        storage_mod._storage_instance = None
        yield
        storage_mod._storage_instance = None

    def test_load_and_save_global_memory(self):
        db = {}

        def fake_connect(_dsn):
            return TestPostgresMemoryStorage._FakeConnection(db)

        mock_psycopg = SimpleNamespace(connect=fake_connect)

        with patch.dict("sys.modules", {"psycopg": mock_psycopg}):
            storage = PostgresMemoryStorage(connection_string="postgresql://localhost/db")

            memory = create_empty_memory()
            memory["facts"] = [{"content": "postgres fact"}]

            assert storage.save(memory) is True
            loaded = storage.load()

        assert loaded["facts"][0]["content"] == "postgres fact"
        assert "global" in db

    def test_load_and_save_agent_memory(self):
        db = {}

        def fake_connect(_dsn):
            return TestPostgresMemoryStorage._FakeConnection(db)

        mock_psycopg = SimpleNamespace(connect=fake_connect)

        with patch.dict("sys.modules", {"psycopg": mock_psycopg}):
            storage = PostgresMemoryStorage(connection_string="postgresql://localhost/db")

            memory = create_empty_memory()
            memory["facts"] = [{"content": "agent fact"}]

            assert storage.save(memory, agent_name="agent_1") is True
            loaded = storage.reload(agent_name="agent_1")

        assert loaded["facts"][0]["content"] == "agent fact"
        assert "agent:agent_1" in db


class TestGetMemoryStorage:
    """Test get_memory_storage function."""

    @pytest.fixture(autouse=True)
    def reset_storage_instance(self):
        """Reset the global storage instance before and after each test."""
        import deerflow.agents.memory.storage as storage_mod

        storage_mod._storage_instance = None
        yield
        storage_mod._storage_instance = None

    def test_returns_file_memory_storage_by_default(self):
        """Should return FileMemoryStorage by default."""
        with patch("deerflow.agents.memory.storage.get_memory_config", return_value=MemoryConfig(storage_class="deerflow.agents.memory.storage.FileMemoryStorage")):
            storage = get_memory_storage()
            assert isinstance(storage, FileMemoryStorage)

    def test_falls_back_to_file_memory_storage_on_error(self):
        """Should fall back to FileMemoryStorage if configured storage fails to load."""
        with patch("deerflow.agents.memory.storage.get_memory_config", return_value=MemoryConfig(storage_class="non.existent.StorageClass")):
            storage = get_memory_storage()
            assert isinstance(storage, FileMemoryStorage)

    def test_returns_singleton_instance(self):
        """Should return the same instance on subsequent calls."""
        with patch("deerflow.agents.memory.storage.get_memory_config", return_value=MemoryConfig(storage_class="deerflow.agents.memory.storage.FileMemoryStorage")):
            storage1 = get_memory_storage()
            storage2 = get_memory_storage()
            assert storage1 is storage2

    def test_get_memory_storage_thread_safety(self):
        """Should safely initialize the singleton even with concurrent calls."""
        results = []

        def get_storage():
            # get_memory_storage is called concurrently from multiple threads while
            # get_memory_config is patched once around thread creation. This verifies
            # that the singleton initialization remains thread-safe.
            results.append(get_memory_storage())

        with patch("deerflow.agents.memory.storage.get_memory_config", return_value=MemoryConfig(storage_class="deerflow.agents.memory.storage.FileMemoryStorage")):
            threads = [threading.Thread(target=get_storage) for _ in range(10)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

        # All results should be the exact same instance
        assert len(results) == 10
        assert all(r is results[0] for r in results)

    def test_get_memory_storage_invalid_class_fallback(self):
        """Should fall back to FileMemoryStorage if the configured class is not actually a class."""
        # Using a built-in function instead of a class
        with patch("deerflow.agents.memory.storage.get_memory_config", return_value=MemoryConfig(storage_class="os.path.join")):
            storage = get_memory_storage()
            assert isinstance(storage, FileMemoryStorage)

    def test_get_memory_storage_non_subclass_fallback(self):
        """Should fall back to FileMemoryStorage if the configured class is not a subclass of MemoryStorage."""
        # Using 'dict' as a class that is not a MemoryStorage subclass
        with patch("deerflow.agents.memory.storage.get_memory_config", return_value=MemoryConfig(storage_class="builtins.dict")):
            storage = get_memory_storage()
            assert isinstance(storage, FileMemoryStorage)

    def test_get_memory_storage_returns_postgres_storage(self):
        mock_psycopg = SimpleNamespace(connect=lambda _dsn: TestPostgresMemoryStorage._FakeConnection({}))

        with (
            patch.dict("sys.modules", {"psycopg": mock_psycopg}),
            patch(
                "deerflow.agents.memory.storage.get_memory_config",
                return_value=MemoryConfig(
                    storage_class="deerflow.agents.memory.storage.PostgresMemoryStorage",
                    connection_string="postgresql://localhost/db",
                ),
            ),
        ):
            storage = get_memory_storage()

        assert isinstance(storage, PostgresMemoryStorage)
