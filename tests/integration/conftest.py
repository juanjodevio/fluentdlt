"""Pytest fixtures for integration tests.

This module provides database fixtures that set up test databases using Alembic
migrations. Supports both SQLite (default, fast) and PostgreSQL (optional).
"""

from __future__ import annotations

import os
import shutil
import tempfile
import time
from pathlib import Path
from typing import Iterator

import pytest
from _pytest.monkeypatch import MonkeyPatch
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import SQLAlchemyError

DEFAULT_SRC_PG_URL = "postgresql+psycopg2://fldt:fldt@localhost:5532/fldt_source"
DEFAULT_DEST_PG_URL = "postgresql+psycopg2://fldt:fldt@localhost:5542/fldt_dest"
POSTGRES_READY_TIMEOUT = 60.0
_INTEGRATION_DIR = Path(__file__).parent.absolute()


def _build_alembic_config() -> Config:
    alembic_ini = _INTEGRATION_DIR / "alembic.ini"
    config = Config(str(alembic_ini))
    script_location = _INTEGRATION_DIR / "alembic"
    config.set_main_option("script_location", str(script_location))
    return config


def _wait_for_postgres(url: str, timeout: float = POSTGRES_READY_TIMEOUT) -> None:
    """Wait until PostgreSQL is ready to accept connections."""
    start = time.time()
    last_exc: Exception | None = None
    while time.time() - start < timeout:
        engine = None
        try:
            engine = create_engine(url)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return
        except Exception as exc:  # pragma: no cover - best effort wait
            last_exc = exc
            time.sleep(1)
        finally:
            if engine is not None:
                engine.dispose()
    if last_exc is not None:
        raise last_exc


def _require_postgres_or_skip(url: str, label: str) -> None:
    """Skip tests gracefully if PostgreSQL is unavailable."""
    try:
        _wait_for_postgres(url)
    except Exception as exc:  # pragma: no cover - environment guard
        pytest.skip(f"{label} not reachable ({url}): {exc}")


def _reset_postgres_schema(url: str) -> None:
    """Drop all non-system schemas and recreate public for a clean slate."""
    engine = create_engine(url, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        schemas = conn.execute(
            text(
                """
                SELECT schema_name
                FROM information_schema.schemata
                WHERE schema_name NOT IN ('pg_catalog', 'information_schema')
                  AND schema_name NOT LIKE 'pg_%'
                """
            )
        )
        for row in schemas:
            schema = row[0]
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS public"))
    engine.dispose()


@pytest.fixture(scope="session")
def alembic_config() -> Config:
    """Get Alembic configuration object.

    Returns Alembic Config pointing to integration test migrations.
    """
    # Get path to alembic.ini
    return _build_alembic_config()


@pytest.fixture(scope="session")
def sqlite_database(alembic_config: Config) -> Iterator[str]:
    """Create SQLite test database with migrations.

    This fixture creates a temporary SQLite database, runs all Alembic
    migrations to create schema and seed data, then yields the connection
    string. Cleanup happens after all tests complete.

    Yields:
        str: SQLite connection string (sqlite:///path/to/db)
    """
    # Create temporary database file
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    # Create connection string
    db_url = f"sqlite:///{db_path}"

    # Configure Alembic to use this database
    alembic_config.set_main_option("sqlalchemy.url", db_url)

    # Run migrations to create schema and seed data
    command.upgrade(alembic_config, "head")

    yield db_url

    # Cleanup: remove database file (best effort on Windows)
    try:
        if os.path.exists(db_path):
            # Close any open connections first
            import gc

            gc.collect()
            os.remove(db_path)
    except (PermissionError, OSError):
        # On Windows, file might be locked - ignore cleanup error
        pass


@pytest.fixture(scope="session")
def postgres_database(alembic_config: Config) -> Iterator[str]:
    """Create PostgreSQL test database with migrations.

    This fixture creates a PostgreSQL test database (if available), runs
    migrations, and cleans up afterward. Requires PostgreSQL to be running
    and accessible via TEST_POSTGRES_URL environment variable.

    Yields:
        str: PostgreSQL connection string or None if not available
    """
    # Check if PostgreSQL is available
    postgres_url = os.getenv("TEST_POSTGRES_URL")

    if not postgres_url:
        pytest.skip(
            "PostgreSQL not available. Set TEST_POSTGRES_URL environment variable."
        )

    # Try to connect to PostgreSQL
    try:
        from sqlalchemy import create_engine, text

        # Test connection
        engine = create_engine(postgres_url)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        engine.dispose()

    except Exception as e:
        pytest.skip(f"PostgreSQL connection failed: {e}")

    # Configure Alembic to use PostgreSQL
    alembic_config.set_main_option("sqlalchemy.url", postgres_url)

    # Run migrations
    command.upgrade(alembic_config, "head")

    yield postgres_url

    # Cleanup: downgrade all migrations
    try:
        command.downgrade(alembic_config, "base")
    except Exception:
        # Best effort cleanup
        pass


@pytest.fixture(scope="session")
def pg_source_database() -> Iterator[str]:
    """Provision and seed the PostgreSQL source database via Alembic."""
    source_url = os.getenv("SRC_PG_URL", DEFAULT_SRC_PG_URL)
    _require_postgres_or_skip(source_url, "SRC_PG_URL")
    _reset_postgres_schema(source_url)

    source_config = _build_alembic_config()
    source_config.set_main_option("sqlalchemy.url", source_url)
    command.upgrade(source_config, "head")

    yield source_url

    _reset_postgres_schema(source_url)


@pytest.fixture(scope="session")
def pg_destination_database() -> Iterator[str]:
    """Provision clean PostgreSQL destination database for dlt output."""
    dest_url = os.getenv("DEST_PG_URL", DEFAULT_DEST_PG_URL)
    _require_postgres_or_skip(dest_url, "DEST_PG_URL")
    _reset_postgres_schema(dest_url)

    yield dest_url

    _reset_postgres_schema(dest_url)


@pytest.fixture(scope="session")
def pg_connection_urls(
    pg_source_database: str, pg_destination_database: str
) -> tuple[str, str]:
    """Return tuple of (source_url, destination_url) for pg<->pg tests."""
    return pg_source_database, pg_destination_database


@pytest.fixture
def postgres_destination_credentials(
    monkeypatch: MonkeyPatch, pg_destination_database: str
) -> None:
    """Configure dlt to write to PostgreSQL destination."""
    url = make_url(pg_destination_database)
    # dlt expects psycopg2-style DSN without the dialect driver suffix
    driverless_url = url.set(drivername="postgresql")
    monkeypatch.setenv(
        "DESTINATION__POSTGRES__CREDENTIALS",
        driverless_url.render_as_string(hide_password=False),
    )


@pytest.fixture
def reset_postgres_destination(pg_destination_database: str) -> Iterator[str]:
    """Ensure destination schema is empty before/after each test."""
    _reset_postgres_schema(pg_destination_database)
    yield pg_destination_database
    _reset_postgres_schema(pg_destination_database)


@pytest.fixture(scope="session")
def test_database(sqlite_database: str) -> str:
    """Get default test database (SQLite).

    This is the default database fixture that most tests should use.
    It's fast and requires no external setup.

    Yields:
        str: Database connection string
    """
    return sqlite_database


@pytest.fixture(scope="session")
def test_dbs_dir() -> Iterator[Path]:
    """Create and provide path to test databases directory.

    Creates tests/.test_dbs/ directory for storing DuckDB files during tests.
    This keeps all test database files in one place and persists them
    for inspection. Files are cleaned up before each test run.

    Yields:
        Path: Path to the test databases directory.
    """
    tests_dir = Path(__file__).parent.parent  # Go up to tests/
    dbs_dir = tests_dir / ".test_dbs"

    # Clean directory if it exists from previous run
    if dbs_dir.exists():
        try:
            shutil.rmtree(dbs_dir)
        except Exception:
            pass  # Best effort cleanup

    # Create fresh directory
    dbs_dir.mkdir(exist_ok=True)

    yield dbs_dir

    # Don't cleanup after tests - leave files for inspection
    # The directory will be cleaned on the next test run
    # and is gitignored anyway


@pytest.fixture(autouse=True)
def clean_dlt_state(test_dbs_dir: Path, monkeypatch: MonkeyPatch) -> Iterator[None]:
    """Clean dlt state directory and set working directory for DuckDB files.

    This ensures each test starts with a clean slate for incremental
    loading state management. Changes working directory to tests/.test_dbs/
    so all DuckDB files are created there.

    Note: autouse=True ensures this runs for all tests automatically.
    """
    state_dir = Path(".dlt")

    # Change to test databases directory BEFORE test starts
    # This ensures DuckDB files are created in the right place
    original_dir = os.getcwd()
    os.chdir(test_dbs_dir)

    # Configure DuckDB path via environment variable as backup
    monkeypatch.setenv(
        "DESTINATION__DUCKDB__CREDENTIALS", str(test_dbs_dir / "test.duckdb")
    )

    yield

    # Return to original directory
    os.chdir(original_dir)

    # Cleanup: remove test state directories from both locations
    for check_dir in [state_dir, test_dbs_dir / ".dlt"]:
        if check_dir.exists():
            try:
                shutil.rmtree(check_dir)
            except Exception:
                pass  # Best effort cleanup


# Check for optional dependencies
try:
    import duckdb

    DUCKDB_AVAILABLE: bool = True
except ImportError:
    DUCKDB_AVAILABLE = False

try:
    import psycopg2

    POSTGRES_DRIVER_AVAILABLE: bool = True
except ImportError:
    POSTGRES_DRIVER_AVAILABLE = False
