"""Pytest fixtures for integration tests.

This module provides database fixtures that set up test databases using Alembic
migrations. Supports both SQLite (default, fast) and PostgreSQL (optional).
"""

import os
import shutil
import tempfile
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config


@pytest.fixture(scope="session")
def alembic_config():
    """Get Alembic configuration object.
    
    Returns Alembic Config pointing to integration test migrations.
    """
    # Get path to alembic.ini
    integration_dir = Path(__file__).parent.absolute()
    alembic_ini = integration_dir / "alembic.ini"
    
    config = Config(str(alembic_ini))
    
    # Set absolute path to script location
    script_location = integration_dir / "alembic"
    config.set_main_option("script_location", str(script_location))
    
    return config


@pytest.fixture(scope="session")
def sqlite_database(alembic_config):
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
def postgres_database(alembic_config):
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
        pytest.skip("PostgreSQL not available. Set TEST_POSTGRES_URL environment variable.")
    
    # Try to connect to PostgreSQL
    try:
        from sqlalchemy import create_engine
        
        # Test connection
        engine = create_engine(postgres_url)
        with engine.connect() as conn:
            conn.execute("SELECT 1")  # type: ignore
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
def test_database(sqlite_database):
    """Get default test database (SQLite).
    
    This is the default database fixture that most tests should use.
    It's fast and requires no external setup.
    
    Yields:
        str: Database connection string
    """
    return sqlite_database


@pytest.fixture(scope="session")
def test_dbs_dir():
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
def clean_dlt_state(test_dbs_dir, monkeypatch):
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
    monkeypatch.setenv("DESTINATION__DUCKDB__CREDENTIALS", str(test_dbs_dir / "test.duckdb"))
    
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
    
    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False

try:
    import psycopg2
    
    POSTGRES_DRIVER_AVAILABLE = True
except ImportError:
    POSTGRES_DRIVER_AVAILABLE = False
