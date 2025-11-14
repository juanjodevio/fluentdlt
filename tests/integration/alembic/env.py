"""Alembic environment configuration for fldt integration tests.

This module configures Alembic to work with multiple database backends
(SQLite, PostgreSQL) for integration testing.
"""

import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import create_engine, pool

# this is the Alembic Config object, which provides access to the values within the .ini file
config = context.config

# Interpret the config file for Python logging
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Target metadata (we don't use models, migrations define schema directly)
target_metadata = None


def get_url():
    """Get database URL from environment or config.
    
    Environment variable TEST_DATABASE_URL takes precedence over alembic.ini.
    This allows tests to override the database URL dynamically.
    """
    return os.getenv("TEST_DATABASE_URL") or config.get_main_option("sqlalchemy.url")


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL and not an Engine.
    By skipping the Engine creation we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the script output.
    """
    url = get_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine and associate a connection with the context.
    """
    url = get_url()
    
    # Create engine with appropriate pooling
    # For SQLite, use NullPool to avoid locking issues
    # For PostgreSQL, use default pooling
    if url.startswith("sqlite"):
        connectable = create_engine(url, poolclass=pool.NullPool)
    else:
        connectable = create_engine(url)

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()

