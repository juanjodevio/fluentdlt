"""Postgres-to-Postgres integration tests."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest
from sqlalchemy import create_engine, text

from fldt import FluentPipeline

from .test_data import TestData

pytestmark = [pytest.mark.integration, pytest.mark.postgres]


def _fetch_rows(
    url: str,
    schema: str,
    table: str,
) -> list[dict[str, Any]]:
    """Fetch all rows from the provided PostgreSQL table."""
    engine = create_engine(url)
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(f'SELECT * FROM "{schema}"."{table}" ORDER BY id')
            )
            return [dict(row._mapping) for row in result]
    finally:
        engine.dispose()


def _insert_user(
    url: str,
    *,
    user_id: int,
    name: str,
    email: str,
    updated_at: str,
) -> None:
    engine = create_engine(url)
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO users (id, name, email, age, is_active, updated_at)
                    VALUES (:id, :name, :email, 21, TRUE, :updated_at)
                    """
                ),
                {"id": user_id, "name": name, "email": email, "updated_at": updated_at},
            )
    finally:
        engine.dispose()


def test_pg_to_pg_full_database_load(
    pg_connection_urls: tuple[str, str],
    postgres_destination_credentials: None,
    reset_postgres_destination: str,
) -> None:
    """End-to-end replication from Postgres source to Postgres destination."""
    source_url, dest_url = pg_connection_urls
    _ = reset_postgres_destination
    pipeline = (
        FluentPipeline.from_sql_database(source_url)
        .to("postgres")
        .with_dataset("fldt_pg_to_pg_full")
        .with_name("pg_to_pg_full_load")
    )

    result = pipeline.run()
    assert result is not None

    users = _fetch_rows(dest_url, "fldt_pg_to_pg_full", "users")
    events = _fetch_rows(dest_url, "fldt_pg_to_pg_full", "events")
    products = _fetch_rows(dest_url, "fldt_pg_to_pg_full", "products")

    assert len(users) == TestData.TOTAL_USERS
    assert len(events) == TestData.TOTAL_EVENTS
    assert len(products) == TestData.TOTAL_PRODUCTS


def test_pg_to_pg_incremental_users(
    pg_connection_urls: tuple[str, str],
    postgres_destination_credentials: None,
    reset_postgres_destination: str,
) -> None:
    pytest.skip("Incremental loading via DltAdapter pending implementation")
    """Verify incremental cursor logic on Postgres destination."""
    source_url, dest_url = pg_connection_urls
    dataset = "fldt_pg_to_pg_incremental"

    _ = reset_postgres_destination
    pipeline = (
        FluentPipeline.from_sql_table(source_url, "users")
        .with_incremental(
            cursor_field="updated_at",
            initial_value=datetime(2024, 1, 3, tzinfo=timezone.utc),
        )
        .to("postgres")
        .with_dataset(dataset)
        .with_name("pg_to_pg_incremental_users")
    )

    first_run = pipeline.run()
    assert first_run is not None

    users = _fetch_rows(dest_url, dataset, "users")
    assert len(users) == TestData.TOTAL_USERS

    _insert_user(
        source_url,
        user_id=999,
        name="Frank",
        email="frank@example.com",
        updated_at="2024-01-06 10:00:00",
    )

    second_run = pipeline.run()
    assert second_run is not None
    users_after_second_run = _fetch_rows(dest_url, dataset, "users")
    assert len(users_after_second_run) == TestData.TOTAL_USERS + 1
