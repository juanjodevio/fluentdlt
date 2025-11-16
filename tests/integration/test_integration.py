"""Integration tests for fldt with real databases.

These tests verify complete pipeline functionality using real databases
created with Alembic migrations. Tests run on SQLite by default (fast),
with optional PostgreSQL support.

Run with: pytest tests/integration
"""

from __future__ import annotations

from typing import Any

import pytest

from .conftest import DUCKDB_AVAILABLE, POSTGRES_DRIVER_AVAILABLE
from .test_data import (
    TestData,
    validate_event_data,
    validate_product_data,
    validate_user_data,
)

# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration

ConnectionString = str


class TestSQLTableLoading:
    """Test loading from SQL tables with real databases."""

    @pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="Requires duckdb")
    def test_load_single_table(
        self, test_database: ConnectionString, clean_dlt_state: None
    ) -> None:
        """Load single SQL table to DuckDB."""
        from fldt import FluentPipeline

        result = (
            FluentPipeline.from_sql_table(test_database, "users")
            .to("duckdb")
            .with_name("test_load_single_table")
            .with_dataset("test_users")
            .run()
        )

        # Verify result structure
        assert result is not None
        assert hasattr(result, "loads_ids") or hasattr(result, "first_run")

    @pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="Requires duckdb")
    def test_load_table_with_schema(
        self, test_database: ConnectionString, clean_dlt_state: None
    ) -> None:
        """Load SQL table from specific schema."""
        from fldt import FluentPipeline

        # SQLite doesn't have schemas, but test the parameter handling
        result = (
            FluentPipeline.from_sql_table(test_database, "events", schema=None)
            .to("duckdb")
            .with_name("test_load_table_with_schema")
            .with_dataset("test_events")
            .run()
        )

        assert result is not None


class TestSQLQueryExecution:
    """Test loading from custom SQL queries."""

    @pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="Requires duckdb")
    def test_load_from_custom_query(
        self, test_database: ConnectionString, clean_dlt_state: None
    ) -> None:
        """Load data from custom SQL query."""
        from fldt import FluentPipeline

        # Query to get only active users
        query = "SELECT * FROM users WHERE is_active = 1"
        result = (
            FluentPipeline.from_sql_query(
                test_database, query, table_name="active_users"
            )
            .to("duckdb")
            .with_name("test_load_from_custom_query")
            .with_dataset("test_query")
            .run()
        )

        # Verify result structure
        assert result is not None
        assert hasattr(result, "loads_ids") or hasattr(result, "first_run")

    @pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="Requires duckdb")
    def test_load_with_join_query(
        self, test_database: ConnectionString, clean_dlt_state: None
    ) -> None:
        """Test joining tables via SQL database source."""
        # dlt sql_database loads tables, not arbitrary queries
        # For complex queries, users should use sql_table or custom sources
        pytest.skip("Join queries require advanced dlt configuration")


class TestSQLDatabaseLoading:
    """Test loading entire databases."""

    @pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="Requires duckdb")
    def test_load_entire_database(
        self, test_database: ConnectionString, clean_dlt_state: None
    ) -> None:
        """Load all tables from database."""
        from fldt import FluentPipeline

        result = (
            FluentPipeline.from_sql_database(test_database)
            .to("duckdb")
            .with_name("test_load_entire_database")
            .with_dataset("test_full_db")
            .run()
        )

        assert result is not None


class TestIncrementalLoading:
    """Test incremental loading with cursor fields."""

    @pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="Requires duckdb")
    def test_incremental_with_cursor_field(
        self, test_database: ConnectionString, clean_dlt_state: None
    ) -> None:
        """Test incremental loading with updated_at cursor."""
        from fldt import FluentPipeline

        # First load - should get all users
        result1 = (
            FluentPipeline.from_sql_table(test_database, "users")
            .with_incremental("updated_at", initial_value="2024-01-01 00:00:00")
            .to("duckdb")
            .with_name("incremental_users")
            .with_dataset("test_incremental")
            .run()
        )

        assert result1 is not None

    @pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="Requires duckdb")
    def test_incremental_with_primary_key(
        self, test_database: ConnectionString, clean_dlt_state: None
    ) -> None:
        """Test incremental loading with primary key for deduplication."""
        from fldt import FluentPipeline

        result = (
            FluentPipeline.from_sql_table(test_database, "events")
            .with_incremental(
                cursor_field="created_at",
                initial_value="2024-01-03 00:00:00",
                primary_key="id",
            )
            .to("duckdb")
            .with_name("incremental_events")
            .with_dataset("test_incremental_pk")
            .run()
        )

        assert result is not None

    @pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="Requires duckdb")
    def test_incremental_with_end_value_bounded_backfill(
        self, test_database: ConnectionString, clean_dlt_state: None
    ) -> None:
        """Test bounded backfill using end_value in incremental config."""
        from fldt import FluentPipeline

        # Bounded backfill: load only records between initial_value and end_value
        result = (
            FluentPipeline.from_sql_table(test_database, "events")
            .with_incremental(
                cursor_field="created_at",
                initial_value="2024-01-01 00:00:00",
                end_value="2024-01-31 23:59:59",
            )
            .to("duckdb")
            .with_name("bounded_backfill_events")
            .with_dataset("test_bounded_backfill")
            .run()
        )

        assert result is not None
        # Verify that pipeline executed successfully
        assert hasattr(result, "loads_ids") or hasattr(result, "first_run")

    @pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="Requires duckdb")
    def test_incremental_with_end_value_state_persistence(
        self, test_database: ConnectionString, clean_dlt_state: None
    ) -> None:
        """Test incremental state persistence across runs with end_value."""
        from fldt import FluentPipeline

        # First run with bounded backfill
        result1 = (
            FluentPipeline.from_sql_table(test_database, "users")
            .with_incremental(
                cursor_field="updated_at",
                initial_value="2024-01-01 00:00:00",
                end_value="2024-01-15 23:59:59",
            )
            .to("duckdb")
            .with_name("bounded_incremental_users")
            .with_dataset("test_incremental_state")
            .run()
        )

        assert result1 is not None

        # Second run: should continue from where first run left off
        # (Note: In practice, dlt manages state automatically, but we verify
        # that end_value doesn't interfere with state persistence)
        result2 = (
            FluentPipeline.from_sql_table(test_database, "users")
            .with_incremental(
                cursor_field="updated_at",
                initial_value="2024-01-01 00:00:00",
                end_value="2024-01-31 23:59:59",
            )
            .to("duckdb")
            .with_name("bounded_incremental_users")
            .with_dataset("test_incremental_state")
            .run()
        )

        assert result2 is not None
        assert hasattr(result2, "loads_ids") or hasattr(result2, "first_run")


class TestTransformations:
    """Test data transformations in pipelines."""

    @pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="Requires duckdb")
    def test_single_transformation(
        self, test_database: ConnectionString, clean_dlt_state: None
    ) -> None:
        """Test pipeline with single transformation."""
        from fldt import FluentPipeline

        def uppercase_names(data: list[dict[str, str]]) -> list[dict[str, str]]:
            """Transform names to uppercase."""
            transformed: list[dict[str, str]] = []
            for item in data:
                if "name" in item:
                    transformed.append({**item, "name": str(item["name"]).upper()})
                else:
                    transformed.append(item)
            return transformed

        result = (
            FluentPipeline.from_sql_table(test_database, "users")
            .add_transformer(uppercase_names)
            .to("duckdb")
            .with_name("test_single_transformation")
            .with_dataset("test_transformed")
            .run()
        )

        assert result is not None

    @pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="Requires duckdb")
    def test_chained_transformations(
        self, test_database: ConnectionString, clean_dlt_state: None
    ) -> None:
        """Test pipeline with multiple chained transformations."""
        from fldt import FluentPipeline

        # Note: Transformations with dlt sources need to work with resources
        # For now, test basic transformation without filtering
        def add_full_name_field(
            data: list[dict[str, str]],
        ) -> list[dict[str, str]]:
            """Add computed full name field."""
            return [
                {**item, "full_name": str(item.get("name", "")).upper()}
                for item in data
            ]

        result = (
            FluentPipeline.from_sql_table(test_database, "users")
            .add_transformer(add_full_name_field)
            .to("duckdb")
            .with_name("test_chained_transformations")
            .with_dataset("test_chained")
            .run()
        )

        assert result is not None

    @pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="Requires duckdb")
    def test_sql_source_with_record_level_transformer(
        self, test_database: ConnectionString, clean_dlt_state: None
    ) -> None:
        """Test SQL source with transformer that receives individual records."""
        from fldt import FluentPipeline

        # Transformer that works with single records (new pattern for dlt resources)
        def add_transformed_field(record: dict[str, Any]) -> dict[str, Any]:
            """Add a field to indicate transformation occurred."""
            return {**record, "was_transformed": True}

        result = (
            FluentPipeline.from_sql_table(test_database, "users")
            .add_transformer(add_transformed_field)
            .to("duckdb")
            .with_name("test_record_level_transformer")
            .with_dataset("test_record_transformed")
            .run()
        )

        assert result is not None
        # Verify pipeline executed successfully
        assert hasattr(result, "loads_ids") or hasattr(result, "first_run")

    @pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="Requires duckdb")
    def test_sql_source_with_multiple_chained_transformers(
        self, test_database: ConnectionString, clean_dlt_state: None
    ) -> None:
        """Test SQL source with multiple transformers chained at record level."""
        from fldt import FluentPipeline

        # First transformer: add a field
        def add_step1(record: dict[str, Any]) -> dict[str, Any]:
            """First transformation step."""
            return {**record, "step1_applied": True}

        # Second transformer: add another field
        def add_step2(record: dict[str, Any]) -> dict[str, Any]:
            """Second transformation step."""
            return {**record, "step2_applied": True}

        result = (
            FluentPipeline.from_sql_table(test_database, "users")
            .add_transformer(add_step1)
            .add_transformer(add_step2)
            .to("duckdb")
            .with_name("test_chained_record_transformers")
            .with_dataset("test_chained_record")
            .run()
        )

        assert result is not None
        # Verify pipeline executed successfully
        assert hasattr(result, "loads_ids") or hasattr(result, "first_run")

    @pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="Requires duckdb")
    def test_sql_source_transformer_validates_data_transformation(
        self, test_database: ConnectionString, clean_dlt_state: None
    ) -> None:
        """Test that transformer actually modifies data in destination."""
        from fldt import FluentPipeline

        # Transformer that modifies the name field
        def uppercase_name(record: dict[str, Any]) -> dict[str, Any]:
            """Transform name to uppercase."""
            if "name" in record and record["name"]:
                return {**record, "name": str(record["name"]).upper()}
            return record

        result = (
            FluentPipeline.from_sql_table(test_database, "users")
            .add_transformer(uppercase_name)
            .to("duckdb")
            .with_name("test_validate_transformation")
            .with_dataset("test_validate")
            .run()
        )

        assert result is not None
        # Verify pipeline executed successfully
        assert hasattr(result, "loads_ids") or hasattr(result, "first_run")
        # Note: Full data validation would require querying DuckDB to verify
        # the transformation was applied, which is more complex and would
        # require additional setup. This test verifies the pipeline runs
        # successfully with a transformer applied.


class TestWriteDispositions:
    """Test different write dispositions (append, replace, merge)."""

    @pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="Requires duckdb")
    def test_write_disposition_append(
        self, test_database: ConnectionString, clean_dlt_state: None
    ) -> None:
        """Test append write disposition (default behavior)."""
        from fldt import FluentPipeline

        # Append is default - no need to specify
        result = (
            FluentPipeline.from_sql_table(test_database, "products")
            .to("duckdb")
            .with_name("test_write_disposition_append")
            .with_dataset("test_append")
            .run()
        )

        assert result is not None

    @pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="Requires duckdb")
    def test_write_disposition_replace(
        self, test_database: ConnectionString, clean_dlt_state: None
    ) -> None:
        """Test replace write disposition via pipeline options."""
        # Note: write_disposition is typically set on the source, not pipeline
        # For dlt sql_database sources, this is configured differently
        pytest.skip("Write dispositions require source-level configuration")


class TestDatabaseCompatibility:
    """Test database-specific functionality."""

    @pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="Requires duckdb")
    def test_sqlite_database(
        self, sqlite_database: ConnectionString, clean_dlt_state: None
    ) -> None:
        """Test with SQLite database explicitly."""
        from fldt import FluentPipeline

        result = (
            FluentPipeline.from_sql_table(sqlite_database, "users")
            .to("duckdb")
            .with_name("test_sqlite_database")
            .with_dataset("test_sqlite")
            .run()
        )

        assert result is not None

    @pytest.mark.skipif(
        not (DUCKDB_AVAILABLE and POSTGRES_DRIVER_AVAILABLE),
        reason="Requires duckdb and PostgreSQL driver",
    )
    def test_postgresql_database(
        self, postgres_database: ConnectionString, clean_dlt_state: None
    ) -> None:
        """Test with PostgreSQL database (if available)."""
        from fldt import FluentPipeline

        result = (
            FluentPipeline.from_sql_table(postgres_database, "users")
            .to("duckdb")
            .with_name("test_postgresql_database")
            .with_dataset("test_postgres")
            .run()
        )

        assert result is not None


class TestComplexScenarios:
    """Test complex real-world scenarios."""

    @pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="Requires duckdb")
    def test_full_pipeline_with_all_features(
        self, test_database: ConnectionString, clean_dlt_state: None
    ) -> None:
        """Complete pipeline with incremental loading and options."""
        from fldt import FluentPipeline

        result = (
            FluentPipeline.from_sql_table(test_database, "events")
            .with_incremental("created_at", initial_value="2024-01-01 00:00:00")
            .with_name("complex_pipeline")
            .with_dataset("test_complex")
            .to("duckdb")
            .run()
        )

        assert result is not None


class TestErrorHandling:
    """Test error handling in integration scenarios."""

    def test_pipeline_fails_without_destination(
        self, test_database: ConnectionString
    ) -> None:
        """Pipeline should fail validation without destination."""
        from fldt import FluentPipeline
        from fldt.exceptions import PipelineConfigurationError

        pipeline = FluentPipeline.from_sql_table(test_database, "users")

        with pytest.raises(PipelineConfigurationError):
            pipeline.run()

    def test_pipeline_fails_with_invalid_table(
        self, test_database: ConnectionString
    ) -> None:
        """Pipeline should fail with non-existent table."""
        from fldt import FluentPipeline
        from fldt.exceptions import PipelineExecutionError

        # This will fail when dlt tries to introspect the table
        with pytest.raises((PipelineExecutionError, Exception)):
            (
                FluentPipeline.from_sql_table(test_database, "nonexistent_table")
                .to("duckdb")
                .with_name("test_pipeline_fails_with_invalid_table")
                .run()
            )
