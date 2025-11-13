"""Integration tests for fldt with actual dlt.

These tests verify the complete pipeline flow with real dlt integration.
They require dlt[sql_database] to be installed.
"""

import pytest

# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration


class TestBasicPipelineIntegration:
    """Test basic pipeline functionality with dlt."""

    def test_pipeline_from_raw_data_to_duckdb(self):
        """Complete pipeline from raw data to DuckDB."""
        from fldt import FluentPipeline
        
        # Create test data
        data = [
            {"id": 1, "name": "Alice", "score": 95},
            {"id": 2, "name": "Bob", "score": 87},
            {"id": 3, "name": "Charlie", "score": 92},
        ]
        
        # Build and run pipeline
        result = (FluentPipeline
            .from_source(data)
            .to("duckdb")
            .with_name("test_pipeline")
            .with_dataset("test_data")
            .run())
        
        # Verify result structure
        assert result is not None
        assert hasattr(result, "loads_ids") or hasattr(result, "first_run")

    def test_pipeline_with_transformations(self):
        """Pipeline with data transformations."""
        from fldt import FluentPipeline
        
        data = [
            {"name": "alice", "age": 25},
            {"name": "bob", "age": 30},
        ]
        
        def uppercase_names(records):
            """Transform names to uppercase."""
            for record in records:
                if "name" in record:
                    record["name"] = record["name"].upper()
            return records
        
        result = (FluentPipeline
            .from_source(data)
            .add_transformer(uppercase_names)
            .to("duckdb")
            .with_dataset("transformed_data")
            .run())
        
        assert result is not None


class TestSQLSourceIntegration:
    """Test SQL source integration.
    
    Note: These tests require a running database and proper credentials.
    They are skipped by default and should be run manually with:
    pytest tests/test_integration.py -m integration -k sql
    """

    @pytest.mark.skip(reason="Requires database credentials")
    def test_from_sql_table(self):
        """Test loading from SQL table."""
        from fldt import FluentPipeline
        
        # This would require actual database credentials
        connection_string = "postgresql://user:pass@localhost/testdb"
        
        result = (FluentPipeline
            .from_sql_table(connection_string, "users")
            .to("duckdb")
            .run())
        
        assert result is not None

    @pytest.mark.skip(reason="Requires database credentials")
    def test_from_sql_query(self):
        """Test loading from SQL query."""
        from fldt import FluentPipeline
        
        connection_string = "postgresql://user:pass@localhost/testdb"
        query = "SELECT * FROM users WHERE active = true"
        
        result = (FluentPipeline
            .from_sql_query(connection_string, query)
            .to("duckdb")
            .run())
        
        assert result is not None


class TestIncrementalLoadingIntegration:
    """Test incremental loading scenarios."""

    @pytest.mark.skip(reason="Requires database with timestamp columns")
    def test_incremental_loading_with_cursor(self):
        """Test incremental loading with cursor field."""
        from fldt import FluentPipeline
        
        connection_string = "postgresql://user:pass@localhost/testdb"
        
        result = (FluentPipeline
            .from_sql_table(connection_string, "events")
            .with_incremental(
                cursor_field="created_at",
                initial_value="2024-01-01"
            )
            .to("duckdb")
            .run())
        
        assert result is not None


class TestErrorHandlingIntegration:
    """Test error handling in real scenarios."""

    def test_pipeline_fails_without_destination(self):
        """Pipeline should fail validation without destination."""
        from fldt import FluentPipeline
        from fldt.exceptions import PipelineConfigurationError
        
        pipeline = FluentPipeline.from_source([1, 2, 3])
        
        with pytest.raises(PipelineConfigurationError):
            pipeline.run()

    def test_pipeline_fails_with_invalid_transformer(self):
        """Pipeline should fail with non-callable transformer."""
        from fldt import FluentPipeline
        from fldt.exceptions import ValidationError
        
        with pytest.raises(ValidationError):
            (FluentPipeline
                .from_source([1, 2, 3])
                .add_transformer("not a function")  # type: ignore
                .to("duckdb"))


# Pytest configuration for integration tests
def pytest_configure(config):
    """Configure custom markers for integration tests."""
    config.addinivalue_line(
        "markers", "integration: mark test as integration test (requires dlt)"
    )

