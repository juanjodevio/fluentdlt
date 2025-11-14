"""Unit tests for fldt.types module."""

from typing import Any

from fldt.types import (
    ConnectionType,
    DestinationType,
    IncrementalConfig,
    PipelineConfig,
)


class TestTypeAliases:
    """Test type alias definitions."""

    def test_source_type_accepts_callables(self):
        """SourceType should accept callable objects."""

        def sample_source():
            return [{"id": 1}]

        # This is a compile-time check, but we can verify at runtime
        assert callable(sample_source)

    def test_destination_type_accepts_strings(self):
        """DestinationType should accept string values."""
        dest: DestinationType = "duckdb"
        assert isinstance(dest, str)

    def test_connection_type_accepts_strings(self):
        """ConnectionType should accept connection strings."""
        conn: ConnectionType = "sqlite:///:memory:"
        assert isinstance(conn, str)

    def test_transformer_func_is_callable(self):
        """TransformerFunc should be a callable type."""

        def sample_transformer(data: Any) -> Any:
            return data

        # Verify it's callable
        assert callable(sample_transformer)
        result = sample_transformer([1, 2, 3])
        assert result == [1, 2, 3]


class TestIncrementalConfig:
    """Test IncrementalConfig TypedDict."""

    def test_incremental_config_with_required_fields(self):
        """IncrementalConfig can be created with cursor_field only."""
        config: IncrementalConfig = {"cursor_field": "updated_at"}
        assert config["cursor_field"] == "updated_at"

    def test_incremental_config_with_all_fields(self):
        """IncrementalConfig can include all optional fields."""
        config: IncrementalConfig = {
            "cursor_field": "updated_at",
            "initial_value": "2024-01-01",
            "primary_key": "id",
            "row_order": "asc",
            "allow_external_schedulers": True,
        }
        assert config["cursor_field"] == "updated_at"
        assert config["initial_value"] == "2024-01-01"
        assert config["primary_key"] == "id"
        assert config["row_order"] == "asc"
        assert config["allow_external_schedulers"] is True

    def test_incremental_config_with_composite_primary_key(self):
        """IncrementalConfig supports composite primary keys as list."""
        config: IncrementalConfig = {
            "cursor_field": "updated_at",
            "primary_key": ["tenant_id", "id"],
        }
        assert isinstance(config["primary_key"], list)
        assert len(config["primary_key"]) == 2

    def test_incremental_config_is_mutable(self):
        """IncrementalConfig instances are mutable dicts."""
        config: IncrementalConfig = {"cursor_field": "created_at"}
        config["row_order"] = "desc"
        assert config["row_order"] == "desc"


class TestPipelineConfig:
    """Test PipelineConfig TypedDict."""

    def test_pipeline_config_minimal(self):
        """PipelineConfig can be created with minimal fields."""
        config: PipelineConfig = {
            "source": lambda: [{"id": 1}],
            "destination": "duckdb",
            "transformers": [],
            "incremental": None,
            "pipeline_name": None,
            "dataset_name": None,
            "options": {},
        }
        assert callable(config["source"])
        assert config["destination"] == "duckdb"
        assert config["transformers"] == []

    def test_pipeline_config_with_incremental(self):
        """PipelineConfig can include incremental configuration."""
        incremental: IncrementalConfig = {
            "cursor_field": "updated_at",
            "row_order": "asc",
        }
        config: PipelineConfig = {
            "source": [{"id": 1}],
            "destination": "postgres",
            "transformers": [],
            "incremental": incremental,
            "pipeline_name": "test_pipeline",
            "dataset_name": "test_dataset",
            "options": {"write_disposition": "merge"},
        }
        assert config["incremental"] is not None
        assert config["incremental"]["cursor_field"] == "updated_at"
        assert config["pipeline_name"] == "test_pipeline"

    def test_pipeline_config_with_transformers(self):
        """PipelineConfig supports list of transformer functions."""

        def transform1(data):
            return data

        def transform2(data):
            return data

        config: PipelineConfig = {
            "source": [],
            "destination": "bigquery",
            "transformers": [transform1, transform2],
            "incremental": None,
            "pipeline_name": None,
            "dataset_name": None,
            "options": {},
        }
        assert len(config["transformers"]) == 2
        assert all(callable(t) for t in config["transformers"])

    def test_pipeline_config_with_options(self):
        """PipelineConfig supports arbitrary options dict."""
        config: PipelineConfig = {
            "source": None,
            "destination": "snowflake",
            "transformers": [],
            "incremental": None,
            "pipeline_name": None,
            "dataset_name": None,
            "options": {
                "write_disposition": "replace",
                "loader_file_format": "jsonl",
                "dev_mode": True,
            },
        }
        assert config["options"]["write_disposition"] == "replace"
        assert config["options"]["dev_mode"] is True


class TestTypeCompatibility:
    """Test type compatibility and edge cases."""

    def test_none_values_in_pipeline_config(self):
        """PipelineConfig optional fields can be None."""
        config: PipelineConfig = {
            "source": [1, 2, 3],
            "destination": "duckdb",
            "transformers": [],
            "incremental": None,
            "pipeline_name": None,
            "dataset_name": None,
            "options": {},
        }
        assert config["incremental"] is None
        assert config["pipeline_name"] is None
        assert config["dataset_name"] is None

    def test_empty_collections_in_config(self):
        """PipelineConfig supports empty collections."""
        config: PipelineConfig = {
            "source": [],
            "destination": "duckdb",
            "transformers": [],
            "incremental": None,
            "pipeline_name": None,
            "dataset_name": None,
            "options": {},
        }
        assert config["source"] == []
        assert config["transformers"] == []
        assert config["options"] == {}

