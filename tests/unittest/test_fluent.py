"""Unit tests for fldt.fluent module."""

from __future__ import annotations

from typing import Any, Callable, Iterable
from unittest.mock import Mock, patch

import pytest

from fldt.exceptions import AdapterError, PipelineConfigurationError, ValidationError
from fldt.fluent import FluentPipeline

Transformer = Callable[[Any], Any]


class TestFluentPipelineInitialization:
    """Test FluentPipeline initialization."""

    def test_pipeline_initializes_with_new_builder(self) -> None:
        """FluentPipeline initializes with a new builder by default."""
        pipeline = FluentPipeline()
        assert pipeline._builder is not None

    def test_pipeline_initializes_with_provided_builder(self) -> None:
        """FluentPipeline can be initialized with existing builder."""
        from fldt.builder import PipelineBuilder

        builder = PipelineBuilder()
        builder.set_source([1, 2, 3])

        pipeline = FluentPipeline(builder)
        assert pipeline._builder is builder


class TestFluentPipelineFromSource:
    """Test from_source factory method."""

    def test_from_source_with_raw_data(self) -> None:
        """from_source() creates pipeline from raw data."""
        data = [{"id": 1}, {"id": 2}]
        pipeline = FluentPipeline.from_source(data)

        assert pipeline._builder._source is data

    def test_from_source_with_callable(self) -> None:
        """from_source() creates pipeline from callable."""

        def data_func() -> list[int]:
            return [1, 2, 3]

        pipeline = FluentPipeline.from_source(data_func)
        assert pipeline._builder._source is data_func

    def test_from_source_validates_none(self) -> None:
        """from_source() raises ValidationError for None."""
        invalid_source: Any = None

        with pytest.raises(ValidationError) as exc_info:
            FluentPipeline.from_source(invalid_source)

        assert "cannot be None" in str(exc_info.value)


class TestFluentPipelineFromSqlTable:
    """Test from_sql_table factory method."""

    @patch("dlt.sources.sql_database.sql_table")
    def test_from_sql_table_with_minimal_params(self, mock_sql_table: Mock) -> None:
        """from_sql_table() works with connection and table only."""
        mock_sql_table.return_value = "mock_source"

        pipeline = FluentPipeline.from_sql_table("postgresql://localhost/db", "users")

        assert pipeline._builder._source == "mock_source"
        mock_sql_table.assert_called_once_with(
            credentials="postgresql://localhost/db",
            table="users",
            schema=None,
        )

    @patch("dlt.sources.sql_database.sql_table")
    def test_from_sql_table_with_schema(self, mock_sql_table: Mock) -> None:
        """from_sql_table() includes schema when provided."""
        mock_sql_table.return_value = "mock_source"

        FluentPipeline.from_sql_table(
            "postgresql://localhost/db", "users", schema="public"
        )

        mock_sql_table.assert_called_once_with(
            credentials="postgresql://localhost/db",
            table="users",
            schema="public",
        )

    @patch("dlt.sources.sql_database.sql_table")
    def test_from_sql_table_with_kwargs(self, mock_sql_table: Mock) -> None:
        """from_sql_table() passes additional kwargs to dlt."""
        mock_sql_table.return_value = "mock_source"

        FluentPipeline.from_sql_table(
            "postgresql://localhost/db", "users", chunk_size=1000, backend="sqlalchemy"
        )

        call_kwargs = mock_sql_table.call_args[1]
        assert call_kwargs["chunk_size"] == 1000
        assert call_kwargs["backend"] == "sqlalchemy"

    def test_from_sql_table_raises_error_if_dlt_not_available(self) -> None:
        """from_sql_table() raises AdapterError if dlt not available."""
        with patch.dict("sys.modules", {"dlt.sources.sql_database": None}):
            with pytest.raises(AdapterError) as exc_info:
                FluentPipeline.from_sql_table("conn", "table")

            assert "not available" in str(exc_info.value)


class TestFluentPipelineFromSqlQuery:
    """Test from_sql_query factory method."""

    @patch("dlt.sources.sql_database.sql_database")
    def test_from_sql_query_with_query(self, mock_sql_database: Mock) -> None:
        """from_sql_query() creates pipeline from SQL query."""
        mock_source = Mock()
        mock_source.with_resources.return_value = "mock_source_with_query"
        mock_sql_database.return_value = mock_source

        query = "SELECT * FROM users WHERE active = true"
        pipeline = FluentPipeline.from_sql_query("postgresql://localhost/db", query)

        assert pipeline._builder._source == "mock_source_with_query"
        mock_sql_database.assert_called_once_with(
            credentials="postgresql://localhost/db"
        )
        mock_source.with_resources.assert_called_once_with(query)

    @patch("dlt.sources.sql_database.sql_database")
    def test_from_sql_query_with_kwargs(self, mock_sql_database: Mock) -> None:
        """from_sql_query() passes kwargs to dlt."""
        mock_source = Mock()
        mock_source.with_resources.return_value = "mock_source"
        mock_sql_database.return_value = mock_source

        FluentPipeline.from_sql_query(
            "postgresql://localhost/db", "SELECT * FROM users", backend="pyodbc"
        )

        call_kwargs = mock_sql_database.call_args[1]
        assert call_kwargs["backend"] == "pyodbc"

    def test_from_sql_query_validates_query(self) -> None:
        """from_sql_query() validates query is non-empty string."""
        with pytest.raises(ValidationError) as exc_info:
            FluentPipeline.from_sql_query("conn", "")

        assert "non-empty string" in str(exc_info.value)

    def test_from_sql_query_raises_error_if_dlt_not_available(self) -> None:
        """from_sql_query() raises AdapterError if dlt not available."""
        with patch.dict("sys.modules", {"dlt.sources.sql_database": None}):
            with pytest.raises(AdapterError) as exc_info:
                FluentPipeline.from_sql_query("conn", "SELECT 1")

            assert "not available" in str(exc_info.value)


class TestFluentPipelineFromSqlDatabase:
    """Test from_sql_database factory method."""

    @patch("dlt.sources.sql_database.sql_database")
    def test_from_sql_database_without_schema(self, mock_sql_database: Mock) -> None:
        """from_sql_database() creates pipeline for entire database."""
        mock_sql_database.return_value = "mock_source"

        pipeline = FluentPipeline.from_sql_database("postgresql://localhost/db")

        assert pipeline._builder._source == "mock_source"
        mock_sql_database.assert_called_once_with(
            credentials="postgresql://localhost/db",
            schema=None,
        )

    @patch("dlt.sources.sql_database.sql_database")
    def test_from_sql_database_with_schema(self, mock_sql_database: Mock) -> None:
        """from_sql_database() includes schema when provided."""
        mock_sql_database.return_value = "mock_source"

        FluentPipeline.from_sql_database("postgresql://localhost/db", schema="public")

        mock_sql_database.assert_called_once_with(
            credentials="postgresql://localhost/db",
            schema="public",
        )

    @patch("dlt.sources.sql_database.sql_database")
    def test_from_sql_database_with_kwargs(self, mock_sql_database: Mock) -> None:
        """from_sql_database() passes kwargs to dlt."""
        mock_sql_database.return_value = "mock_source"

        FluentPipeline.from_sql_database(
            "postgresql://localhost/db", table_names=["users", "orders"]
        )

        call_kwargs = mock_sql_database.call_args[1]
        assert call_kwargs["table_names"] == ["users", "orders"]

    def test_from_sql_database_raises_error_if_dlt_not_available(self) -> None:
        """from_sql_database() raises AdapterError if dlt not available."""
        with patch.dict("sys.modules", {"dlt.sources.sql_database": None}):
            with pytest.raises(AdapterError) as exc_info:
                FluentPipeline.from_sql_database("conn")

            assert "not available" in str(exc_info.value)


class TestFluentPipelineTo:
    """Test to() method for setting destination."""

    def test_to_sets_destination(self) -> None:
        """to() sets the destination."""
        pipeline = FluentPipeline().from_source([1])

        result = pipeline.to("duckdb")

        assert result is pipeline  # Returns self
        assert pipeline._builder._destination == "duckdb"

    def test_to_validates_destination(self) -> None:
        """to() validates destination."""
        pipeline = FluentPipeline()

        with pytest.raises(ValidationError):
            pipeline.to("")


class TestFluentPipelineAddTransformer:
    """Test add_transformer() method."""

    def test_add_transformer_adds_function(self) -> None:
        """add_transformer() adds transformation function."""
        pipeline = FluentPipeline()

        def transformer(x: int) -> int:
            return x * 2

        result = pipeline.add_transformer(transformer)

        assert result is pipeline
        assert transformer in pipeline._builder._transformers

    def test_add_transformer_validates_callable(self) -> None:
        """add_transformer() validates transformer is callable."""
        pipeline = FluentPipeline()

        invalid_transformer: Any = "not callable"

        with pytest.raises(ValidationError):
            pipeline.add_transformer(invalid_transformer)


class TestFluentPipelineWithIncremental:
    """Test with_incremental() method."""

    def test_with_incremental_sets_config(self) -> None:
        """with_incremental() configures incremental loading."""
        pipeline = FluentPipeline()

        result = pipeline.with_incremental("updated_at")

        assert result is pipeline
        config = pipeline._builder._incremental
        assert config is not None
        assert config["cursor_field"] == "updated_at"

    def test_with_incremental_with_all_params(self) -> None:
        """with_incremental() accepts all parameters."""
        pipeline = FluentPipeline()

        pipeline.with_incremental(
            "updated_at", initial_value="2024-01-01", primary_key="id"
        )

        config = pipeline._builder._incremental
        assert config is not None
        assert config["cursor_field"] == "updated_at"
        assert config["initial_value"] == "2024-01-01"
        assert config["primary_key"] == "id"

    def test_with_incremental_validates_params(self) -> None:
        """with_incremental() validates parameters."""
        pipeline = FluentPipeline()

        with pytest.raises(ValidationError):
            pipeline.with_incremental("")  # Empty cursor_field


class TestFluentPipelineWithName:
    """Test with_name() method."""

    def test_with_name_sets_pipeline_name(self) -> None:
        """with_name() sets the pipeline name."""
        pipeline = FluentPipeline()

        result = pipeline.with_name("my_pipeline")

        assert result is pipeline
        assert pipeline._builder._pipeline_name == "my_pipeline"

    def test_with_name_validates_name(self) -> None:
        """with_name() validates name."""
        pipeline = FluentPipeline()

        with pytest.raises(ValidationError):
            pipeline.with_name("")


class TestFluentPipelineWithDataset:
    """Test with_dataset() method."""

    def test_with_dataset_sets_dataset_name(self) -> None:
        """with_dataset() sets the dataset name."""
        pipeline = FluentPipeline()

        result = pipeline.with_dataset("my_dataset")

        assert result is pipeline
        assert pipeline._builder._dataset_name == "my_dataset"

    def test_with_dataset_validates_name(self) -> None:
        """with_dataset() validates name."""
        pipeline = FluentPipeline()

        with pytest.raises(ValidationError):
            pipeline.with_dataset("")


class TestFluentPipelineWithOptions:
    """Test with_options() method."""

    def test_with_options_sets_options(self) -> None:
        """with_options() sets pipeline options."""
        pipeline = FluentPipeline()

        result = pipeline.with_options(dev_mode=True, write_disposition="replace")

        assert result is pipeline
        assert pipeline._builder._options["dev_mode"] is True
        assert pipeline._builder._options["write_disposition"] == "replace"


class TestFluentPipelineRun:
    """Test run() method."""

    def test_run_builds_and_executes_pipeline(self) -> None:
        """run() builds config and executes via executor."""
        mock_adapter = Mock()
        mock_adapter.create_pipeline.return_value = "mock_pipeline"
        mock_adapter.run_pipeline.return_value = "mock_result"
        mock_adapter.prepare_source_with_incremental.side_effect = (
            lambda source, _: source
        )

        pipeline = FluentPipeline.from_source([1, 2, 3]).to("duckdb")
        result = pipeline.run(mock_adapter)

        assert result == "mock_result"
        mock_adapter.create_pipeline.assert_called_once()
        mock_adapter.run_pipeline.assert_called_once()

    def test_run_uses_default_adapter_if_none_provided(self) -> None:
        """run() creates DltAdapter if none provided."""
        with patch("fldt.fluent.DltAdapter") as mock_dlt_adapter_class:
            mock_adapter = Mock()
            mock_adapter.create_pipeline.return_value = "mock_pipeline"
            mock_adapter.run_pipeline.return_value = "mock_result"
            mock_adapter.prepare_source_with_incremental.side_effect = (
                lambda source, _: source
            )
            mock_dlt_adapter_class.return_value = mock_adapter

            pipeline = FluentPipeline.from_source([1]).to("duckdb")
            result = pipeline.run()

            mock_dlt_adapter_class.assert_called_once()
            assert result == "mock_result"

    def test_run_requires_source_and_destination(self) -> None:
        """run() raises error if configuration is incomplete."""
        pipeline = FluentPipeline.from_source([1])
        # No destination set

        with pytest.raises(PipelineConfigurationError):
            pipeline.run(Mock())


class TestFluentPipelineMethodChaining:
    """Test method chaining functionality."""

    def test_full_method_chain(self) -> None:
        """All methods support chaining."""
        mock_adapter = Mock()
        mock_adapter.create_pipeline.return_value = "mock_pipeline"
        mock_adapter.run_pipeline.return_value = "mock_result"
        mock_adapter.prepare_source_with_incremental.side_effect = (
            lambda source, _: source
        )

        result = (
            FluentPipeline.from_source([1, 2, 3])
            .to("duckdb")
            .add_transformer(lambda x: x)
            .with_incremental("updated_at")
            .with_name("test_pipeline")
            .with_dataset("test_dataset")
            .with_options(dev_mode=True)
            .run(mock_adapter)
        )

        assert result == "mock_result"

    @patch("dlt.sources.sql_database.sql_table")
    def test_sql_table_method_chain(self, mock_sql_table: Mock) -> None:
        """SQL convenience methods support chaining."""
        mock_sql_table.return_value = "mock_source"
        mock_adapter = Mock()
        mock_adapter.create_pipeline.return_value = "mock_pipeline"
        mock_adapter.run_pipeline.return_value = "mock_result"
        mock_adapter.prepare_source_with_incremental.side_effect = (
            lambda source, _: source
        )

        result = (
            FluentPipeline.from_sql_table("postgresql://localhost/db", "users")
            .add_transformer(lambda x: x)
            .with_incremental("updated_at")
            .to("duckdb")
            .run(mock_adapter)
        )

        assert result == "mock_result"


class TestFluentPipelineIntegration:
    """Test integration scenarios."""

    @patch("dlt.sources.sql_database.sql_table")
    def test_complete_sql_pipeline_workflow(self, mock_sql_table: Mock) -> None:
        """Complete workflow from SQL table to destination."""
        mock_sql_table.return_value = [{"id": 1}, {"id": 2}]
        mock_adapter = Mock()
        mock_adapter.create_pipeline.return_value = "mock_pipeline"
        mock_adapter.run_pipeline.return_value = {"status": "success"}
        mock_adapter.prepare_source_with_incremental.side_effect = (
            lambda source, _: source
        )

        result = (
            FluentPipeline.from_sql_table(
                "postgresql://localhost/db", "users", schema="public"
            )
            .add_transformer(lambda data: [item for item in data if item["id"] > 0])
            .with_incremental("updated_at", initial_value="2024-01-01")
            .with_name("user_sync")
            .with_dataset("analytics")
            .to("duckdb")
            .run(mock_adapter)
        )

        assert result == {"status": "success"}

    def test_pipeline_with_multiple_transformers(self) -> None:
        """Pipeline with multiple transformers applied in sequence."""
        mock_adapter = Mock()
        mock_adapter.create_pipeline.return_value = "mock_pipeline"
        mock_adapter.run_pipeline.return_value = "result"
        mock_adapter.prepare_source_with_incremental.side_effect = (
            lambda source, _: source
        )

        def add_ten(data: Iterable[int]) -> list[int]:
            return [x + 10 for x in data]

        def multiply_two(data: Iterable[int]) -> list[int]:
            return [x * 2 for x in data]

        def apply_chain(source: Iterable[int], transformers: list[Any]) -> list[int]:
            result = list(source)
            for transformer in transformers:
                result = transformer(result)
            return result

        mock_adapter.apply_transformations.side_effect = apply_chain

        pipeline = (
            FluentPipeline.from_source([1, 2, 3])
            .add_transformer(add_ten)
            .add_transformer(multiply_two)
            .to("duckdb")
        )

        pipeline.run(mock_adapter)

        # Verify transformers were applied: [(1+10)*2, (2+10)*2, (3+10)*2] = [22, 24, 26]
        call_args = mock_adapter.run_pipeline.call_args
        transformed_data = call_args[0][1]
        assert transformed_data == [22, 24, 26]


class TestFluentPipelineRepr:
    """Test string representation."""

    def test_repr_shows_builder_state(self) -> None:
        """repr() shows pipeline and builder state."""
        pipeline = FluentPipeline.from_source([1]).to("duckdb")
        repr_str = repr(pipeline)

        assert "FluentPipeline" in repr_str
        assert "builder=" in repr_str
