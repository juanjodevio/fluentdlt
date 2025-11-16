"""Unit tests for fldt.fluent module."""

from __future__ import annotations

from typing import Any, Callable, Iterable, cast
from unittest.mock import MagicMock, Mock, patch

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

    @patch("sqlalchemy.create_engine")
    @patch("dlt.source")
    @patch("dlt.resource")
    def test_from_sql_query_with_query(
        self,
        mock_dlt_resource: Mock,
        mock_dlt_source: Mock,
        mock_create_engine: Mock,
    ) -> None:
        """from_sql_query() creates pipeline from SQL query."""
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine

        # dlt.resource is a decorator: @dlt.resource(name=...) calls dlt.resource(name=...)
        # which returns a decorator function that then wraps the actual function
        def identity_decorator(func: Any) -> Any:
            return func

        mock_dlt_resource.return_value = identity_decorator

        # dlt.source is also a decorator - when decorated function is called, it returns resources
        def source_func() -> Any:
            return identity_decorator(lambda: None)

        mock_dlt_source.return_value = lambda: source_func

        query = "SELECT * FROM users WHERE active = true"
        pipeline = FluentPipeline.from_sql_query(
            "postgresql://localhost/db", query, table_name="active_users"
        )

        assert pipeline._builder._source is not None
        mock_create_engine.assert_called_once_with("postgresql://localhost/db")
        # Verify resource was created with correct table_name
        mock_dlt_resource.assert_called_once()
        call_kwargs = (
            mock_dlt_resource.call_args[1]
            if len(mock_dlt_resource.call_args) > 1
            else {}
        )
        assert call_kwargs.get("name") == "active_users"
        # Verify source was created
        mock_dlt_source.assert_called_once()

    @patch("sqlalchemy.create_engine")
    @patch("dlt.source")
    @patch("dlt.resource")
    def test_from_sql_query_with_kwargs(
        self,
        mock_dlt_resource: Mock,
        mock_dlt_source: Mock,
        mock_create_engine: Mock,
    ) -> None:
        """from_sql_query() creates source with custom resource."""
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine

        # dlt.resource is a decorator, so it returns a decorator function
        def identity_decorator(func: Any) -> Any:
            return func

        mock_dlt_resource.return_value = identity_decorator
        mock_dlt_source.return_value = lambda: identity_decorator(lambda: None)

        FluentPipeline.from_sql_query(
            "postgresql://localhost/db",
            "SELECT * FROM users",
            table_name="users",
            backend="pyodbc",
        )

        # Verify source was created (kwargs are not passed to sql_database anymore)
        mock_dlt_source.assert_called_once()

    @patch("dlt.source")
    @patch("dlt.resource")
    def test_from_sql_query_with_engine_object(
        self, mock_dlt_resource: Mock, mock_dlt_source: Mock
    ) -> None:
        """from_sql_query() works with SQLAlchemy Engine object."""
        from sqlalchemy.engine import Engine

        mock_engine = Mock(spec=Engine)

        # dlt.resource is a decorator, so it returns a decorator function
        def identity_decorator(func: Any) -> Any:
            return func

        mock_dlt_resource.return_value = identity_decorator
        mock_dlt_source.return_value = lambda: identity_decorator(lambda: None)

        query = "SELECT * FROM users"
        pipeline = FluentPipeline.from_sql_query(mock_engine, query, table_name="users")

        assert pipeline._builder._source is not None
        # Verify source was created (no sql_database call)
        mock_dlt_source.assert_called_once()

    def test_from_sql_query_validates_query(self) -> None:
        """from_sql_query() validates query is non-empty string."""
        with pytest.raises(ValidationError) as exc_info:
            FluentPipeline.from_sql_query("conn", "", table_name="table")

        assert "non-empty string" in str(exc_info.value)

    def test_from_sql_query_validates_table_name(self) -> None:
        """from_sql_query() validates table_name is non-empty string."""
        with pytest.raises(ValidationError) as exc_info:
            FluentPipeline.from_sql_query("conn", "SELECT 1", table_name="")

        assert "table_name must be a non-empty string" in str(exc_info.value)

    def test_from_sql_query_raises_error_if_dlt_not_available(self) -> None:
        """from_sql_query() raises AdapterError if dlt not available."""
        with patch.dict("sys.modules", {"dlt.sources.sql_database": None}):
            with pytest.raises(AdapterError) as exc_info:
                FluentPipeline.from_sql_query("conn", "SELECT 1", table_name="table")

            assert "not available" in str(exc_info.value)

    def test_from_sql_query_validates_invalid_connection_type(self) -> None:
        """from_sql_query() validates connection is Engine or string."""
        invalid_connection: Any = 123  # Not a string or Engine

        with pytest.raises(ValidationError) as exc_info:
            FluentPipeline.from_sql_query(
                invalid_connection, "SELECT 1", table_name="table"
            )

        assert "Connection must be a SQLAlchemy Engine" in str(exc_info.value)

    def test_from_sql_query_validates_table_name_not_string(self) -> None:
        """from_sql_query() validates table_name is a string."""
        invalid_table_name: Any = 123  # Not a string

        with pytest.raises(ValidationError) as exc_info:
            FluentPipeline.from_sql_query(
                "conn", "SELECT 1", table_name=invalid_table_name
            )

        assert "table_name must be a non-empty string" in str(exc_info.value)

    def test_from_sql_query_validates_query_not_string(self) -> None:
        """from_sql_query() validates query is a string."""
        invalid_query: Any = 123  # Not a string

        with pytest.raises(ValidationError) as exc_info:
            FluentPipeline.from_sql_query("conn", invalid_query, table_name="table")

        assert "Query must be a non-empty string" in str(exc_info.value)

    def test_from_sql_query_executes_query_with_real_dlt(self) -> None:
        """from_sql_query() creates and executes query with real dlt (no mocks)."""
        from sqlalchemy import create_engine, text

        # Create a real engine for SQLite in-memory
        engine = create_engine("sqlite:///:memory:")
        with engine.connect() as conn:
            conn.execute(text("CREATE TABLE test (id INTEGER, name TEXT)"))
            conn.execute(text("INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob')"))
            conn.commit()

        # Create pipeline without mocks - this will execute the real code
        pipeline = FluentPipeline.from_sql_query(
            engine, "SELECT * FROM test WHERE id = 1", table_name="test_results"
        )

        # Verify the source was created
        assert pipeline._builder._source is not None

        # Access the source's resources to execute the query
        # This will execute lines 234-238 (the query execution)
        source = pipeline._builder._source

        # Get resources from the source
        if hasattr(source, "resources"):
            resources = source.resources
            # Find the test_results resource
            if "test_results" in resources:
                resource = resources["test_results"]
                # Execute the resource generator to cover the query execution
                rows = list(resource())
                assert len(rows) == 1
                assert rows[0]["id"] == 1
                assert rows[0]["name"] == "Alice"
            else:
                # Try to get the first resource if name doesn't match
                resource = next(iter(resources.values()))
                rows = list(resource())
                assert len(rows) == 1
                assert rows[0]["id"] == 1
                assert rows[0]["name"] == "Alice"


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


class TestFluentPipelineFromDf:
    """Test from_df factory method."""

    def test_from_df_with_dataframe(self) -> None:
        """from_df() creates pipeline from pandas DataFrame."""
        try:
            import pandas as pd  # type: ignore[import-untyped]
        except ImportError:
            pytest.skip("pandas not available")

        df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
        pipeline = FluentPipeline.from_df(df)

        # Source should be converted to records (list of dicts)
        source = pipeline._builder._source
        assert isinstance(source, list)
        source_list = cast(list[dict[str, Any]], source)
        assert len(source_list) == 2
        assert source_list[0] == {"id": 1, "name": "Alice"}
        assert source_list[1] == {"id": 2, "name": "Bob"}

    def test_from_df_with_empty_dataframe(self) -> None:
        """from_df() handles empty DataFrame."""
        try:
            import pandas as pd
        except ImportError:
            pytest.skip("pandas not available")

        df = pd.DataFrame()
        pipeline = FluentPipeline.from_df(df)

        source = pipeline._builder._source
        assert isinstance(source, list)
        assert len(source) == 0

    def test_from_df_validates_dataframe_type(self) -> None:
        """from_df() raises ValidationError for non-DataFrame input."""
        # Create a mock pandas module with a DataFrame class
        class MockDataFrame:
            """Mock DataFrame class."""

            pass

        mock_pd = MagicMock()
        mock_pd.DataFrame = MockDataFrame

        invalid_df: Any = [1, 2, 3]  # Not a DataFrame

        with patch.dict("sys.modules", {"pandas": mock_pd}):
            with pytest.raises(ValidationError) as exc_info:
                FluentPipeline.from_df(invalid_df)

            assert "Expected pandas DataFrame" in str(exc_info.value)

    def test_from_df_raises_error_if_pandas_not_available(self) -> None:
        """from_df() raises AdapterError if pandas not available."""
        with patch.dict("sys.modules", {"pandas": None}):
            with pytest.raises(AdapterError) as exc_info:
                FluentPipeline.from_df("not_a_df")

            assert "pandas is not installed" in str(exc_info.value)

    def test_from_df_preserves_dataframe_data(self) -> None:
        """from_df() correctly converts DataFrame columns and values."""
        try:
            import pandas as pd
        except ImportError:
            pytest.skip("pandas not available")

        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "score": [95.5, 87.0, 92.3],
                "active": [True, False, True],
            }
        )
        pipeline = FluentPipeline.from_df(df)

        source = pipeline._builder._source
        assert isinstance(source, list)
        source_list = cast(list[dict[str, Any]], source)
        assert len(source_list) == 3
        assert source_list[0]["id"] == 1
        assert source_list[0]["name"] == "Alice"
        assert source_list[0]["score"] == 95.5
        assert source_list[0]["active"] is True
        assert source_list[1]["active"] is False


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
            "updated_at",
            initial_value="2024-01-01",
            end_value="2024-02-01",
            primary_key="id",
        )

        config = pipeline._builder._incremental
        assert config is not None
        assert config["cursor_field"] == "updated_at"
        assert config["initial_value"] == "2024-01-01"
        assert config["end_value"] == "2024-02-01"
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
