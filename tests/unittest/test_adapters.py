"""Unit tests for fldt adapter modules."""

from __future__ import annotations

from typing import Any, Callable, cast
from unittest.mock import MagicMock, Mock, patch

import pytest

from fldt.adapters.dlt_adapter import DltAdapter
from fldt.adapters.protocol import PipelineAdapter
from fldt.exceptions import AdapterError, PipelineExecutionError, ValidationError
from fldt.types import IncrementalConfig, PipelineConfig

Transformer = Callable[[Any], Any]


class TestPipelineAdapterProtocol:
    """Test PipelineAdapter protocol definition."""

    def test_protocol_has_required_methods(self) -> None:
        """PipelineAdapter protocol requires specific methods."""
        # Check that protocol has the expected methods
        assert hasattr(PipelineAdapter, "create_pipeline")
        assert hasattr(PipelineAdapter, "run_pipeline")
        assert hasattr(PipelineAdapter, "apply_transformations")
        assert hasattr(PipelineAdapter, "prepare_source_with_incremental")

    def test_custom_adapter_implements_protocol(self) -> None:
        """Custom class implementing protocol methods works."""

        class CustomAdapter:
            def create_pipeline(self, config: PipelineConfig) -> Any:
                return {"type": "custom", "config": config}

            def run_pipeline(self, pipeline: Any, source: Any) -> Any:
                return {"status": "success"}

            def apply_transformations(
                self, source: Any, transformers: list[Any]
            ) -> Any:
                return source

            def prepare_source_with_incremental(
                self, source: Any, incremental_config: dict[str, Any] | None
            ) -> Any:
                return source

        # Should work as PipelineAdapter due to structural typing
        adapter = cast(PipelineAdapter, CustomAdapter())
        assert hasattr(adapter, "create_pipeline")
        assert hasattr(adapter, "run_pipeline")
        assert hasattr(adapter, "apply_transformations")


class TestDltAdapterInitialization:
    """Test DltAdapter initialization and setup."""

    def test_adapter_initializes_without_dlt(self) -> None:
        """DltAdapter can be created without dlt being loaded."""
        adapter = DltAdapter()
        assert adapter._dlt is None
        assert adapter._dlt_sources is None

    def test_lazy_loading_loads_dlt_on_first_use(self) -> None:
        """DLT modules are loaded on first use."""
        mock_dlt = MagicMock()
        mock_dlt.sources = MagicMock()

        adapter = DltAdapter()

        # dlt not loaded yet
        assert adapter._dlt is None

        # Trigger lazy loading
        with patch.dict(
            "sys.modules", {"dlt": mock_dlt, "dlt.sources": mock_dlt.sources}
        ):
            adapter._ensure_dlt_loaded()

        # Now loaded
        assert adapter._dlt is not None

    def test_lazy_loading_fails_gracefully_if_dlt_missing(self) -> None:
        """Lazy loading raises AdapterError if dlt is not installed."""
        adapter = DltAdapter()

        # Mock the import to fail
        with patch.dict("sys.modules", {"dlt": None}):
            with pytest.raises(AdapterError) as exc_info:
                adapter._ensure_dlt_loaded()

            assert "not installed" in str(exc_info.value)
            assert exc_info.value.__cause__ is not None

    def test_lazy_loading_only_happens_once(self) -> None:
        """DLT modules are only loaded once."""
        mock_dlt = MagicMock()
        mock_dlt.sources = MagicMock()
        adapter = DltAdapter()

        # Load first time
        with patch.dict(
            "sys.modules", {"dlt": mock_dlt, "dlt.sources": mock_dlt.sources}
        ):
            adapter._ensure_dlt_loaded()
            first_dlt = adapter._dlt

            adapter._ensure_dlt_loaded()  # Second call
            second_dlt = adapter._dlt

        # Should be the same object (no re-import)
        assert first_dlt is second_dlt
        assert adapter._dlt is not None


class TestDltAdapterPipelineCreation:
    """Test DltAdapter pipeline creation."""

    def test_create_pipeline_requires_destination(self) -> None:
        """create_pipeline raises ValidationError if no destination."""
        adapter = DltAdapter()
        config: PipelineConfig = {
            "source": [],
            "destination": "",  # Empty destination
            "transformers": [],
            "incremental": None,
            "pipeline_name": None,
            "dataset_name": None,
            "options": {},
        }

        with pytest.raises(ValidationError) as exc_info:
            adapter.create_pipeline(config)

        assert "Destination must be set" in str(exc_info.value)

    def test_create_pipeline_with_minimal_config(self) -> None:
        """create_pipeline works with minimal configuration."""
        mock_dlt = MagicMock()
        mock_dlt.sources = MagicMock()
        mock_pipeline = MagicMock()
        mock_dlt.pipeline.return_value = mock_pipeline

        adapter = DltAdapter()
        with patch.dict(
            "sys.modules", {"dlt": mock_dlt, "dlt.sources": mock_dlt.sources}
        ):
            adapter._ensure_dlt_loaded()

        config: PipelineConfig = {
            "source": [],
            "destination": "duckdb",
            "transformers": [],
            "incremental": None,
            "pipeline_name": None,
            "dataset_name": None,
            "options": {},
        }

        result = adapter.create_pipeline(config)

        assert result == mock_pipeline
        mock_dlt.pipeline.assert_called_once()
        call_args = mock_dlt.pipeline.call_args[1]
        assert call_args["destination"] == "duckdb"
        assert call_args["pipeline_name"] == "fldt_pipeline"  # default

    def test_create_pipeline_with_full_config(self) -> None:
        """create_pipeline uses all configuration options."""
        mock_dlt = MagicMock()
        mock_dlt.sources = MagicMock()
        mock_pipeline = MagicMock()
        mock_dlt.pipeline.return_value = mock_pipeline

        adapter = DltAdapter()
        with patch.dict(
            "sys.modules", {"dlt": mock_dlt, "dlt.sources": mock_dlt.sources}
        ):
            adapter._ensure_dlt_loaded()

        config: PipelineConfig = {
            "source": [],
            "destination": "postgres",
            "transformers": [],
            "incremental": None,
            "pipeline_name": "my_pipeline",
            "dataset_name": "my_dataset",
            "options": {"dev_mode": True, "write_disposition": "replace"},
        }

        result = adapter.create_pipeline(config)

        assert result == mock_pipeline
        call_args = mock_dlt.pipeline.call_args[1]
        assert call_args["pipeline_name"] == "my_pipeline"
        assert call_args["destination"] == "postgres"
        assert call_args["dataset_name"] == "my_dataset"
        assert call_args["dev_mode"] is True
        assert call_args["write_disposition"] == "replace"

    def test_create_pipeline_handles_dlt_errors(self) -> None:
        """create_pipeline raises AdapterError on dlt errors."""
        mock_dlt = MagicMock()
        mock_dlt.sources = MagicMock()
        mock_dlt.pipeline.side_effect = Exception("DLT internal error")

        adapter = DltAdapter()
        with patch.dict(
            "sys.modules", {"dlt": mock_dlt, "dlt.sources": mock_dlt.sources}
        ):
            adapter._ensure_dlt_loaded()

        config: PipelineConfig = {
            "source": [],
            "destination": "bigquery",
            "transformers": [],
            "incremental": None,
            "pipeline_name": None,
            "dataset_name": None,
            "options": {},
        }

        with pytest.raises(AdapterError) as exc_info:
            adapter.create_pipeline(config)

        assert "Failed to create dlt pipeline" in str(exc_info.value)
        assert isinstance(exc_info.value.__cause__, Exception)


class TestDltAdapterPipelineExecution:
    """Test DltAdapter pipeline execution."""

    def test_run_pipeline_executes_successfully(self) -> None:
        """run_pipeline executes and returns results."""
        adapter = DltAdapter()
        adapter._dlt = MagicMock()  # Mock as loaded

        mock_pipeline = MagicMock()
        mock_result = MagicMock()
        mock_result.loads_ids = ["load_1", "load_2"]
        mock_pipeline.run.return_value = mock_result
        wrapped_source = MagicMock()
        adapter._dlt.resource.return_value = wrapped_source

        source = [{"id": 1}, {"id": 2}]
        result = adapter.run_pipeline(mock_pipeline, source)

        assert result == mock_result
        adapter._dlt.resource.assert_called_once_with(source, name="transformed_data")
        mock_pipeline.run.assert_called_once_with(wrapped_source)

    def test_run_pipeline_handles_execution_errors(self) -> None:
        """run_pipeline raises PipelineExecutionError on failure."""
        adapter = DltAdapter()
        adapter._dlt = MagicMock()  # Mock as loaded

        mock_pipeline = MagicMock()
        mock_pipeline.run.side_effect = RuntimeError("Execution failed")
        adapter._dlt.resource.return_value = MagicMock()

        source = [{"id": 1}]

        with pytest.raises(PipelineExecutionError) as exc_info:
            adapter.run_pipeline(mock_pipeline, source)

        assert "Pipeline execution failed" in str(exc_info.value)
        assert isinstance(exc_info.value.__cause__, RuntimeError)


class TestDltAdapterTransformations:
    """Test DltAdapter transformation handling."""

    def test_apply_transformations_with_no_transformers(self) -> None:
        """apply_transformations returns source unchanged if no transformers."""
        adapter = DltAdapter()
        source = [{"id": 1}, {"id": 2}]
        transformers: list[Any] = []

        result = adapter.apply_transformations(source, transformers)

        assert result is source  # Should be exact same object

    def test_apply_transformations_applies_single_transformer(self) -> None:
        """apply_transformations applies single transformer correctly."""
        adapter = DltAdapter()
        source = [{"value": 10}]

        def double_value(data: list[dict[str, int]]) -> list[dict[str, int]]:
            return [{"value": item["value"] * 2} for item in data]

        result = adapter.apply_transformations(source, [double_value])

        assert result == [{"value": 20}]

    def test_apply_transformations_chains_multiple_transformers(self) -> None:
        """apply_transformations chains transformers in sequence."""
        adapter = DltAdapter()
        source = [{"value": 5}]

        def add_ten(data: list[dict[str, int]]) -> list[dict[str, int]]:
            return [{"value": item["value"] + 10} for item in data]

        def multiply_two(data: list[dict[str, int]]) -> list[dict[str, int]]:
            return [{"value": item["value"] * 2} for item in data]

        result = adapter.apply_transformations(source, [add_ten, multiply_two])

        # Should apply add_ten first (5 + 10 = 15), then multiply_two (15 * 2 = 30)
        assert result == [{"value": 30}]

    def test_apply_transformations_validates_callable(self) -> None:
        """apply_transformations raises error if transformer not callable."""
        adapter = DltAdapter()
        source = [{"id": 1}]
        not_callable: Any = "not a function"

        with pytest.raises(PipelineExecutionError) as exc_info:
            adapter.apply_transformations(source, [not_callable])

        assert "not callable" in str(exc_info.value)

    def test_apply_transformations_handles_transformer_errors(self) -> None:
        """apply_transformations raises PipelineExecutionError on transformer failure."""
        adapter = DltAdapter()
        source = [{"id": 1}]

        def failing_transformer(data: list[dict[str, int]]) -> list[dict[str, int]]:
            raise ValueError("Transformer failed")

        with pytest.raises(PipelineExecutionError) as exc_info:
            adapter.apply_transformations(source, [failing_transformer])

        assert "Transformation 1 failed" in str(exc_info.value)
        assert isinstance(exc_info.value.__cause__, ValueError)

    def test_apply_transformations_reports_correct_transformer_index(self) -> None:
        """apply_transformations reports which transformer failed."""
        adapter = DltAdapter()
        source = [{"id": 1}]

        def transformer1(data: list[dict[str, int]]) -> list[dict[str, int]]:
            return data

        def transformer2(data: list[dict[str, int]]) -> list[dict[str, int]]:
            raise RuntimeError("Second transformer fails")

        def transformer3(data: list[dict[str, int]]) -> list[dict[str, int]]:
            return data

        with pytest.raises(PipelineExecutionError) as exc_info:
            adapter.apply_transformations(
                source, [transformer1, transformer2, transformer3]
            )

        assert "Transformation 2 failed" in str(exc_info.value)

    def test_apply_transformations_with_dlt_resource(self) -> None:
        """apply_transformations applies transformers to dlt resource at record level."""
        mock_dlt = MagicMock()
        mock_dlt.sources = MagicMock()

        # Create a mock dlt resource
        mock_resource = MagicMock()
        mock_resource.__name__ = "test_resource"
        mock_resource.name = "test_resource"

        # Mock the transformer decorator
        mock_transformer_result = MagicMock()
        mock_transformer_result.__name__ = "transformed_resource"
        mock_dlt.transformer.return_value = lambda func: func
        # Make transformer return the function itself (simplified mock)

        adapter = DltAdapter()
        with patch.dict(
            "sys.modules", {"dlt": mock_dlt, "dlt.sources": mock_dlt.sources}
        ):
            adapter._ensure_dlt_loaded()

        # Transformer that modifies records
        def add_field(record: dict[str, Any]) -> dict[str, Any]:
            """Add a field to a single record."""
            return {**record, "transformed": True}

        # Apply transformation
        result = adapter.apply_transformations(mock_resource, [add_field])

        # Should have called dlt.transformer
        assert mock_dlt.transformer.called
        # Result should be the transformed resource (not the original)
        assert result is not mock_resource

    def test_apply_transformations_with_dlt_resource_chaining(self) -> None:
        """apply_transformations chains multiple transformers on dlt resource."""
        mock_dlt = MagicMock()
        mock_dlt.sources = MagicMock()

        # Create a mock dlt resource
        mock_resource = MagicMock()
        mock_resource.__name__ = "test_resource"
        mock_resource.name = "test_resource"

        # Track transformer calls
        transformer_calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

        def mock_transformer_decorator(*args: Any, **kwargs: Any) -> Any:
            """Mock transformer decorator that tracks calls."""
            transformer_calls.append((args, kwargs))

            def decorator(func: Any) -> Any:
                # Return a mock transformed resource
                mock_transformed = MagicMock()
                mock_transformed.__name__ = func.__name__
                return mock_transformed

            return decorator

        mock_dlt.transformer.side_effect = mock_transformer_decorator

        adapter = DltAdapter()
        with patch.dict(
            "sys.modules", {"dlt": mock_dlt, "dlt.sources": mock_dlt.sources}
        ):
            adapter._ensure_dlt_loaded()

        def transformer1(record: dict[str, Any]) -> dict[str, Any]:
            return {**record, "step1": True}

        def transformer2(record: dict[str, Any]) -> dict[str, Any]:
            return {**record, "step2": True}

        # Apply multiple transformations
        result = adapter.apply_transformations(
            mock_resource, [transformer1, transformer2]
        )

        # Should have called transformer twice (once for each transformer)
        assert len(transformer_calls) == 2
        # Result should be the final transformed resource
        assert result is not None

    def test_apply_transformations_detects_dlt_resource(self) -> None:
        """_is_dlt_resource correctly identifies dlt resources vs raw data."""
        mock_dlt = MagicMock()
        mock_dlt.sources = MagicMock()

        adapter = DltAdapter()
        with patch.dict(
            "sys.modules", {"dlt": mock_dlt, "dlt.sources": mock_dlt.sources}
        ):
            adapter._ensure_dlt_loaded()

        # Raw data should not be detected as dlt resource
        assert not adapter._is_dlt_resource([{"id": 1}])
        assert not adapter._is_dlt_resource({"key": "value"})
        assert not adapter._is_dlt_resource(("item",))

        # Mock dlt resource should be detected
        mock_resource = MagicMock()
        mock_resource.__name__ = "test_resource"
        mock_resource.name = "test_resource"
        assert adapter._is_dlt_resource(mock_resource)

        # Mock dlt source (has resources attribute)
        mock_source = MagicMock()
        mock_source.resources = [mock_resource]
        assert adapter._is_dlt_resource(mock_source)

    def test_apply_transformations_preserves_raw_data_behavior(self) -> None:
        """apply_transformations still works with raw data as before."""
        adapter = DltAdapter()
        source = [{"value": 10}]

        def double_value(data: list[dict[str, int]]) -> list[dict[str, int]]:
            return [{"value": item["value"] * 2} for item in data]

        result = adapter.apply_transformations(source, [double_value])

        # Should work exactly as before for raw data
        assert result == [{"value": 20}]
        # Should not be the same object (new list created)
        assert result is not source

    def test_apply_transformations_with_pandas_dataframe(self) -> None:
        """apply_transformations treats pandas DataFrame as raw data."""
        try:
            import pandas as pd  # type: ignore[import-untyped]
        except ImportError:
            pytest.skip("pandas not available")

        adapter = DltAdapter()
        df = pd.DataFrame({"value": [10, 20, 30]})

        def double_value(data: pd.DataFrame) -> pd.DataFrame:
            """Double the values in the DataFrame."""
            result_df = data.copy()
            result_df["value"] = result_df["value"] * 2
            return result_df

        result = adapter.apply_transformations(df, [double_value])

        # Should be treated as raw data (not dlt resource)
        assert isinstance(result, pd.DataFrame)
        assert not adapter._is_dlt_resource(result)
        # Verify transformation was applied
        assert result["value"].tolist() == [20, 40, 60]

    def test_apply_transformations_with_pandas_series(self) -> None:
        """apply_transformations treats pandas Series as raw data."""
        try:
            import pandas as pd
        except ImportError:
            pytest.skip("pandas not available")

        adapter = DltAdapter()
        series = pd.Series([10, 20, 30])

        def double_value(data: pd.Series) -> pd.Series:
            """Double the values in the Series."""
            return data * 2

        result = adapter.apply_transformations(series, [double_value])

        # Should be treated as raw data (not dlt resource)
        assert isinstance(result, pd.Series)
        assert not adapter._is_dlt_resource(result)
        # Verify transformation was applied
        assert result.tolist() == [20, 40, 60]

    def test_is_dlt_resource_detects_pandas_dataframe(self) -> None:
        """_is_dlt_resource correctly identifies pandas DataFrame as raw data."""
        try:
            import pandas as pd
        except ImportError:
            pytest.skip("pandas not available")

        adapter = DltAdapter()
        df = pd.DataFrame({"value": [10, 20, 30]})

        # pandas DataFrame should not be detected as dlt resource
        assert not adapter._is_dlt_resource(df)

    def test_is_dlt_resource_detects_pandas_series(self) -> None:
        """_is_dlt_resource correctly identifies pandas Series as raw data."""
        try:
            import pandas as pd
        except ImportError:
            pytest.skip("pandas not available")

        adapter = DltAdapter()
        series = pd.Series([10, 20, 30])

        # pandas Series should not be detected as dlt resource
        assert not adapter._is_dlt_resource(series)

    def test_apply_transformations_with_dlt_resource_receives_records(self) -> None:
        """Transformers applied to dlt resources receive individual records."""
        mock_dlt = MagicMock()
        mock_dlt.sources = MagicMock()

        # Create a mock dlt resource that yields records
        records_received: list[Any] = []

        def mock_transformer_decorator(*args: Any, **kwargs: Any) -> Any:
            """Mock transformer decorator."""

            def decorator(func: Any) -> Any:
                # Wrap the function to track what it receives
                def wrapper(items: Any) -> Any:
                    for item in items:
                        records_received.append(item)
                        # Call the user's transformer function
                        # The implementation should call it with the record
                        yield func(item) if callable(func) else item

                return wrapper

            return decorator

        mock_dlt.transformer.side_effect = mock_transformer_decorator

        adapter = DltAdapter()
        with patch.dict(
            "sys.modules", {"dlt": mock_dlt, "dlt.sources": mock_dlt.sources}
        ):
            adapter._ensure_dlt_loaded()

        # Create a mock resource
        mock_resource = MagicMock()
        mock_resource.__name__ = "test_resource"

        # Transformer that expects a single record (dict)
        def transform_record(record: dict[str, Any]) -> dict[str, Any]:
            """Transformer that receives a single record."""
            return {**record, "transformed": True}

        # Note: This test verifies the structure, but actual record processing
        # happens during pipeline execution. The key is that transformers
        # are set up to receive records, not the source object.
        result = adapter.apply_transformations(mock_resource, [transform_record])

        # Should have created transformer
        assert mock_dlt.transformer.called
        # Result should be transformed resource
        assert result is not None


class TestDltAdapterIncrementalLoading:
    """Test DltAdapter incremental loading configuration."""

    def test_prepare_source_without_incremental_config(self) -> None:
        """prepare_source_with_incremental returns source unchanged without config."""
        adapter = DltAdapter()
        source = [{"id": 1}]

        result = adapter.prepare_source_with_incremental(source, None)

        assert result is source

    def test_prepare_source_with_incremental_config(self) -> None:
        """prepare_source_with_incremental configures incremental loading."""
        mock_dlt = MagicMock()
        mock_dlt.sources = MagicMock()
        mock_incremental = MagicMock()
        mock_dlt.sources.incremental.return_value = mock_incremental
        mock_wrapped_source = MagicMock()

        adapter = DltAdapter()
        with patch.dict(
            "sys.modules", {"dlt": mock_dlt, "dlt.sources": mock_dlt.sources}
        ):
            adapter._ensure_dlt_loaded()

        source = MagicMock()
        source.with_incremental.return_value = mock_wrapped_source

        incremental_config: IncrementalConfig = {
            "cursor_field": "updated_at",
            "initial_value": "2024-01-01",
            "primary_key": "id",
            "allow_external_schedulers": False,
        }

        config_copy = cast(IncrementalConfig, dict(incremental_config))
        result = adapter.prepare_source_with_incremental(source, config_copy)

        assert result == mock_wrapped_source
        mock_dlt.sources.incremental.assert_called_once()
        call_args = mock_dlt.sources.incremental.call_args[1]
        assert call_args["cursor_path"] == "$.updated_at"
        assert call_args["initial_value"] == "2024-01-01"
        assert call_args["primary_key"] == "id"
        assert call_args["allow_external_schedulers"] is False
        source.with_incremental.assert_called_once_with(mock_incremental)
        mock_dlt.resource.assert_not_called()

    def test_prepare_source_with_minimal_incremental_config(self) -> None:
        """prepare_source_with_incremental works with only cursor_field."""
        mock_dlt = MagicMock()
        mock_dlt.sources = MagicMock()
        mock_incremental = MagicMock()
        mock_dlt.sources.incremental.return_value = mock_incremental
        mock_resource = MagicMock()
        mock_dlt.resource.return_value = mock_resource

        adapter = DltAdapter()
        with patch.dict(
            "sys.modules", {"dlt": mock_dlt, "dlt.sources": mock_dlt.sources}
        ):
            adapter._ensure_dlt_loaded()

        source = [{"updated_at": "2024-01-01"}]
        incremental_config: IncrementalConfig = {"cursor_field": "updated_at"}

        config_copy = cast(IncrementalConfig, dict(incremental_config))
        result = adapter.prepare_source_with_incremental(source, config_copy)

        assert result == mock_resource
        call_args = mock_dlt.sources.incremental.call_args[1]
        assert call_args["cursor_path"] == "$.updated_at"
        assert "initial_value" not in call_args
        assert "primary_key" not in call_args
        mock_dlt.resource.assert_called_once()
        _, resource_kwargs = mock_dlt.resource.call_args
        assert resource_kwargs["incremental"] == mock_incremental

    def test_prepare_source_handles_incremental_errors(self) -> None:
        """prepare_source_with_incremental raises AdapterError on failure."""
        mock_dlt = MagicMock()
        mock_dlt.sources = MagicMock()
        mock_dlt.sources.incremental.side_effect = Exception(
            "Incremental config failed"
        )

        adapter = DltAdapter()
        with patch.dict(
            "sys.modules", {"dlt": mock_dlt, "dlt.sources": mock_dlt.sources}
        ):
            adapter._ensure_dlt_loaded()

        source = [{"id": 1}]
        incremental_config: IncrementalConfig = {"cursor_field": "updated_at"}
        config_copy = cast(IncrementalConfig, dict(incremental_config))

        with pytest.raises(AdapterError) as exc_info:
            adapter.prepare_source_with_incremental(source, config_copy)

        assert "Failed to configure incremental loading" in str(exc_info.value)
        assert isinstance(exc_info.value.__cause__, Exception)

    def test_prepare_source_with_non_string_cursor_field(self) -> None:
        """prepare_source_with_incremental handles non-string cursor_field."""
        mock_dlt = MagicMock()
        mock_dlt.sources = MagicMock()
        mock_incremental = MagicMock()
        mock_dlt.sources.incremental.return_value = mock_incremental
        mock_resource = MagicMock()
        mock_dlt.resource.return_value = mock_resource

        adapter = DltAdapter()
        with patch.dict(
            "sys.modules", {"dlt": mock_dlt, "dlt.sources": mock_dlt.sources}
        ):
            adapter._ensure_dlt_loaded()

        source = [{"updated_at": "2024-01-01"}]
        # cursor_field as non-string (list or other type)
        incremental_config: IncrementalConfig = {
            "cursor_field": ["$.updated_at", "$.id"]  # type: ignore
        }

        config_copy = cast(IncrementalConfig, dict(incremental_config))
        result = adapter.prepare_source_with_incremental(source, config_copy)

        assert result == mock_resource
        call_args = mock_dlt.sources.incremental.call_args[1]
        assert call_args["cursor_path"] == ["$.updated_at", "$.id"]
        mock_dlt.resource.assert_called_once()

    def test_prepare_source_with_end_value(self) -> None:
        """prepare_source_with_incremental forwards end_value to dlt."""
        mock_dlt = MagicMock()
        mock_dlt.sources = MagicMock()
        mock_incremental = MagicMock()
        mock_dlt.sources.incremental.return_value = mock_incremental
        mock_resource = MagicMock()
        mock_dlt.resource.return_value = mock_resource

        adapter = DltAdapter()
        with patch.dict(
            "sys.modules", {"dlt": mock_dlt, "dlt.sources": mock_dlt.sources}
        ):
            adapter._ensure_dlt_loaded()

        source = [{"updated_at": "2024-01-01"}]
        incremental_config: IncrementalConfig = {
            "cursor_field": "updated_at",
            "initial_value": "2024-01-01",
            "end_value": "2024-12-31",
        }

        config_copy = cast(IncrementalConfig, dict(incremental_config))
        result = adapter.prepare_source_with_incremental(source, config_copy)

        assert result == mock_resource
        call_args = mock_dlt.sources.incremental.call_args[1]
        assert call_args["cursor_path"] == "$.updated_at"
        assert call_args["initial_value"] == "2024-01-01"
        assert call_args["end_value"] == "2024-12-31"

    def test_prepare_source_with_allow_external_schedulers(self) -> None:
        """prepare_source_with_incremental forwards allow_external_schedulers to dlt."""
        mock_dlt = MagicMock()
        mock_dlt.sources = MagicMock()
        mock_incremental = MagicMock()
        mock_dlt.sources.incremental.return_value = mock_incremental
        mock_resource = MagicMock()
        mock_dlt.resource.return_value = mock_resource

        adapter = DltAdapter()
        with patch.dict(
            "sys.modules", {"dlt": mock_dlt, "dlt.sources": mock_dlt.sources}
        ):
            adapter._ensure_dlt_loaded()

        source = [{"updated_at": "2024-01-01"}]
        incremental_config: IncrementalConfig = {
            "cursor_field": "updated_at",
            "allow_external_schedulers": True,
        }

        config_copy = cast(IncrementalConfig, dict(incremental_config))
        result = adapter.prepare_source_with_incremental(source, config_copy)

        assert result == mock_resource
        call_args = mock_dlt.sources.incremental.call_args[1]
        assert call_args["cursor_path"] == "$.updated_at"
        assert call_args["allow_external_schedulers"] is True

    def test_prepare_source_with_custom_kwargs(self) -> None:
        """prepare_source_with_incremental forwards custom kwargs to dlt."""
        mock_dlt = MagicMock()
        mock_dlt.sources = MagicMock()
        mock_incremental = MagicMock()
        mock_dlt.sources.incremental.return_value = mock_incremental
        mock_resource = MagicMock()
        mock_dlt.resource.return_value = mock_resource

        adapter = DltAdapter()
        with patch.dict(
            "sys.modules", {"dlt": mock_dlt, "dlt.sources": mock_dlt.sources}
        ):
            adapter._ensure_dlt_loaded()

        source = [{"updated_at": "2024-01-01"}]
        # Include custom kwargs that aren't in IncrementalConfig type
        incremental_config = {
            "cursor_field": "updated_at",
            "initial_value": "2024-01-01",
            "custom_option": "custom_value",
            "another_option": 42,
        }

        config_copy = cast(IncrementalConfig, dict(incremental_config))
        result = adapter.prepare_source_with_incremental(source, config_copy)

        assert result == mock_resource
        call_args = mock_dlt.sources.incremental.call_args[1]
        assert call_args["cursor_path"] == "$.updated_at"
        assert call_args["initial_value"] == "2024-01-01"
        assert call_args["custom_option"] == "custom_value"
        assert call_args["another_option"] == 42

    def test_prepare_source_with_all_incremental_fields(self) -> None:
        """prepare_source_with_incremental forwards all incremental config fields."""
        mock_dlt = MagicMock()
        mock_dlt.sources = MagicMock()
        mock_incremental = MagicMock()
        mock_dlt.sources.incremental.return_value = mock_incremental
        mock_resource = MagicMock()
        mock_dlt.resource.return_value = mock_resource

        adapter = DltAdapter()
        with patch.dict(
            "sys.modules", {"dlt": mock_dlt, "dlt.sources": mock_dlt.sources}
        ):
            adapter._ensure_dlt_loaded()

        source = [{"updated_at": "2024-01-01"}]
        incremental_config: IncrementalConfig = {
            "cursor_field": "updated_at",
            "initial_value": "2024-01-01",
            "end_value": "2024-12-31",
            "primary_key": "id",
            "allow_external_schedulers": True,
        }

        config_copy = cast(IncrementalConfig, dict(incremental_config))
        result = adapter.prepare_source_with_incremental(source, config_copy)

        assert result == mock_resource
        call_args = mock_dlt.sources.incremental.call_args[1]
        assert call_args["cursor_path"] == "$.updated_at"
        assert call_args["initial_value"] == "2024-01-01"
        assert call_args["end_value"] == "2024-12-31"
        assert call_args["primary_key"] == "id"
        assert call_args["allow_external_schedulers"] is True

    def test_prepare_source_for_pipeline_with_non_iterable_source(self) -> None:
        """_prepare_source_for_pipeline returns source unchanged if not iterable."""
        mock_dlt = MagicMock()

        adapter = DltAdapter()
        with patch.dict("sys.modules", {"dlt": mock_dlt}):
            adapter._ensure_dlt_loaded()

        # Source that is not list, tuple, or dict (e.g., a dlt resource object)
        mock_source = MagicMock()
        result = adapter._prepare_source_for_pipeline(mock_source)

        assert result is mock_source
        mock_dlt.resource.assert_not_called()

    def test_prepare_source_for_pipeline_with_iterable_source(self) -> None:
        """_prepare_source_for_pipeline wraps list/tuple/dict sources."""
        mock_dlt = MagicMock()
        mock_resource = MagicMock()
        mock_dlt.resource.return_value = mock_resource

        adapter = DltAdapter()
        with patch.dict("sys.modules", {"dlt": mock_dlt}):
            adapter._ensure_dlt_loaded()

        # Test with list
        source_list = [{"id": 1}]
        result = adapter._prepare_source_for_pipeline(source_list)
        assert result == mock_resource
        mock_dlt.resource.assert_called_once_with(source_list, name="transformed_data")

        mock_dlt.resource.reset_mock()
        # Test with tuple
        source_tuple = ({"id": 1},)
        result = adapter._prepare_source_for_pipeline(source_tuple)
        assert result == mock_resource
        mock_dlt.resource.assert_called_once_with(source_tuple, name="transformed_data")

        mock_dlt.resource.reset_mock()
        # Test with dict
        source_dict = {"table": [{"id": 1}]}
        result = adapter._prepare_source_for_pipeline(source_dict)
        assert result == mock_resource
        mock_dlt.resource.assert_called_once_with(source_dict, name="transformed_data")

    def test_ensure_dlt_loaded_handles_non_import_error(self) -> None:
        """_ensure_dlt_loaded raises AdapterError for non-ImportError exceptions."""
        adapter = DltAdapter()

        # Mock import to raise a non-ImportError exception
        with patch("builtins.__import__", side_effect=RuntimeError("Unexpected error")):
            with pytest.raises(AdapterError) as exc_info:
                adapter._ensure_dlt_loaded()

        assert "Failed to load dlt modules" in str(exc_info.value)
        assert isinstance(exc_info.value.__cause__, RuntimeError)
