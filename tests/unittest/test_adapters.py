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

        adapter = DltAdapter()
        with patch.dict(
            "sys.modules", {"dlt": mock_dlt, "dlt.sources": mock_dlt.sources}
        ):
            adapter._ensure_dlt_loaded()

        source = [{"id": 1, "updated_at": "2024-01-01"}]
        incremental_config: IncrementalConfig = {
            "cursor_field": "updated_at",
            "initial_value": "2024-01-01",
            "primary_key": "id",
            "row_order": "asc",
            "allow_external_schedulers": False,
        }

        result = adapter.prepare_source_with_incremental(
            source, dict(incremental_config)
        )

        assert result == mock_incremental
        mock_dlt.sources.incremental.assert_called_once()
        call_args = mock_dlt.sources.incremental.call_args[1]
        assert call_args["cursor_path"] == "updated_at"
        assert call_args["initial_value"] == "2024-01-01"
        assert call_args["primary_key"] == "id"

    def test_prepare_source_with_minimal_incremental_config(self) -> None:
        """prepare_source_with_incremental works with only cursor_field."""
        mock_dlt = MagicMock()
        mock_dlt.sources = MagicMock()
        mock_incremental = MagicMock()
        mock_dlt.sources.incremental.return_value = mock_incremental

        adapter = DltAdapter()
        with patch.dict(
            "sys.modules", {"dlt": mock_dlt, "dlt.sources": mock_dlt.sources}
        ):
            adapter._ensure_dlt_loaded()

        source = [{"updated_at": "2024-01-01"}]
        incremental_config: IncrementalConfig = {"cursor_field": "updated_at"}

        result = adapter.prepare_source_with_incremental(
            source, dict(incremental_config)
        )

        assert result == mock_incremental
        call_args = mock_dlt.sources.incremental.call_args[1]
        assert call_args["cursor_path"] == "updated_at"
        assert "initial_value" not in call_args
        assert "primary_key" not in call_args

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

        with pytest.raises(AdapterError) as exc_info:
            adapter.prepare_source_with_incremental(source, dict(incremental_config))

        assert "Failed to configure incremental loading" in str(exc_info.value)
        assert isinstance(exc_info.value.__cause__, Exception)
