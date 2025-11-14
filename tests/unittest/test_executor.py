"""Unit tests for fldt.executor module."""

from __future__ import annotations

from typing import Any, Iterable
from unittest.mock import Mock

import pytest

from fldt.exceptions import PipelineConfigurationError, PipelineExecutionError
from fldt.executor import PipelineExecutor
from fldt.types import PipelineConfig


def _apply_transformers(source: Any, transformers: list[Any]) -> Any:
    result = source
    for transformer in transformers:
        result = transformer(result)
    return result


class TestPipelineExecutorInitialization:
    """Test PipelineExecutor initialization."""

    def test_executor_initializes_with_adapter(self) -> None:
        """PipelineExecutor initializes with an adapter."""
        adapter = Mock()
        executor = PipelineExecutor(adapter)

        assert executor._adapter is adapter

    def test_executor_rejects_none_adapter(self) -> None:
        """PipelineExecutor raises error for None adapter."""
        with pytest.raises(PipelineConfigurationError) as exc_info:
            PipelineExecutor(None)  # type: ignore

        assert "Adapter cannot be None" in str(exc_info.value)


class TestPipelineExecutorExecute:
    """Test pipeline execution."""

    def test_execute_with_minimal_config(self) -> None:
        """execute() runs pipeline with minimal configuration."""
        adapter = Mock()
        adapter.create_pipeline.return_value = "mock_pipeline"
        adapter.run_pipeline.return_value = "mock_result"
        adapter.apply_transformations.side_effect = _apply_transformers

        executor = PipelineExecutor(adapter)
        config: PipelineConfig = {
            "source": [{"id": 1}],
            "destination": "duckdb",
            "transformers": [],
            "incremental": None,
            "pipeline_name": None,
            "dataset_name": None,
            "options": {},
        }

        result = executor.execute(config)

        assert result == "mock_result"
        adapter.create_pipeline.assert_called_once_with(config)
        adapter.apply_transformations.assert_called_once_with(
            config["source"], config["transformers"]
        )
        adapter.run_pipeline.assert_called_once()

    def test_execute_with_transformers(self) -> None:
        """execute() applies transformers before running pipeline."""
        adapter = Mock()
        adapter.create_pipeline.return_value = "mock_pipeline"
        adapter.run_pipeline.return_value = "mock_result"
        adapter.apply_transformations.side_effect = _apply_transformers

        executor = PipelineExecutor(adapter)

        # Transformer that doubles values
        def double(data: Iterable[int]) -> list[int]:
            return [item * 2 for item in data]

        config: PipelineConfig = {
            "source": [1, 2, 3],
            "destination": "duckdb",
            "transformers": [double],
            "incremental": None,
            "pipeline_name": None,
            "dataset_name": None,
            "options": {},
        }

        executor.execute(config)

        # Check that run_pipeline was called with transformed data
        call_args = adapter.run_pipeline.call_args
        transformed_source = call_args[0][1]
        assert transformed_source == [2, 4, 6]

    def test_execute_with_multiple_transformers(self) -> None:
        """execute() chains multiple transformers."""
        adapter = Mock()
        adapter.create_pipeline.return_value = "mock_pipeline"
        adapter.run_pipeline.return_value = "mock_result"
        adapter.apply_transformations.side_effect = _apply_transformers

        executor = PipelineExecutor(adapter)

        def add_ten(data: Iterable[int]) -> list[int]:
            return [item + 10 for item in data]

        def multiply_two(data: Iterable[int]) -> list[int]:
            return [item * 2 for item in data]

        config: PipelineConfig = {
            "source": [1, 2, 3],
            "destination": "duckdb",
            "transformers": [add_ten, multiply_two],  # (x + 10) * 2
            "incremental": None,
            "pipeline_name": None,
            "dataset_name": None,
            "options": {},
        }

        executor.execute(config)

        # Should be [22, 24, 26]: [(1+10)*2, (2+10)*2, (3+10)*2]
        call_args = adapter.run_pipeline.call_args
        transformed_source = call_args[0][1]
        assert transformed_source == [22, 24, 26]

    def test_execute_without_transformers(self) -> None:
        """execute() works without transformers."""
        adapter = Mock()
        adapter.create_pipeline.return_value = "mock_pipeline"
        adapter.run_pipeline.return_value = "mock_result"
        adapter.apply_transformations.side_effect = _apply_transformers

        executor = PipelineExecutor(adapter)
        config: PipelineConfig = {
            "source": [1, 2, 3],
            "destination": "duckdb",
            "transformers": [],
            "incremental": None,
            "pipeline_name": None,
            "dataset_name": None,
            "options": {},
        }

        executor.execute(config)

        # Source should be passed unchanged
        call_args = adapter.run_pipeline.call_args
        source = call_args[0][1]
        assert source == [1, 2, 3]

    def test_execute_calls_adapter_in_correct_order(self) -> None:
        """execute() calls adapter methods in correct sequence."""
        adapter = Mock()
        adapter.create_pipeline.return_value = "mock_pipeline"
        adapter.run_pipeline.return_value = "mock_result"
        adapter.apply_transformations.side_effect = _apply_transformers

        executor = PipelineExecutor(adapter)
        config: PipelineConfig = {
            "source": [1],
            "destination": "duckdb",
            "transformers": [],
            "incremental": None,
            "pipeline_name": None,
            "dataset_name": None,
            "options": {},
        }

        executor.execute(config)

        # Verify call order
        assert adapter.method_calls[0][0] == "create_pipeline"
        assert adapter.method_calls[1][0] == "apply_transformations"
        assert adapter.method_calls[2][0] == "run_pipeline"


class TestPipelineExecutorValidation:
    """Test configuration validation."""

    def test_execute_validates_config_is_dict(self) -> None:
        """execute() raises error if config is not a dict."""
        adapter = Mock()
        executor = PipelineExecutor(adapter)

        with pytest.raises(PipelineConfigurationError) as exc_info:
            executor.execute("not a dict")  # type: ignore

        assert "dictionary" in str(exc_info.value)

    def test_execute_validates_source_exists(self) -> None:
        """execute() raises error if source is missing."""
        adapter = Mock()
        executor = PipelineExecutor(adapter)

        config: PipelineConfig = {
            "source": None,  # Missing source
            "destination": "duckdb",
            "transformers": [],
            "incremental": None,
            "pipeline_name": None,
            "dataset_name": None,
            "options": {},
        }

        with pytest.raises(PipelineConfigurationError) as exc_info:
            executor.execute(config)

        assert "source" in str(exc_info.value)

    def test_execute_validates_destination_exists(self) -> None:
        """execute() raises error if destination is missing."""
        adapter = Mock()
        executor = PipelineExecutor(adapter)

        config: PipelineConfig = {
            "source": [1],
            "destination": None,  # Missing destination
            "transformers": [],
            "incremental": None,
            "pipeline_name": None,
            "dataset_name": None,
            "options": {},
        }

        with pytest.raises(PipelineConfigurationError) as exc_info:
            executor.execute(config)

        assert "destination" in str(exc_info.value)

    def test_execute_validates_transformers_key_exists(self) -> None:
        """execute() raises error if transformers key is missing."""
        adapter = Mock()
        executor = PipelineExecutor(adapter)

        config = {
            "source": [1],
            "destination": "duckdb",
            # Missing transformers key
        }

        with pytest.raises(PipelineConfigurationError) as exc_info:
            executor.execute(config)  # type: ignore

        assert "transformers" in str(exc_info.value)


class TestPipelineExecutorErrorHandling:
    """Test error handling during execution."""

    def test_execute_handles_adapter_create_error(self) -> None:
        """execute() wraps adapter creation errors."""
        adapter = Mock()
        adapter.create_pipeline.side_effect = RuntimeError("Adapter error")

        executor = PipelineExecutor(adapter)
        config: PipelineConfig = {
            "source": [1],
            "destination": "duckdb",
            "transformers": [],
            "incremental": None,
            "pipeline_name": None,
            "dataset_name": None,
            "options": {},
        }

        with pytest.raises(PipelineExecutionError) as exc_info:
            executor.execute(config)

        assert "execution failed" in str(exc_info.value)
        assert isinstance(exc_info.value.__cause__, RuntimeError)

    def test_execute_handles_adapter_run_error(self) -> None:
        """execute() wraps adapter run errors."""
        adapter = Mock()
        adapter.create_pipeline.return_value = "mock_pipeline"
        adapter.run_pipeline.side_effect = RuntimeError("Run error")
        adapter.apply_transformations.side_effect = _apply_transformers

        executor = PipelineExecutor(adapter)
        config: PipelineConfig = {
            "source": [1],
            "destination": "duckdb",
            "transformers": [],
            "incremental": None,
            "pipeline_name": None,
            "dataset_name": None,
            "options": {},
        }

        with pytest.raises(PipelineExecutionError) as exc_info:
            executor.execute(config)

        assert "execution failed" in str(exc_info.value)

    def test_execute_handles_transformer_error(self) -> None:
        """execute() wraps transformation errors."""
        adapter = Mock()
        adapter.create_pipeline.return_value = "mock_pipeline"
        adapter.apply_transformations.side_effect = PipelineExecutionError(
            "Transformation failed"
        )

        executor = PipelineExecutor(adapter)

        config: PipelineConfig = {
            "source": [1],
            "destination": "duckdb",
            "transformers": [lambda data: data],
            "incremental": None,
            "pipeline_name": None,
            "dataset_name": None,
            "options": {},
        }

        with pytest.raises(PipelineExecutionError) as exc_info:
            executor.execute(config)

        assert "Transformation failed" in str(exc_info.value)

    def test_execute_preserves_pipeline_execution_errors(self) -> None:
        """execute() re-raises PipelineExecutionError without wrapping."""
        adapter = Mock()
        original_error = PipelineExecutionError("Original error")
        adapter.create_pipeline.side_effect = original_error

        executor = PipelineExecutor(adapter)
        config: PipelineConfig = {
            "source": [1],
            "destination": "duckdb",
            "transformers": [],
            "incremental": None,
            "pipeline_name": None,
            "dataset_name": None,
            "options": {},
        }

        with pytest.raises(PipelineExecutionError) as exc_info:
            executor.execute(config)

        # Should be the same error, not wrapped
        assert exc_info.value is original_error


class TestPipelineExecutorIntegration:
    """Test integration scenarios."""

    def test_execute_with_full_pipeline_config(self) -> None:
        """execute() handles complete pipeline configuration."""
        adapter = Mock()
        adapter.create_pipeline.return_value = "mock_pipeline"
        adapter.run_pipeline.return_value = {"status": "success", "rows": 100}
        adapter.apply_transformations.side_effect = _apply_transformers

        executor = PipelineExecutor(adapter)

        def uppercase_names(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
            return [{**item, "name": item["name"].upper()} for item in data]

        config: PipelineConfig = {
            "source": [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}],
            "destination": "postgres",
            "transformers": [uppercase_names],
            "incremental": {"cursor_field": "updated_at"},
            "pipeline_name": "test_pipeline",
            "dataset_name": "test_dataset",
            "options": {"dev_mode": True},
        }

        result = executor.execute(config)

        assert result == {"status": "success", "rows": 100}

        # Verify transformed data was passed to adapter
        call_args = adapter.run_pipeline.call_args
        transformed_source = call_args[0][1]
        assert transformed_source[0]["name"] == "ALICE"
        assert transformed_source[1]["name"] == "BOB"

    def test_execute_multiple_times_with_same_executor(self) -> None:
        """Executor can be reused for multiple executions."""
        adapter = Mock()
        adapter.create_pipeline.return_value = "mock_pipeline"
        adapter.run_pipeline.return_value = "result"
        adapter.apply_transformations.side_effect = _apply_transformers

        executor = PipelineExecutor(adapter)

        config1: PipelineConfig = {
            "source": [1],
            "destination": "duckdb",
            "transformers": [],
            "incremental": None,
            "pipeline_name": None,
            "dataset_name": None,
            "options": {},
        }

        config2: PipelineConfig = {
            "source": [2],
            "destination": "postgres",
            "transformers": [],
            "incremental": None,
            "pipeline_name": None,
            "dataset_name": None,
            "options": {},
        }

        result1 = executor.execute(config1)
        result2 = executor.execute(config2)

        assert result1 == "result"
        assert result2 == "result"
        assert adapter.create_pipeline.call_count == 2


class TestPipelineExecutorRepr:
    """Test string representation."""

    def test_repr_shows_adapter_type(self) -> None:
        """repr() shows adapter type."""
        adapter = Mock()
        executor = PipelineExecutor(adapter)

        repr_str = repr(executor)

        assert "PipelineExecutor" in repr_str
        assert "adapter" in repr_str
