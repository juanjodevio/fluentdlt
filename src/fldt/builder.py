"""Pipeline configuration builder for fldt.

This module provides the PipelineBuilder class which constructs validated
pipeline configurations. The builder follows the Builder pattern and ensures
all configurations are validated before being frozen.
"""

import logging
from typing import Any

from fldt.exceptions import PipelineConfigurationError, ValidationError
from fldt.types import (
    DestinationType,
    IncrementalConfig,
    PipelineConfig,
    SourceType,
    TransformerFunc,
)

logger = logging.getLogger(__name__)


class PipelineBuilder:
    """Builder for constructing validated pipeline configurations.

    The PipelineBuilder follows the Builder pattern to construct pipeline
    configurations step by step. It validates inputs as they're added and
    produces an immutable (frozen) configuration when built.

    This class focuses on configuration construction and validation,
    delegating execution to the PipelineExecutor.

    Example:
        ```python
        builder = PipelineBuilder()
        config = (builder
            .set_source(my_source)
            .set_destination("duckdb")
            .add_transformer(lambda x: x * 2)
            .build())
        ```
    """

    def __init__(self) -> None:
        """Initialize an empty pipeline builder."""
        self._source: SourceType | None = None
        self._destination: DestinationType | None = None
        self._transformers: list[TransformerFunc] = []
        self._incremental: IncrementalConfig | None = None
        self._pipeline_name: str | None = None
        self._dataset_name: str | None = None
        self._options: dict[str, Any] = {}
        logger.debug("Initialized new PipelineBuilder")

    def set_source(self, source: SourceType) -> "PipelineBuilder":
        """Set the data source for the pipeline.

        Args:
            source: Data source (dlt source, callable, iterable, or raw data).

        Returns:
            Self for method chaining.

        Raises:
            ValidationError: If source is None.
        """
        if source is None:
            raise ValidationError("Source cannot be None")

        self._source = source
        logger.debug("Set pipeline source", extra={"source_type": type(source).__name__})
        return self

    def set_destination(self, destination: DestinationType) -> "PipelineBuilder":
        """Set the destination for the pipeline.

        Args:
            destination: Target destination (string name or dlt destination object).

        Returns:
            Self for method chaining.

        Raises:
            ValidationError: If destination is invalid.
        """
        if not destination:
            raise ValidationError("Destination cannot be empty")

        if isinstance(destination, str) and not destination.strip():
            raise ValidationError("Destination name cannot be blank")

        self._destination = destination
        logger.debug("Set pipeline destination", extra={"destination": str(destination)})
        return self

    def add_transformer(self, transformer: TransformerFunc) -> "PipelineBuilder":
        """Add a transformation function to the pipeline.

        Transformations are applied in the order they are added.

        Args:
            transformer: A callable that transforms data.

        Returns:
            Self for method chaining.

        Raises:
            ValidationError: If transformer is not callable.
        """
        if not callable(transformer):
            raise ValidationError(
                f"Transformer must be callable, got {type(transformer).__name__}"
            )

        self._transformers.append(transformer)
        logger.debug(
            "Added transformer to pipeline",
            extra={"transformer_count": len(self._transformers)},
        )
        return self

    def set_incremental(
        self,
        cursor_field: str,
        initial_value: Any = None,
        primary_key: str | list[str] | None = None,
        row_order: str = "asc",
        **kwargs: Any,
    ) -> "PipelineBuilder":
        """Configure incremental loading for the pipeline.

        Args:
            cursor_field: Field name to use as cursor (e.g., 'updated_at').
            initial_value: Starting value for the cursor (optional).
            primary_key: Primary key field(s) for deduplication (optional).
            row_order: Row ordering - 'asc' or 'desc'. Defaults to 'asc'.
            **kwargs: Additional incremental loading options.

        Returns:
            Self for method chaining.

        Raises:
            ValidationError: If cursor_field is invalid.
        """
        if not cursor_field or not isinstance(cursor_field, str):
            raise ValidationError("cursor_field must be a non-empty string")

        if row_order not in ("asc", "desc"):
            raise ValidationError("row_order must be 'asc' or 'desc'")

        self._incremental = {
            "cursor_field": cursor_field,
            "row_order": row_order,
        }

        if initial_value is not None:
            self._incremental["initial_value"] = initial_value

        if primary_key is not None:
            self._incremental["primary_key"] = primary_key

        # Add any additional kwargs
        for key, value in kwargs.items():
            self._incremental[key] = value  # type: ignore

        logger.debug(
            "Configured incremental loading",
            extra={"cursor_field": cursor_field, "row_order": row_order},
        )
        return self

    def set_pipeline_name(self, name: str) -> "PipelineBuilder":
        """Set the pipeline name.

        Args:
            name: Name for the pipeline.

        Returns:
            Self for method chaining.

        Raises:
            ValidationError: If name is invalid.
        """
        if not name or not isinstance(name, str) or not name.strip():
            raise ValidationError("Pipeline name must be a non-empty string")

        self._pipeline_name = name
        logger.debug("Set pipeline name", extra={"name": name})
        return self

    def set_dataset_name(self, name: str) -> "PipelineBuilder":
        """Set the dataset name.

        Args:
            name: Name for the destination dataset.

        Returns:
            Self for method chaining.

        Raises:
            ValidationError: If name is invalid.
        """
        if not name or not isinstance(name, str) or not name.strip():
            raise ValidationError("Dataset name must be a non-empty string")

        self._dataset_name = name
        logger.debug("Set dataset name", extra={"name": name})
        return self

    def set_option(self, key: str, value: Any) -> "PipelineBuilder":
        """Set a pipeline option.

        Args:
            key: Option key.
            value: Option value.

        Returns:
            Self for method chaining.

        Raises:
            ValidationError: If key is invalid.
        """
        if not key or not isinstance(key, str):
            raise ValidationError("Option key must be a non-empty string")

        self._options[key] = value
        logger.debug("Set pipeline option", extra={"key": key, "value": str(value)})
        return self

    def set_options(self, options: dict[str, Any]) -> "PipelineBuilder":
        """Set multiple pipeline options at once.

        Args:
            options: Dictionary of options to set.

        Returns:
            Self for method chaining.

        Raises:
            ValidationError: If options is not a dict.
        """
        if not isinstance(options, dict):
            raise ValidationError("Options must be a dictionary")

        for key, value in options.items():
            self.set_option(key, value)

        return self

    def build(self) -> PipelineConfig:
        """Build and validate the pipeline configuration.

        Creates a frozen (immutable) configuration dictionary that can be
        passed to the PipelineExecutor.

        Returns:
            Validated and frozen pipeline configuration.

        Raises:
            PipelineConfigurationError: If required configuration is missing.
        """
        # Validate required fields
        if self._source is None:
            raise PipelineConfigurationError("Source must be set before building")

        if self._destination is None:
            raise PipelineConfigurationError("Destination must be set before building")

        # Build configuration
        config: PipelineConfig = {
            "source": self._source,
            "destination": self._destination,
            "transformers": self._transformers.copy(),
            "incremental": self._incremental.copy() if self._incremental else None,
            "pipeline_name": self._pipeline_name,
            "dataset_name": self._dataset_name,
            "options": self._options.copy(),
        }

        logger.info(
            "Built pipeline configuration",
            extra={
                "has_source": config["source"] is not None,
                "destination": str(config["destination"]),
                "transformer_count": len(config["transformers"]),
                "has_incremental": config["incremental"] is not None,
            },
        )

        return config

    def __repr__(self) -> str:
        """Return string representation of the builder state."""
        return (
            f"PipelineBuilder("
            f"source={'set' if self._source else 'unset'}, "
            f"destination={'set' if self._destination else 'unset'}, "
            f"transformers={len(self._transformers)}, "
            f"incremental={'set' if self._incremental else 'unset'})"
        )

