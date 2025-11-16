"""DLT adapter implementation for fldt.

This module provides a concrete implementation of the PipelineAdapter protocol
using dlt (data load tool) as the underlying pipeline engine.
"""

import logging
from types import ModuleType
from typing import Any

from fldt.exceptions import AdapterError, PipelineExecutionError, ValidationError
from fldt.types import IncrementalConfig, PipelineConfig

logger = logging.getLogger(__name__)


class DltAdapter:
    """Adapter for dlt (data load tool) pipeline backend.

    This adapter translates fldt's pipeline configuration into dlt-specific
    pipeline construction and execution. It handles lazy loading of dlt to
    avoid import-time dependencies.

    The adapter manages:
    - Pipeline creation with proper configuration
    - Source preparation with incremental loading
    - Transformation application
    - Pipeline execution and result handling
    """

    def __init__(self) -> None:
        """Initialize the DLT adapter.

        Note: dlt is lazily imported on first use to avoid import-time
        errors if dlt is not installed or configured.
        """
        self._dlt: ModuleType | None = None
        self._dlt_sources: ModuleType | None = None

    def _ensure_dlt_loaded(self) -> None:
        """Lazy-load dlt modules.

        This method imports dlt on first use, providing better error
        messages if dlt is not available.

        Raises:
            AdapterError: If dlt cannot be imported.
        """
        if self._dlt is not None:
            return

        try:
            import dlt
            import dlt.sources

            self._dlt = dlt
            self._dlt_sources = dlt.sources
            logger.debug("DLT modules loaded successfully")
        except ImportError as e:
            raise AdapterError(
                "dlt is not installed. Install it with: pip install 'dlt[sql_database]'"
            ) from e
        except Exception as e:
            raise AdapterError(f"Failed to load dlt modules: {e}") from e

    def create_pipeline(self, config: PipelineConfig) -> Any:
        """Create a dlt pipeline from configuration.

        Args:
            config: Pipeline configuration with destination and options.

        Returns:
            dlt.Pipeline object ready for execution.

        Raises:
            AdapterError: If pipeline creation fails.
            ValidationError: If destination is not set.
        """
        self._ensure_dlt_loaded()
        assert self._dlt is not None

        if not config.get("destination"):
            raise ValidationError("Destination must be set before creating pipeline")

        try:
            # Extract pipeline options
            pipeline_name = config.get("pipeline_name") or "fldt_pipeline"
            destination = config["destination"]
            dataset_name = config.get("dataset_name")

            # Build pipeline creation arguments
            pipeline_args: dict[str, Any] = {
                "pipeline_name": pipeline_name,
                "destination": destination,
            }

            if dataset_name:
                pipeline_args["dataset_name"] = dataset_name

            # Add any additional options
            options = config.get("options", {})
            pipeline_args.update(options)

            logger.info(
                "Creating dlt pipeline",
                extra={
                    "pipeline_name": pipeline_name,
                    "destination": destination,
                    "dataset_name": dataset_name,
                },
            )

            pipeline = self._dlt.pipeline(**pipeline_args)
            logger.debug("Pipeline created successfully")
            return pipeline

        except Exception as e:
            logger.error("Failed to create pipeline", extra={"error": str(e)})
            raise AdapterError(f"Failed to create dlt pipeline: {e}") from e

    def run_pipeline(self, pipeline: Any, source: Any) -> Any:
        """Execute a dlt pipeline with the given source.

        Args:
            pipeline: dlt.Pipeline object.
            source: Data source (possibly with transformations applied).

        Returns:
            dlt.LoadInfo with execution results.

        Raises:
            PipelineExecutionError: If pipeline execution fails.
        """
        self._ensure_dlt_loaded()
        assert self._dlt is not None
        prepared_source = self._prepare_source_for_pipeline(source)

        try:
            logger.info("Starting pipeline execution")
            result = pipeline.run(prepared_source)
            logger.info(
                "Pipeline execution completed",
                extra={
                    "loads": (
                        len(result.loads_ids) if hasattr(result, "loads_ids") else 0
                    ),
                },
            )
            return result

        except Exception as e:
            logger.error("Pipeline execution failed", extra={"error": str(e)})
            raise PipelineExecutionError(f"Pipeline execution failed: {e}") from e

    def apply_transformations(
        self,
        source: Any,
        transformers: list[Any],
    ) -> Any:
        """Apply transformations to the source data.

        Transformations are applied sequentially. For dlt resources/sources,
        transformers are applied at the record level using dlt's transformer
        pattern. For raw data (lists, dicts, tuples), transformers are applied
        directly to the data.

        Args:
            source: Original data source (dlt resource/source or raw data).
            transformers: List of transformation functions.

        Returns:
            Transformed source (or original if no transformers).

        Raises:
            PipelineExecutionError: If any transformation fails.
        """
        if not transformers:
            logger.debug("No transformers to apply")
            return source

        logger.info("Applying transformations", extra={"count": len(transformers)})

        # Validate all transformers are callable
        for idx, transformer in enumerate(transformers):
            if not callable(transformer):
                raise PipelineExecutionError(
                    f"Transformer at index {idx} is not callable: {type(transformer)}"
                )

        # Check if source is a dlt resource/source (not raw Python data)
        is_dlt_resource = self._is_dlt_resource(source)

        if is_dlt_resource:
            return self._apply_transformations_to_dlt_resource(source, transformers)
        else:
            return self._apply_transformations_to_raw_data(source, transformers)

    def _is_dlt_resource(self, source: Any) -> bool:
        """Check if source is a dlt resource or source object.

        Args:
            source: Source to check.

        Returns:
            True if source appears to be a dlt resource/source, False otherwise.
        """
        # Raw Python data types are not dlt resources
        if isinstance(source, (list, tuple, dict)):
            # Check if dict is a simple dict (not a dlt source wrapper)
            if isinstance(source, dict):
                # Simple heuristic: if it's a dict with only string keys
                # and no dlt-specific attributes, treat as raw data
                if not hasattr(source, "__dlt__") and not hasattr(source, "resources"):
                    return False
            else:
                return False

        # Check for pandas DataFrames and Series (treat as raw data)
        try:
            import pandas as pd  # type: ignore[import-untyped]

            if isinstance(source, (pd.DataFrame, pd.Series)):
                return False
        except ImportError:
            # pandas not available, skip check
            pass

        # Check for dlt-specific attributes/methods
        # dlt resources typically have these characteristics:
        # - Have a `__name__` attribute
        # - Are callable or have `resources` attribute
        # - Have dlt-specific metadata
        if hasattr(source, "resources") or hasattr(source, "__dlt__"):
            return True

        # If it's callable and not a simple function (likely a dlt resource)
        # but exclude simple types
        if callable(source) and not isinstance(source, (type, type(lambda: None))):
            # Additional check: dlt resources often have specific attributes
            if hasattr(source, "__name__") or hasattr(source, "name"):
                return True

        # Default: assume it's a dlt resource if it's not raw Python data
        # This is safer for SQL/dlt-backed sources
        return not isinstance(source, (list, tuple, dict, str, int, float, bool))

    def _apply_transformations_to_dlt_resource(
        self, source: Any, transformers: list[Any]
    ) -> Any:
        """Apply transformations to a dlt resource at record level.

        Wraps the source with dlt transformers that process individual records.

        Args:
            source: dlt resource or source object.
            transformers: List of transformation functions that accept records.

        Returns:
            Transformed dlt resource.

        Raises:
            PipelineExecutionError: If transformation fails.
        """
        self._ensure_dlt_loaded()
        assert self._dlt is not None
        dlt_module = self._dlt  # Capture for type checker

        try:
            # Start with the source
            current_source = source

            # Apply each transformer sequentially using dlt's transformer pattern
            for idx, transformer_func in enumerate(transformers):
                logger.debug(f"Applying dlt transformer {idx + 1}/{len(transformers)}")

                # Create a dlt transformer that wraps the user's transformer function
                # The transformer receives items (records) and applies the user's function
                # Use default parameter to capture transformer_func in closure properly
                def make_transformer(transformer: Any) -> Any:
                    """Factory to create transformer with proper closure."""

                    @dlt_module.transformer(  # type: ignore[misc]
                        data_from=current_source,
                        name=f"transformer_{idx + 1}",
                    )
                    def transformed_resource(items: Any) -> Any:
                        """Wrapper that applies user transformer to each record."""
                        for item in items:
                            # User transformer expects a list of records or a single record
                            # Try both patterns for flexibility
                            try:
                                # Pattern 1: Transformer expects a list
                                if isinstance(item, dict):
                                    # Single record - try calling with list first
                                    try:
                                        transformed = transformer([item])
                                        if (
                                            isinstance(transformed, list)
                                            and len(transformed) > 0
                                        ):
                                            yield transformed[0]
                                        else:
                                            yield item
                                    except (TypeError, AttributeError):
                                        # Pattern 2: Transformer expects single record
                                        transformed = transformer(item)
                                        if isinstance(transformed, dict):
                                            yield transformed
                                        else:
                                            yield item
                                else:
                                    # Not a dict, pass through
                                    yield item
                            except Exception as e:
                                logger.error(
                                    f"Error transforming record: {e}",
                                    extra={"item": str(item)[:100]},
                                )
                                raise

                    return transformed_resource

                current_source = make_transformer(transformer_func)

            logger.debug("All dlt transformations applied successfully")
            return current_source

        except Exception as e:
            logger.error(
                "Failed to apply transformations to dlt resource",
                extra={"error": str(e)},
            )
            raise PipelineExecutionError(
                f"Failed to apply transformations to dlt resource: {e}"
            ) from e

    def _apply_transformations_to_raw_data(
        self, source: Any, transformers: list[Any]
    ) -> Any:
        """Apply transformations to raw Python data (lists, dicts, tuples, pandas DataFrames/Series).

        Args:
            source: Raw data (list, dict, tuple, pandas DataFrame/Series, etc.).
            transformers: List of transformation functions.

        Returns:
            Transformed data.

        Raises:
            PipelineExecutionError: If transformation fails.
        """
        result = source
        for idx, transformer in enumerate(transformers):
            try:
                logger.debug(
                    f"Applying raw data transformer {idx + 1}/{len(transformers)}"
                )
                result = transformer(result)

            except Exception as e:
                logger.error(
                    f"Transformation {idx + 1} failed",
                    extra={"transformer": str(transformer), "error": str(e)},
                )
                raise PipelineExecutionError(
                    f"Transformation {idx + 1} failed: {e}"
                ) from e

        logger.debug("All raw data transformations applied successfully")
        return result

    def _prepare_source_for_pipeline(self, source: Any) -> Any:
        """Ensure the source is acceptable for dlt pipeline execution."""
        assert self._dlt is not None
        if isinstance(source, (list, tuple, dict)):
            logger.debug("Wrapping raw iterable source into dlt.resource")
            return self._dlt.resource(source, name="transformed_data")
        return source

    def prepare_source_with_incremental(
        self,
        source: Any,
        incremental_config: IncrementalConfig | None,
    ) -> Any:
        """Prepare a source with incremental loading configuration.

        This method wraps the source with dlt's incremental loading
        functionality if incremental config is provided.

        Args:
            source: Original data source.
            incremental_config: Incremental loading configuration (cursor field, etc.).

        Returns:
            Source configured for incremental loading, or original source.

        Raises:
            AdapterError: If incremental configuration fails.
        """
        if not incremental_config:
            return source

        self._ensure_dlt_loaded()
        assert self._dlt is not None
        assert self._dlt_sources is not None

        try:
            cursor_field = incremental_config["cursor_field"]
            if isinstance(cursor_field, str):
                cursor_path: Any = (
                    cursor_field
                    if cursor_field.startswith("$")
                    else f"$.{cursor_field}"
                )
            else:
                cursor_path = cursor_field
            logger.info(
                "Configuring incremental loading",
                extra={"cursor_field": cursor_field},
            )

            incremental_args: dict[str, Any] = {"cursor_path": cursor_path}
            if "initial_value" in incremental_config:
                incremental_args["initial_value"] = incremental_config["initial_value"]
            if "primary_key" in incremental_config:
                incremental_args["primary_key"] = incremental_config["primary_key"]
            incremental = self._dlt_sources.incremental(**incremental_args)

            if hasattr(source, "with_incremental") and callable(
                getattr(source, "with_incremental")
            ):
                logger.debug("Applying incremental via source.with_incremental")
                return source.with_incremental(incremental)

            resource_name = getattr(source, "__name__", "incremental_source")
            logger.debug(
                "Wrapping source into dlt.resource with incremental",
                extra={"resource_name": resource_name},
            )
            wrapped_source = self._dlt.resource(
                source,
                name=resource_name,
                incremental=incremental,
            )
            return wrapped_source

        except Exception as e:
            logger.error(
                "Failed to configure incremental loading", extra={"error": str(e)}
            )
            raise AdapterError(f"Failed to configure incremental loading: {e}") from e
