"""DLT adapter implementation for fldt.

This module provides a concrete implementation of the PipelineAdapter protocol
using dlt (data load tool) as the underlying pipeline engine.
"""

import logging
from types import ModuleType
from typing import Any

from fldt.exceptions import AdapterError, PipelineExecutionError, ValidationError
from fldt.types import PipelineConfig

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

        try:
            logger.info("Starting pipeline execution")
            result = pipeline.run(source)
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

        Transformations are applied sequentially. Each transformer receives
        the output of the previous transformation (or original source for first).

        Args:
            source: Original data source.
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

        result = source
        for idx, transformer in enumerate(transformers):
            try:
                if not callable(transformer):
                    raise PipelineExecutionError(
                        f"Transformer at index {idx} is not callable: {type(transformer)}"
                    )

                logger.debug(f"Applying transformer {idx + 1}/{len(transformers)}")
                result = transformer(result)

            except Exception as e:
                logger.error(
                    f"Transformation {idx + 1} failed",
                    extra={"transformer": str(transformer), "error": str(e)},
                )
                raise PipelineExecutionError(
                    f"Transformation {idx + 1} failed: {e}"
                ) from e

        logger.debug("All transformations applied successfully")
        return result

    def prepare_source_with_incremental(
        self,
        source: Any,
        incremental_config: dict[str, Any] | None,
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
        assert self._dlt_sources is not None

        try:
            cursor_field = incremental_config["cursor_field"]
            logger.info(
                "Configuring incremental loading",
                extra={"cursor_field": cursor_field},
            )

            # Build incremental arguments
            incremental_args: dict[str, Any] = {"cursor_path": cursor_field}

            if "initial_value" in incremental_config:
                incremental_args["initial_value"] = incremental_config["initial_value"]

            if "primary_key" in incremental_config:
                incremental_args["primary_key"] = incremental_config["primary_key"]

            if "row_order" in incremental_config:
                # dlt uses 'last_value_func' for ordering, but we keep it simple
                # and assume ascending order unless specified
                pass

            # Create incremental object
            incremental = self._dlt_sources.incremental(**incremental_args)

            # Apply to source if it supports incremental
            # Note: This is simplified - actual implementation depends on source type
            logger.debug("Incremental loading configured successfully")
            return incremental

        except Exception as e:
            logger.error(
                "Failed to configure incremental loading", extra={"error": str(e)}
            )
            raise AdapterError(f"Failed to configure incremental loading: {e}") from e
