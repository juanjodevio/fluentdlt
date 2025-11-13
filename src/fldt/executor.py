"""Pipeline executor for fldt.

This module provides the PipelineExecutor class which handles the actual
execution of configured pipelines using the appropriate adapter.
"""

import logging
from typing import Any

from fldt.adapters.protocol import PipelineAdapter
from fldt.exceptions import PipelineConfigurationError, PipelineExecutionError
from fldt.transformers import TransformerChain
from fldt.types import PipelineConfig

logger = logging.getLogger(__name__)


class PipelineExecutor:
    """Executes pipeline configurations using an adapter.

    The PipelineExecutor takes a validated pipeline configuration and an
    adapter, then orchestrates the execution flow:
    1. Create pipeline from configuration
    2. Apply transformations to source
    3. Run pipeline with transformed source

    This class focuses solely on execution orchestration, delegating
    adapter-specific operations to the provided adapter.

    Example:
        ```python
        executor = PipelineExecutor(adapter)
        result = executor.execute(config)
        ```
    """

    def __init__(self, adapter: PipelineAdapter) -> None:
        """Initialize the executor with an adapter.

        Args:
            adapter: Pipeline adapter to use for execution.

        Raises:
            PipelineConfigurationError: If adapter is None.
        """
        if adapter is None:
            raise PipelineConfigurationError("Adapter cannot be None")

        self._adapter = adapter
        logger.debug(
            "Initialized PipelineExecutor",
            extra={"adapter_type": type(adapter).__name__},
        )

    def execute(self, config: PipelineConfig) -> Any:
        """Execute a pipeline configuration.

        This method orchestrates the complete pipeline execution:
        1. Validates the configuration
        2. Creates the pipeline via adapter
        3. Applies transformations to the source
        4. Runs the pipeline

        Args:
            config: Validated pipeline configuration from PipelineBuilder.

        Returns:
            Execution result from the adapter (adapter-specific format).

        Raises:
            PipelineConfigurationError: If configuration is invalid.
            PipelineExecutionError: If execution fails.
        """
        logger.info("Starting pipeline execution")

        # Validate configuration
        self._validate_config(config)

        try:
            # Step 1: Create pipeline
            logger.debug("Creating pipeline")
            pipeline = self._adapter.create_pipeline(config)

            # Step 2: Apply transformations
            logger.debug("Preparing source with transformations")
            transformed_source = self._apply_transformations(
                config["source"],
                config["transformers"],
            )

            # Step 3: Run pipeline
            logger.debug("Running pipeline")
            result = self._adapter.run_pipeline(pipeline, transformed_source)

            logger.info("Pipeline execution completed successfully")
            return result

        except PipelineExecutionError:
            # Re-raise execution errors as-is
            raise
        except Exception as e:
            logger.error(
                "Pipeline execution failed",
                extra={"error": str(e), "error_type": type(e).__name__},
            )
            raise PipelineExecutionError(f"Pipeline execution failed: {e}") from e

    def _validate_config(self, config: PipelineConfig) -> None:
        """Validate pipeline configuration before execution.

        Args:
            config: Pipeline configuration to validate.

        Raises:
            PipelineConfigurationError: If configuration is invalid.
        """
        if not isinstance(config, dict):
            raise PipelineConfigurationError("Config must be a dictionary")

        if "source" not in config or config["source"] is None:
            raise PipelineConfigurationError("Config must have a source")

        if "destination" not in config or not config["destination"]:
            raise PipelineConfigurationError("Config must have a destination")

        if "transformers" not in config:
            raise PipelineConfigurationError("Config must have transformers list")

        logger.debug("Configuration validated successfully")

    def _apply_transformations(
        self,
        source: Any,
        transformers: list[Any],
    ) -> Any:
        """Apply transformations to the source data.

        Uses TransformerChain for transformation application to ensure
        consistent error handling and logging.

        Args:
            source: Original data source.
            transformers: List of transformation functions.

        Returns:
            Transformed source data.

        Raises:
            PipelineExecutionError: If transformation fails.
        """
        if not transformers:
            logger.debug("No transformations to apply")
            return source

        try:
            chain = TransformerChain(transformers)
            return chain.apply(source)
        except Exception as e:
            logger.error(
                "Transformation failed during execution",
                extra={"error": str(e)},
            )
            raise PipelineExecutionError(f"Transformation failed: {e}") from e

    def __repr__(self) -> str:
        """Return string representation of the executor."""
        return f"PipelineExecutor(adapter={type(self._adapter).__name__})"

