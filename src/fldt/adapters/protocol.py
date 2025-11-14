"""Protocol definition for pipeline adapters.

This module defines the abstract interface that all pipeline adapters
must implement. Using Protocol (PEP 544) allows for structural subtyping
without explicit inheritance.
"""

from typing import Any, Protocol

from fldt.types import PipelineConfig


class PipelineAdapter(Protocol):
    """Protocol for pipeline adapter implementations.

    Adapters are responsible for translating fldt's pipeline configuration
    into concrete pipeline execution using a specific backend (e.g., dlt).

    All adapter implementations must provide these methods to be compatible
    with fldt's execution engine.
    """

    def create_pipeline(
        self,
        config: PipelineConfig,
    ) -> Any:
        """Create a pipeline instance from configuration.

        This method should construct a pipeline object from the provided
        configuration but should not execute it. The pipeline object's
        structure is adapter-specific.

        Args:
            config: Complete pipeline configuration including source,
                destination, and options.

        Returns:
            Adapter-specific pipeline object ready for execution.

        Raises:
            AdapterError: If pipeline creation fails.
            ValidationError: If configuration is invalid.
        """
        ...

    def run_pipeline(
        self,
        pipeline: Any,
        source: Any,
    ) -> Any:
        """Execute the pipeline with the given source.

        This method should run the pipeline and return execution results.
        The exact structure of the results is adapter-specific but should
        contain at least success/failure information.

        Args:
            pipeline: Pipeline object created by create_pipeline().
            source: Data source to process (may have transformations applied).

        Returns:
            Adapter-specific execution results/metadata.

        Raises:
            PipelineExecutionError: If pipeline execution fails.
        """
        ...

    def apply_transformations(
        self,
        source: Any,
        transformers: list[Any],
    ) -> Any:
        """Apply transformation functions to the source data.

        This method should apply each transformer in sequence to the source
        data, with each transformer receiving the output of the previous one.

        Args:
            source: Original data source.
            transformers: List of transformation functions to apply.

        Returns:
            Transformed data source ready for pipeline execution.

        Raises:
            PipelineExecutionError: If any transformation fails.
        """
        ...

