"""Type definitions and protocols for fldt.

This module defines all type aliases, protocols, and configuration structures
used throughout the fldt package. These types ensure type safety and provide
clear contracts for all public APIs.
"""

from collections.abc import Callable
from typing import Any, Protocol, TypedDict

try:
    from sqlalchemy.engine import Engine
except ImportError:
    Engine = Any  # type: ignore


class DltSourceProtocol(Protocol):
    """Protocol for dlt source objects."""

    def __call__(self) -> Any:
        """Execute the source to yield data."""
        ...


# Source can be a dlt source, callable, iterable, or raw data
SourceType = DltSourceProtocol | Callable[..., Any] | Any

# Destination can be a string name or dlt destination object
DestinationType = str | Any

# Connection is SQLAlchemy Engine or connection string
ConnectionType = Engine | str

# Transformer is a callable that takes data and returns transformed data
TransformerFunc = Callable[[Any], Any]


class IncrementalConfig(TypedDict, total=False):
    """Configuration for incremental loading.

    Attributes:
        cursor_field: Field name to use for incremental cursor (e.g., 'updated_at').
        initial_value: Starting value for the cursor (optional).
        end_value: Ending value for bounded backfills (optional).
        primary_key: Primary key field(s) for deduplication (optional).
        allow_external_schedulers: Allow external schedulers to manage state.
    """

    cursor_field: str
    initial_value: Any
    primary_key: str | list[str] | None
    end_value: Any
    allow_external_schedulers: bool


class PipelineConfig(TypedDict, total=False):
    """Configuration for pipeline execution.

    Attributes:
        source: Data source to extract from.
        destination: Target destination to load into.
        transformers: List of transformation functions.
        incremental: Incremental loading configuration.
        pipeline_name: Name for the dlt pipeline.
        dataset_name: Name for the destination dataset.
        options: Additional pipeline options.
    """

    source: SourceType
    destination: DestinationType
    transformers: list[TransformerFunc]
    incremental: IncrementalConfig | None
    pipeline_name: str | None
    dataset_name: str | None
    options: dict[str, Any]
