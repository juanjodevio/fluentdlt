"""Fluent Data Loading Toolkit — Write ETL pipelines that read like English."""

# Public API exports
from fldt.adapters import DltAdapter, PipelineAdapter
from fldt.builder import PipelineBuilder
from fldt.exceptions import (
    AdapterError,
    FluentDLTError,
    PipelineConfigurationError,
    PipelineExecutionError,
    ValidationError,
)
from fldt.executor import PipelineExecutor
from fldt.fluent import FluentPipeline
from fldt.transformers import TransformerChain
from fldt.types import (
    ConnectionType,
    DestinationType,
    IncrementalConfig,
    PipelineConfig,
    SourceType,
    TransformerFunc,
)

__version__ = "0.1.0"

__all__ = [
    # Main API
    "FluentPipeline",
    # Exceptions
    "FluentDLTError",
    "ValidationError",
    "PipelineConfigurationError",
    "PipelineExecutionError",
    "AdapterError",
    # Types
    "SourceType",
    "DestinationType",
    "ConnectionType",
    "TransformerFunc",
    "IncrementalConfig",
    "PipelineConfig",
    # Adapters
    "PipelineAdapter",
    "DltAdapter",
    # Transformers
    "TransformerChain",
    # Builder and Executor
    "PipelineBuilder",
    "PipelineExecutor",
]
