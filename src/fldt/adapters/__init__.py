"""Adapter implementations for fldt.

This package contains the adapter protocol and concrete implementations
for different pipeline backends (e.g., dlt).
"""

from fldt.adapters.dlt_adapter import DltAdapter
from fldt.adapters.protocol import PipelineAdapter

__all__ = ["PipelineAdapter", "DltAdapter"]

