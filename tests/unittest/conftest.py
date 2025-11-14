"""Pytest configuration and shared fixtures for fldt tests."""

from __future__ import annotations

from typing import Any, Callable
from unittest.mock import MagicMock

import pytest

DataRow = dict[str, Any]
Transformer = Callable[[list[DataRow]], list[DataRow]]


@pytest.fixture
def mock_sqlalchemy_engine() -> MagicMock:
    """Mock SQLAlchemy engine for testing."""

    engine = MagicMock()
    engine.url = "sqlite:///:memory:"
    engine.dialect.name = "sqlite"
    return engine


@pytest.fixture
def sample_data() -> list[DataRow]:
    """Sample data for testing pipelines."""
    return [
        {"id": 1, "name": "Alice", "updated_at": "2024-01-01"},
        {"id": 2, "name": "Bob", "updated_at": "2024-01-02"},
        {"id": 3, "name": "Charlie", "updated_at": "2024-01-03"},
    ]


@pytest.fixture
def sample_transformer() -> Transformer:
    """Sample transformer function for testing."""

    def uppercase_name(data: list[DataRow]) -> list[DataRow]:
        """Transform names to uppercase."""
        for item in data:
            if "name" in item:
                item["name"] = str(item["name"]).upper()
        return data

    return uppercase_name
