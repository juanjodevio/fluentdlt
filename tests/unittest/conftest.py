"""Pytest configuration and shared fixtures for fldt tests."""

import pytest


@pytest.fixture
def mock_sqlalchemy_engine():
    """Mock SQLAlchemy engine for testing."""
    from unittest.mock import MagicMock

    engine = MagicMock()
    engine.url = "sqlite:///:memory:"
    engine.dialect.name = "sqlite"
    return engine


@pytest.fixture
def sample_data():
    """Sample data for testing pipelines."""
    return [
        {"id": 1, "name": "Alice", "updated_at": "2024-01-01"},
        {"id": 2, "name": "Bob", "updated_at": "2024-01-02"},
        {"id": 3, "name": "Charlie", "updated_at": "2024-01-03"},
    ]


@pytest.fixture
def sample_transformer():
    """Sample transformer function for testing."""

    def uppercase_name(data):
        """Transform names to uppercase."""
        for item in data:
            if "name" in item:
                item["name"] = item["name"].upper()
        return data

    return uppercase_name

