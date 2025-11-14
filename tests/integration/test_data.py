"""Test data constants and validation helpers for integration tests.

This module provides deterministic test data and helper functions for
validating integration test results.
"""

from datetime import datetime

from pytest import approx


class TestData:
    """Container for test data expectations.

    This class defines the expected test data that's seeded via Alembic
    migrations. Use these constants in tests to validate loaded data.
    """

    # Users table data
    USERS = [
        {
            "id": 1,
            "name": "Alice",
            "email": "alice@example.com",
            "age": 28,
            "is_active": True,
        },
        {
            "id": 2,
            "name": "Bob",
            "email": "bob@example.com",
            "age": 35,
            "is_active": True,
        },
        {
            "id": 3,
            "name": "Charlie",
            "email": "charlie@example.com",
            "age": 42,
            "is_active": False,
        },
        {
            "id": 4,
            "name": "Diana",
            "email": "diana@example.com",
            "age": 31,
            "is_active": True,
        },
        {
            "id": 5,
            "name": "Eve",
            "email": "eve@example.com",
            "age": 29,
            "is_active": True,
        },
    ]

    # Events table data
    EVENTS = [
        {"id": 1, "user_id": 1, "event_type": "login"},
        {"id": 2, "user_id": 1, "event_type": "purchase"},
        {"id": 3, "user_id": 2, "event_type": "login"},
        {"id": 4, "user_id": 3, "event_type": "login"},
        {"id": 5, "user_id": 1, "event_type": "logout"},
        {"id": 6, "user_id": 4, "event_type": "login"},
        {"id": 7, "user_id": 2, "event_type": "purchase"},
        {"id": 8, "user_id": 5, "event_type": "login"},
        {"id": 9, "user_id": 4, "event_type": "purchase"},
        {"id": 10, "user_id": 5, "event_type": "logout"},
    ]

    # Products table data
    PRODUCTS = [
        {
            "id": 1,
            "name": "Widget",
            "category": "Electronics",
            "price": 25.00,
            "stock": 100,
        },
        {
            "id": 2,
            "name": "Gadget",
            "category": "Electronics",
            "price": 50.00,
            "stock": 50,
        },
        {"id": 3, "name": "Tool", "category": "Hardware", "price": 15.00, "stock": 200},
    ]

    # Counts
    TOTAL_USERS = len(USERS)
    TOTAL_EVENTS = len(EVENTS)
    TOTAL_PRODUCTS = len(PRODUCTS)
    ACTIVE_USERS = sum(1 for u in USERS if u["is_active"])


def validate_user_data(loaded_data: list[dict]) -> bool:
    """Validate loaded user data matches expected schema and values.

    Args:
        loaded_data: List of user records loaded from database.

    Returns:
        True if data is valid, raises AssertionError otherwise.
    """
    assert (
        len(loaded_data) == TestData.TOTAL_USERS
    ), f"Expected {TestData.TOTAL_USERS} users, got {len(loaded_data)}"

    # Check first user
    alice = next(u for u in loaded_data if u.get("id") == 1)
    assert alice["name"] == "Alice"
    assert alice["email"] == "alice@example.com"
    assert alice["age"] == 28

    return True


def validate_event_data(loaded_data: list[dict]) -> bool:
    """Validate loaded event data matches expected schema and values.

    Args:
        loaded_data: List of event records loaded from database.

    Returns:
        True if data is valid, raises AssertionError otherwise.
    """
    assert (
        len(loaded_data) == TestData.TOTAL_EVENTS
    ), f"Expected {TestData.TOTAL_EVENTS} events, got {len(loaded_data)}"

    # Check event types are present
    event_types = {e.get("event_type") for e in loaded_data}
    assert "login" in event_types
    assert "logout" in event_types
    assert "purchase" in event_types

    return True


def validate_product_data(loaded_data: list[dict]) -> bool:
    """Validate loaded product data matches expected schema and values.

    Args:
        loaded_data: List of product records loaded from database.

    Returns:
        True if data is valid, raises AssertionError otherwise.
    """
    assert (
        len(loaded_data) == TestData.TOTAL_PRODUCTS
    ), f"Expected {TestData.TOTAL_PRODUCTS} products, got {len(loaded_data)}"

    # Check product prices
    widget = next(p for p in loaded_data if p.get("name") == "Widget")
    assert float(widget["price"]) == approx(25.0)

    return True


def get_incremental_cutoff_date() -> str:
    """Get cutoff date for incremental loading tests.

    Returns date that splits test data roughly in half for testing
    incremental loading behavior.

    Returns:
        ISO format date string.
    """
    return "2024-01-03 00:00:00"
