"""Unit tests for integration test conftest helpers.

Tests helper functions from tests/integration/conftest.py that can be
unit tested without requiring actual database connections.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from tests.integration import conftest


class TestBuildAlembicConfig:
    """Test _build_alembic_config helper."""

    def test_build_alembic_config_returns_config(self) -> None:
        """_build_alembic_config returns a valid Alembic Config object."""
        config = conftest._build_alembic_config()

        assert config is not None
        assert hasattr(config, "set_main_option")
        # Verify script_location is set to alembic directory
        script_location = config.get_main_option("script_location")
        assert script_location is not None
        assert "alembic" in script_location


class TestRequirePostgresOrSkip:
    """Test _require_postgres_or_skip helper."""

    @patch("tests.integration.conftest._wait_for_postgres")
    def test_require_postgres_or_skip_when_available(
        self, mock_wait: MagicMock
    ) -> None:
        """_require_postgres_or_skip does not skip when Postgres is available."""
        mock_wait.return_value = None

        # Should not raise or skip
        conftest._require_postgres_or_skip("postgresql://localhost/test", "test_label")

        mock_wait.assert_called_once_with("postgresql://localhost/test")

    @patch("tests.integration.conftest._wait_for_postgres")
    def test_require_postgres_or_skip_when_unavailable(
        self, mock_wait: MagicMock
    ) -> None:
        """_require_postgres_or_skip skips when Postgres is unavailable."""
        mock_wait.side_effect = Exception("Connection refused")

        with pytest.raises(pytest.skip.Exception):  # type: ignore
            conftest._require_postgres_or_skip(
                "postgresql://localhost/test", "test_label"
            )


class TestWaitForPostgres:
    """Test _wait_for_postgres helper."""

    @patch("tests.integration.conftest.create_engine")
    @patch("tests.integration.conftest.time.sleep")
    def test_wait_for_postgres_succeeds_on_first_try(
        self, mock_sleep: MagicMock, mock_create_engine: MagicMock
    ) -> None:
        """_wait_for_postgres returns immediately if connection succeeds."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_engine.connect.return_value.__enter__.return_value = mock_conn

        conftest._wait_for_postgres("postgresql://localhost/test", timeout=5.0)

        mock_create_engine.assert_called_once_with("postgresql://localhost/test")
        mock_sleep.assert_not_called()
        mock_engine.dispose.assert_called_once()

    @patch("tests.integration.conftest.create_engine")
    @patch("tests.integration.conftest.time.sleep")
    @patch("tests.integration.conftest.time.time")
    def test_wait_for_postgres_retries_on_failure(
        self, mock_time: MagicMock, mock_sleep: MagicMock, mock_create_engine: MagicMock
    ) -> None:
        """_wait_for_postgres retries until connection succeeds."""
        # Simulate time progression - provide enough values for all iterations
        call_count = [0]

        def time_side_effect() -> float:
            count = call_count[0]
            call_count[0] += 1
            return float(count)

        mock_time.side_effect = time_side_effect

        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_create_engine.return_value = mock_engine

        # Create context manager for successful connection
        successful_ctx = MagicMock()
        successful_ctx.__enter__ = MagicMock(return_value=mock_conn)
        successful_ctx.__exit__ = MagicMock(return_value=None)

        # First two calls fail, third succeeds
        connect_call_count = [0]

        def connect_side_effect(*args: object, **kwargs: object) -> MagicMock:
            count = connect_call_count[0]
            connect_call_count[0] += 1
            if count < 2:
                raise Exception("Connection refused")
            return successful_ctx

        mock_engine.connect.side_effect = connect_side_effect

        conftest._wait_for_postgres("postgresql://localhost/test", timeout=5.0)

        assert mock_engine.connect.call_count == 3
        assert mock_sleep.call_count == 2  # Two retries

    @patch("tests.integration.conftest.create_engine")
    @patch("tests.integration.conftest.time.sleep")
    @patch("tests.integration.conftest.time.time")
    def test_wait_for_postgres_raises_on_timeout(
        self, mock_time: MagicMock, mock_sleep: MagicMock, mock_create_engine: MagicMock
    ) -> None:
        """_wait_for_postgres raises last exception if timeout exceeded."""
        # Simulate time progression beyond timeout
        mock_time.side_effect = [0.0, 1.0, 61.0]  # Start, retry, timeout

        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        connection_error = Exception("Connection refused")
        mock_engine.connect.side_effect = connection_error

        with pytest.raises(Exception) as exc_info:
            conftest._wait_for_postgres("postgresql://localhost/test", timeout=60.0)

        assert exc_info.value is connection_error

