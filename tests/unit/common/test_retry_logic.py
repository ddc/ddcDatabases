"""Tests for retry logic functionality."""

import asyncio
from ddcDatabases.core.retry import (
    CONNECTION_ERROR_KEYWORDS,
    RetryPolicy,
    _calculate_retry_delay,
    _handle_retry_exception,
    _is_connection_error,
    retry_operation,
    retry_operation_async,
)
import pytest
import time
from unittest.mock import MagicMock, patch


class TestRetryPolicy:
    """Test RetryPolicy dataclass."""

    def test_default_values(self):
        """Test RetryPolicy default values."""
        config = RetryPolicy()
        assert config.enable_retry is True
        assert config.max_retries == 3
        assert config.initial_delay == pytest.approx(1.0)
        assert config.max_delay == pytest.approx(30.0)
        assert config.jitter == pytest.approx(0.1)

    def test_custom_values(self):
        """Test RetryPolicy with custom values."""
        config = RetryPolicy(
            enable_retry=False,
            max_retries=5,
            initial_delay=2.0,
            max_delay=60.0,
            jitter=0.2,
        )
        assert config.enable_retry is False
        assert config.max_retries == 5
        assert config.initial_delay == pytest.approx(2.0)
        assert config.max_delay == pytest.approx(60.0)
        assert config.jitter == pytest.approx(0.2)

    def test_invalid_max_retries(self):
        """Test that negative max_retries raises ValueError."""
        with pytest.raises(ValueError, match="max_retries must be non-negative"):
            RetryPolicy(max_retries=-1)

    def test_invalid_initial_delay(self):
        """Test that negative initial_delay raises ValueError."""
        with pytest.raises(ValueError, match="initial_delay must be non-negative"):
            RetryPolicy(initial_delay=-1.0)

    def test_invalid_max_delay(self):
        """Test that max_delay < initial_delay raises ValueError."""
        with pytest.raises(ValueError, match="max_delay must be >= initial_delay"):
            RetryPolicy(initial_delay=10.0, max_delay=5.0)

    def test_invalid_jitter(self):
        """Test that jitter outside 0-1 range raises ValueError."""
        with pytest.raises(ValueError, match="jitter must be between 0 and 1"):
            RetryPolicy(jitter=1.5)

        with pytest.raises(ValueError, match="jitter must be between 0 and 1"):
            RetryPolicy(jitter=-0.1)

    def test_zero_max_retries(self):
        """Test that zero max_retries is valid."""
        config = RetryPolicy(max_retries=0)
        assert config.max_retries == 0

    def test_frozen_config(self):
        """Test that RetryPolicy is frozen (immutable)."""
        config = RetryPolicy()
        with pytest.raises(AttributeError):
            config.max_retries = 10


class TestConnectionErrorKeywords:
    """Test connection error keywords."""

    def test_keywords_exist(self):
        """Test that connection error keywords exist."""
        assert len(CONNECTION_ERROR_KEYWORDS) > 0

    def test_keywords_are_frozenset(self):
        """Test that keywords are a frozenset."""
        assert isinstance(CONNECTION_ERROR_KEYWORDS, frozenset)

    def test_common_keywords_included(self):
        """Test that common connection error keywords are included."""
        expected_keywords = [
            "connection",
            "timeout",
            "refused",
            "network",
            "socket",
        ]
        for keyword in expected_keywords:
            assert keyword in CONNECTION_ERROR_KEYWORDS


class TestIsConnectionError:
    """Test _is_connection_error function."""

    def test_connection_error_by_message(self):
        """Test detection by error message."""
        assert _is_connection_error(Exception("Connection refused"))
        assert _is_connection_error(Exception("Network timeout occurred"))
        assert _is_connection_error(Exception("Socket error"))
        assert _is_connection_error(Exception("server closed connection"))

    def test_connection_error_by_type_name(self):
        """Test detection by exception type name."""

        class ConnectionError(Exception):
            pass

        class NetworkError(Exception):
            pass

        class TimeoutException(Exception):
            pass

        assert _is_connection_error(ConnectionError("test"))
        assert _is_connection_error(NetworkError("test"))
        assert _is_connection_error(TimeoutException("test"))

    def test_non_connection_error(self):
        """Test that non-connection errors are not detected."""
        assert not _is_connection_error(Exception("Invalid syntax"))
        assert not _is_connection_error(ValueError("Bad value"))
        assert not _is_connection_error(TypeError("Type mismatch"))

    def test_case_insensitive_detection(self):
        """Test that detection is case-insensitive."""
        assert _is_connection_error(Exception("CONNECTION REFUSED"))
        assert _is_connection_error(Exception("Timeout Occurred"))


class TestCalculateRetryDelay:
    """Test _calculate_retry_delay function."""

    def test_exponential_backoff(self):
        """Test exponential backoff calculation."""
        config = RetryPolicy(initial_delay=1.0, max_delay=30.0, jitter=0)

        # Delay should double each attempt
        delay_0 = _calculate_retry_delay(0, config)
        delay_1 = _calculate_retry_delay(1, config)
        delay_2 = _calculate_retry_delay(2, config)

        assert abs(delay_0 - 1.0) < 0.01
        assert abs(delay_1 - 2.0) < 0.01
        assert abs(delay_2 - 4.0) < 0.01

    def test_max_delay_cap(self):
        """Test that delay is capped at max_delay."""
        config = RetryPolicy(initial_delay=1.0, max_delay=10.0, jitter=0)

        # Even with many attempts, delay shouldn't exceed max_delay
        delay = _calculate_retry_delay(10, config)
        assert delay <= 10.0

    def test_jitter_variation(self):
        """Test that jitter adds variation."""
        config = RetryPolicy(initial_delay=10.0, max_delay=30.0, jitter=0.1)

        # With jitter, delays should vary
        delays = [_calculate_retry_delay(0, config) for _ in range(10)]

        # All delays should be within jitter range of 10.0
        for delay in delays:
            assert 9.0 <= delay <= 11.0

        # Delays should not all be identical (very low probability)
        assert len(set(delays)) > 1

    def test_zero_jitter(self):
        """Test with zero jitter."""
        config = RetryPolicy(initial_delay=1.0, max_delay=30.0, jitter=0)

        delays = [_calculate_retry_delay(0, config) for _ in range(10)]

        # All delays should be exactly the same
        assert all(d == delays[0] for d in delays)


class TestHandleRetryException:
    """Test _handle_retry_exception function."""

    def test_raises_non_connection_error(self):
        """Test that non-connection errors are re-raised immediately."""
        config = RetryPolicy()

        # Must call within exception context since function uses bare 'raise'
        try:
            raise ValueError("bad value")
        except ValueError as e:
            with pytest.raises(ValueError):
                _handle_retry_exception(e, 0, config, "test_op")

    def test_raises_when_max_retries_reached(self):
        """Test that error is re-raised when max retries reached."""
        config = RetryPolicy(max_retries=2)

        # Must call within exception context since function uses bare 'raise'
        try:
            raise ConnectionError("fail")
        except ConnectionError as e:
            with pytest.raises(ConnectionError):
                _handle_retry_exception(e, 2, config, "test_op")

    def test_returns_delay_for_connection_error(self):
        """Test that delay is returned for retryable connection errors."""
        config = RetryPolicy(max_retries=3, initial_delay=1.0, jitter=0)

        try:
            raise ConnectionError("fail")
        except ConnectionError as e:
            delay = _handle_retry_exception(e, 0, config, "test_op")
            assert delay == pytest.approx(1.0)

    def test_returns_exponential_delay(self):
        """Test that delay increases exponentially."""
        config = RetryPolicy(max_retries=5, initial_delay=1.0, jitter=0)

        try:
            raise ConnectionError("fail")
        except ConnectionError as e:
            delay_0 = _handle_retry_exception(e, 0, config, "test_op")
            delay_1 = _handle_retry_exception(e, 1, config, "test_op")
            delay_2 = _handle_retry_exception(e, 2, config, "test_op")

            assert delay_0 == pytest.approx(1.0)
            assert delay_1 == pytest.approx(2.0)
            assert delay_2 == pytest.approx(4.0)

    def test_raises_at_exact_max_retries_boundary(self):
        """Test boundary condition at exactly max_retries."""
        config = RetryPolicy(max_retries=3)

        try:
            raise ConnectionError("fail")
        except ConnectionError as e:
            # Attempt 3 (0-indexed) should raise since it equals max_retries
            with pytest.raises(ConnectionError):
                _handle_retry_exception(e, 3, config, "test_op")

            # Attempt 2 should still return delay
            delay = _handle_retry_exception(e, 2, config, "test_op")
            assert delay > 0


class TestRetryOperationSync:
    """Test _retry_operation synchronous function."""

    def test_success_first_try(self):
        """Test successful operation on first try."""
        config = RetryPolicy()
        operation = MagicMock(return_value="success")

        result = retry_operation(operation, config, "test_op")

        assert result == "success"
        operation.assert_called_once()

    def test_success_after_retry(self):
        """Test successful operation after retry."""
        config = RetryPolicy(initial_delay=0.01)
        operation = MagicMock(side_effect=[ConnectionError("fail"), "success"])

        result = retry_operation(operation, config, "test_op")

        assert result == "success"
        assert operation.call_count == 2

    def test_all_retries_exhausted(self):
        """Test that exception is raised after all retries."""
        config = RetryPolicy(max_retries=2, initial_delay=0.01)
        operation = MagicMock(side_effect=ConnectionError("fail"))

        with pytest.raises(ConnectionError):
            retry_operation(operation, config, "test_op")

        assert operation.call_count == 3  # Initial + 2 retries

    def test_non_connection_error_not_retried(self):
        """Test that non-connection errors are not retried."""
        config = RetryPolicy()
        operation = MagicMock(side_effect=ValueError("bad value"))

        with pytest.raises(ValueError):
            retry_operation(operation, config, "test_op")

        operation.assert_called_once()

    def test_retry_disabled(self):
        """Test that retry is skipped when disabled."""
        config = RetryPolicy(enable_retry=False)
        operation = MagicMock(side_effect=ConnectionError("fail"))

        with pytest.raises(ConnectionError):
            retry_operation(operation, config, "test_op")

        operation.assert_called_once()

    @patch('ddcDatabases.core.retry.time.sleep')
    def test_delay_between_retries(self, mock_sleep):
        """Test that delays occur between retries."""
        config = RetryPolicy(max_retries=2, initial_delay=1.0, jitter=0)
        operation = MagicMock(side_effect=[ConnectionError("fail"), ConnectionError("fail"), "success"])

        result = retry_operation(operation, config, "test_op")

        assert result == "success"
        assert mock_sleep.call_count == 2

    def test_zero_max_retries_single_attempt(self):
        """Test with zero max_retries - only one attempt is made."""
        config = RetryPolicy(max_retries=0, initial_delay=0.01)
        operation = MagicMock(side_effect=ConnectionError("fail"))

        with pytest.raises(ConnectionError):
            retry_operation(operation, config, "test_op")

        # With max_retries=0, only 1 attempt (no retries)
        operation.assert_called_once()

    def test_zero_max_retries_success(self):
        """Test with zero max_retries succeeds on first try."""
        config = RetryPolicy(max_retries=0)
        operation = MagicMock(return_value="success")

        result = retry_operation(operation, config, "test_op")

        assert result == "success"
        operation.assert_called_once()

    def test_returns_falsy_value(self):
        """Test that falsy return values (0, False, None, '') are returned correctly."""
        config = RetryPolicy()

        for falsy_value in [0, False, None, '', [], {}]:
            operation = MagicMock(return_value=falsy_value)
            result = retry_operation(operation, config, "test_op")
            assert result == falsy_value


class TestRetryOperationAsync:
    """Test _retry_operation_async asynchronous function."""

    @pytest.mark.asyncio
    async def test_success_first_try(self):
        """Test successful async operation on first try."""
        config = RetryPolicy()

        async def operation():
            return "success"

        result = await retry_operation_async(operation, config, "test_op")
        assert result == "success"

    @pytest.mark.asyncio
    async def test_success_after_retry(self):
        """Test successful async operation after retry."""
        config = RetryPolicy(initial_delay=0.01)
        call_count = [0]

        async def operation():
            call_count[0] += 1
            if call_count[0] == 1:
                raise ConnectionError("fail")
            return "success"

        result = await retry_operation_async(operation, config, "test_op")

        assert result == "success"
        assert call_count[0] == 2

    @pytest.mark.asyncio
    async def test_all_retries_exhausted(self):
        """Test that exception is raised after all async retries."""
        config = RetryPolicy(max_retries=2, initial_delay=0.01)

        async def operation():
            raise ConnectionError("fail")

        with pytest.raises(ConnectionError):
            await retry_operation_async(operation, config, "test_op")

    @pytest.mark.asyncio
    async def test_non_connection_error_not_retried(self):
        """Test that non-connection errors are not retried in async."""
        config = RetryPolicy()
        call_count = [0]

        async def operation():
            call_count[0] += 1
            raise ValueError("bad value")

        with pytest.raises(ValueError):
            await retry_operation_async(operation, config, "test_op")

        assert call_count[0] == 1

    @pytest.mark.asyncio
    async def test_zero_max_retries_single_attempt(self):
        """Test async with zero max_retries - only one attempt is made."""
        config = RetryPolicy(max_retries=0, initial_delay=0.01)
        call_count = [0]

        async def operation():
            call_count[0] += 1
            raise ConnectionError("fail")

        with pytest.raises(ConnectionError):
            await retry_operation_async(operation, config, "test_op")

        assert call_count[0] == 1

    @pytest.mark.asyncio
    async def test_returns_falsy_value(self):
        """Test that falsy return values are returned correctly in async."""
        config = RetryPolicy()

        for falsy_value in [0, False, None, '', [], {}]:

            async def operation(val=falsy_value):
                return val

            result = await retry_operation_async(operation, config, "test_op")
            assert result == falsy_value

    @pytest.mark.asyncio
    async def test_retry_disabled(self):
        """Test that async retry is skipped when disabled."""
        config = RetryPolicy(enable_retry=False)
        call_count = [0]

        async def operation():
            call_count[0] += 1
            raise ConnectionError("fail")

        with pytest.raises(ConnectionError):
            await retry_operation_async(operation, config, "test_op")

        assert call_count[0] == 1


class TestRetryPolicyInDatabaseClasses:
    """Test retry config integration in database classes."""

    def test_postgresql_retry_config(self):
        """Test PostgreSQL includes retry config."""
        from ddcDatabases.postgresql import PostgreSQL

        pg = PostgreSQL()
        retry_info = pg.get_retry_info()

        assert retry_info.enable_retry is True
        assert retry_info.max_retries == 3
        assert retry_info.initial_retry_delay == pytest.approx(1.0)
        assert retry_info.max_retry_delay == pytest.approx(30.0)

    def test_mysql_retry_config(self):
        """Test MySQL includes retry config."""
        from ddcDatabases.mysql import MySQL

        mysql = MySQL()
        retry_info = mysql.get_retry_info()

        assert retry_info.enable_retry is True
        assert retry_info.max_retries == 3

    def test_mssql_retry_config(self):
        """Test MSSQL includes retry config."""
        from ddcDatabases.mssql import MSSQL

        mssql = MSSQL()
        retry_info = mssql.get_retry_info()

        assert retry_info.enable_retry is True
        assert retry_info.max_retries == 3

    def test_oracle_retry_config(self):
        """Test Oracle includes retry config."""
        from ddcDatabases.oracle import Oracle

        oracle = Oracle()
        retry_info = oracle.get_retry_info()

        assert retry_info.enable_retry is True
        assert retry_info.max_retries == 3

    def test_sqlite_retry_config(self):
        """Test SQLite includes retry config."""
        from ddcDatabases.sqlite import Sqlite

        sqlite = Sqlite()
        retry_info = sqlite.get_retry_info()

        # SQLite has retry disabled by default
        assert retry_info.enable_retry is False
        assert retry_info.max_retries == 1

    def test_mongodb_retry_config(self):
        """Test MongoDB includes retry config."""
        from ddcDatabases.mongodb import MongoDB

        mongodb = MongoDB(collection="test")
        retry_info = mongodb.get_retry_info()

        assert retry_info.enable_retry is True
        assert retry_info.max_retries == 3

    def test_custom_retry_settings(self):
        """Test passing custom retry settings to database class."""
        from ddcDatabases.core.configs import RetryConfig
        from ddcDatabases.postgresql import PostgreSQL

        pg = PostgreSQL(
            retry_config=RetryConfig(
                enable_retry=False,
                max_retries=5,
                initial_retry_delay=2.0,
                max_retry_delay=60.0,
            ),
        )
        retry_info = pg.get_retry_info()

        assert retry_info.enable_retry is False
        assert retry_info.max_retries == 5
        assert retry_info.initial_retry_delay == pytest.approx(2.0)
        assert retry_info.max_retry_delay == pytest.approx(60.0)
