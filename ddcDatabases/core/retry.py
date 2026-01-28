from __future__ import annotations

import asyncio
import logging
import random
import time
from .configs import BaseRetryConfig
from .constants import CONNECTION_ERROR_KEYWORDS
from typing import Awaitable, Callable, TypeVar

# Type variable for generic return types
T = TypeVar('T')

_logger = logging.getLogger(__name__)
_logger.addHandler(logging.NullHandler())


def _is_connection_error(exc: Exception) -> bool:
    """
    Detect if an exception is connection-related.

    Args:
        exc: The exception to check

    Returns:
        True if the exception appears to be connection-related
    """
    error_str = str(exc).lower()
    exc_type_name = type(exc).__name__.lower()

    # Check exception type name
    if any(keyword in exc_type_name for keyword in ("connection", "network", "timeout", "operational")):
        return True

    # Check error message for keywords
    return any(keyword in error_str for keyword in CONNECTION_ERROR_KEYWORDS)


def _calculate_retry_delay(attempt: int, config: BaseRetryConfig) -> float:
    """
    Calculate the delay before the next retry using exponential backoff with jitter.

    Args:
        attempt: Current retry attempt (0-indexed)
        config: Retry configuration

    Returns:
        Delay in seconds
    """
    # Exponential backoff: delay = initial_retry_delay * 2^attempt
    base_delay = config.initial_retry_delay * (2**attempt)

    # Cap at max_retry_delay
    capped_delay = min(base_delay, config.max_retry_delay)

    # Add jitter (randomize +/- jitter%)
    jitter = getattr(config, 'jitter', 0.0) or 0.0
    jitter_range = capped_delay * jitter
    jitter_offset = random.uniform(-jitter_range, jitter_range)

    return max(0, capped_delay + jitter_offset)


def _handle_retry_exception(
    e: Exception,
    attempt: int,
    config: BaseRetryConfig,
    operation_name: str,
    logger: logging.Logger | None = None,
) -> float:
    """
    Handle an exception during retry operation.

    Args:
        e: The exception that was raised
        attempt: Current retry attempt (0-indexed)
        config: Retry configuration
        operation_name: Name of the operation for logging
        logger: Optional logger instance (falls back to module-level logger)

    Returns:
        Delay in seconds before next retry

    Raises:
        The exception if it's not retryable or max retries reached
    """
    log = logger or _logger

    if not _is_connection_error(e):
        # Not a connection error, don't retry
        raise

    if attempt >= config.max_retries:
        # No more retries
        log.error(f"[{operation_name}] All {config.max_retries + 1} attempts failed. Last error: {e!r}")
        raise

    delay = _calculate_retry_delay(attempt, config)
    log.warning(
        f"[{operation_name}] Attempt {attempt + 1}/{config.max_retries + 1} failed: {e!r}. "
        f"Retrying in {delay:.2f}s..."
    )
    return delay


def retry_operation(
    operation: Callable[[], T],
    config: BaseRetryConfig,
    operation_name: str = "operation",
    logger: logging.Logger | None = None,
) -> T:
    """
    Execute an operation with retry logic (synchronous).

    Args:
        operation: Callable to execute
        config: Retry configuration
        operation_name: Name of the operation for logging
        logger: Optional logger instance (falls back to module-level logger)

    Returns:
        Result of the operation

    Raises:
        The last exception if all retries fail
    """
    if not config.enable_retry:
        return operation()

    last_exception: Exception | None = None

    for attempt in range(config.max_retries + 1):
        try:
            return operation()
        except Exception as e:
            last_exception = e
            delay = _handle_retry_exception(e, attempt, config, operation_name, logger)
            time.sleep(delay)

    # Should not reach here, but satisfy type checker
    if last_exception:
        raise last_exception
    raise RuntimeError("Unexpected state in retry logic")


async def retry_operation_async(
    operation: Callable[[], Awaitable[T]],
    config: BaseRetryConfig,
    operation_name: str = "operation",
    logger: logging.Logger | None = None,
) -> T:
    """
    Execute an operation with retry logic (asynchronous).

    Args:
        operation: Async callable to execute
        config: Retry configuration
        operation_name: Name of the operation for logging
        logger: Optional logger instance (falls back to module-level logger)

    Returns:
        Result of the operation

    Raises:
        The last exception if all retries fail
    """
    if not config.enable_retry:
        return await operation()

    last_exception: Exception | None = None

    for attempt in range(config.max_retries + 1):
        try:
            return await operation()
        except Exception as e:
            last_exception = e
            delay = _handle_retry_exception(e, attempt, config, operation_name, logger)
            await asyncio.sleep(delay)

    # Should not reach here, but satisfy type checker
    if last_exception:
        raise last_exception
    raise RuntimeError("Unexpected state in retry logic")
