from __future__ import annotations

import asyncio
from dataclasses import dataclass
import logging
import random
import time
from typing import Awaitable, Callable, TypeVar

# Type variable for generic return types
T = TypeVar('T')

# Logger for retry operations
_logger = logging.getLogger(__name__)
_logger.addHandler(logging.NullHandler())

# Keywords that indicate connection-related errors
CONNECTION_ERROR_KEYWORDS: frozenset[str] = frozenset(
    {
        "connection",
        "connect",
        "timeout",
        "timed out",
        "refused",
        "reset",
        "broken pipe",
        "network",
        "socket",
        "server closed",
        "lost connection",
        "server has gone away",
        "communication link",
        "operational error",
        "connection refused",
        "connection reset",
        "connection timed out",
        "no route to host",
        "host unreachable",
        "name or service not known",
        "temporary failure",
        "eof detected",
        "ssl error",
        "handshake failure",
        "authentication failed",
        "too many connections",
        "connection pool",
        "pool exhausted",
    }
)


@dataclass(slots=True, frozen=True)
class RetryPolicy:
    """Configuration for retry behavior on database operations."""

    enable_retry: bool = True
    max_retries: int = 3
    initial_delay: float = 1.0
    max_delay: float = 30.0
    jitter: float = 0.1

    def __post_init__(self) -> None:
        if self.max_retries < 0:
            raise ValueError("max_retries must be non-negative")
        if self.initial_delay < 0:
            raise ValueError("initial_delay must be non-negative")
        if self.max_delay < self.initial_delay:
            raise ValueError("max_delay must be >= initial_delay")
        if not 0 <= self.jitter <= 1:
            raise ValueError("jitter must be between 0 and 1")


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


def _calculate_retry_delay(attempt: int, config: RetryPolicy) -> float:
    """
    Calculate the delay before the next retry using exponential backoff with jitter.

    Args:
        attempt: Current retry attempt (0-indexed)
        config: Retry configuration

    Returns:
        Delay in seconds
    """
    # Exponential backoff: delay = initial_delay * 2^attempt
    base_delay = config.initial_delay * (2**attempt)

    # Cap at max_delay
    capped_delay = min(base_delay, config.max_delay)

    # Add jitter (randomize +/- jitter%)
    jitter_range = capped_delay * config.jitter
    jitter_offset = random.uniform(-jitter_range, jitter_range)

    return max(0, capped_delay + jitter_offset)


def _handle_retry_exception(
    e: Exception,
    attempt: int,
    config: RetryPolicy,
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
    config: RetryPolicy,
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
    config: RetryPolicy,
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
