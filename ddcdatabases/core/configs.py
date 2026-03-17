import dataclasses
from dataclasses import dataclass
from typing import Any, TypeVar

_C = TypeVar("_C")

# Field maps for merging retry configs with settings
CONNECTION_RETRY_FIELD_MAP: dict[str, str] = {
    "enable_retry": "connection_enable_retry",
    "max_retries": "connection_max_retries",
    "initial_retry_delay": "connection_initial_retry_delay",
    "max_retry_delay": "connection_max_retry_delay",
}

OPERATION_RETRY_FIELD_MAP: dict[str, str] = {
    "enable_retry": "operation_enable_retry",
    "max_retries": "operation_max_retries",
    "initial_retry_delay": "operation_initial_retry_delay",
    "max_retry_delay": "operation_max_retry_delay",
    "jitter": "operation_jitter",
}


def merge_config_with_settings(
    config_cls: type[_C],
    override: _C | None,
    settings: Any,
    field_map: dict[str, str] | None = None,
) -> _C:
    """Create config instance, using override values when not None, else settings defaults.

    Args:
        config_cls: The dataclass class to instantiate
        override: Optional override instance (or None to use all defaults)
        settings: Settings object with default values
        field_map: Dict mapping config field names to settings attribute names.
                   If None, config field names must match settings attribute names.
    """
    override = override or config_cls()
    field_map = field_map or {}
    kwargs = {}
    for field in dataclasses.fields(config_cls):  # type: ignore[arg-type]
        name = field.name
        settings_attr = field_map.get(name, name)
        val = getattr(override, name)
        kwargs[name] = val if val is not None else getattr(settings, settings_attr)
    return config_cls(**kwargs)


def _validate_retry_config(
    max_retries: int | None,
    initial_retry_delay: float | None,
    max_retry_delay: float | None,
) -> None:
    """Validation for retry configs to avoid super() issues with frozen slotted dataclasses"""
    if max_retries is not None and max_retries < 0:
        raise ValueError("max_retries must be non-negative")
    if initial_retry_delay is not None and initial_retry_delay < 0:
        raise ValueError("initial_retry_delay must be non-negative")
    if max_retry_delay is not None and initial_retry_delay is not None and max_retry_delay < initial_retry_delay:
        raise ValueError("max_retry_delay must be >= initial_retry_delay")


@dataclass(frozen=True, slots=True)
class BaseConnectionConfig:
    host: str | None = None
    port: int | None = None
    user: str | None = None
    password: str | None = None

    def __post_init__(self) -> None:
        if self.port is not None and not (1 <= self.port <= 65535):
            raise ValueError("port must be between 1 and 65535")


@dataclass(frozen=True, slots=True)
class BaseSSLConfig:
    ssl_mode: str | None = None
    ssl_ca_cert_path: str | None = None
    ssl_client_cert_path: str | None = None
    ssl_client_key_path: str | None = None


@dataclass(frozen=True, slots=True)
class BasePoolConfig:
    pool_size: int | None = None
    max_overflow: int | None = None
    pool_recycle: int | None = None
    connection_timeout: int | None = None

    def __post_init__(self) -> None:
        if self.pool_size is not None and self.pool_size < 0:
            raise ValueError("pool_size must be non-negative")
        if self.max_overflow is not None and self.max_overflow < 0:
            raise ValueError("max_overflow must be non-negative")
        if self.pool_recycle is not None and self.pool_recycle < 0:
            raise ValueError("pool_recycle must be non-negative")
        if self.connection_timeout is not None and self.connection_timeout < 0:
            raise ValueError("connection_timeout must be non-negative")


@dataclass(frozen=True, slots=True)
class BaseSessionConfig:
    echo: bool | None = None
    autoflush: bool | None = None
    expire_on_commit: bool | None = None
    autocommit: bool | None = None


@dataclass(frozen=True, slots=True)
class BaseRetryConfig:
    enable_retry: bool | None = None
    max_retries: int | None = None
    initial_retry_delay: float | None = None
    max_retry_delay: float | None = None

    def __post_init__(self) -> None:
        _validate_retry_config(self.max_retries, self.initial_retry_delay, self.max_retry_delay)


@dataclass(frozen=True, slots=True)
class BaseOperationRetryConfig(BaseRetryConfig):
    jitter: float | None = None

    def __post_init__(self) -> None:
        _validate_retry_config(self.max_retries, self.initial_retry_delay, self.max_retry_delay)
        if self.jitter is not None and not (0 <= self.jitter <= 1):
            raise ValueError("jitter must be between 0 and 1")
