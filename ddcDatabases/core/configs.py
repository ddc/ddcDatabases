from dataclasses import dataclass


@dataclass(frozen=True)
class BaseConnectionConfig:
    host: str | None = None
    port: int | None = None
    user: str | None = None
    password: str | None = None


@dataclass(frozen=True)
class BaseSSLConfig:
    ssl_mode: str | None = None
    ssl_ca_cert_path: str | None = None
    ssl_client_cert_path: str | None = None
    ssl_client_key_path: str | None = None


@dataclass(frozen=True)
class PoolConfig:
    pool_size: int | None = None
    max_overflow: int | None = None
    pool_recycle: int | None = None
    connection_timeout: int | None = None


@dataclass(frozen=True)
class SessionConfig:
    echo: bool | None = None
    autoflush: bool | None = None
    expire_on_commit: bool | None = None
    autocommit: bool | None = None


@dataclass(frozen=True)
class RetryConfig:
    enable_retry: bool | None = None
    max_retries: int | None = None
    initial_retry_delay: float | None = None
    max_retry_delay: float | None = None
