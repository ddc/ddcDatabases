import pytest


@pytest.fixture(autouse=True)
def clear_settings_cache():
    """Clear all settings caches before and after each test to ensure isolation."""
    try:
        import ddcDatabases.core.settings
        from ddcDatabases.core.settings import (
            get_mongodb_settings,
            get_mssql_settings,
            get_mysql_settings,
            get_oracle_settings,
            get_postgresql_settings,
            get_sqlite_settings,
        )

        get_sqlite_settings.cache_clear()
        get_postgresql_settings.cache_clear()
        get_mssql_settings.cache_clear()
        get_mysql_settings.cache_clear()
        get_mongodb_settings.cache_clear()
        get_oracle_settings.cache_clear()
        ddcDatabases.core.settings._dotenv_loaded = False
    except ImportError:
        pass

    yield

    try:
        get_sqlite_settings.cache_clear()
        get_postgresql_settings.cache_clear()
        get_mssql_settings.cache_clear()
        get_mysql_settings.cache_clear()
        get_mongodb_settings.cache_clear()
        get_oracle_settings.cache_clear()
        ddcDatabases.core.settings._dotenv_loaded = False
    except (NameError, ImportError):
        pass
