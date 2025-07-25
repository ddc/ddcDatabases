import pytest
import sys
from sqlalchemy.pool import StaticPool
from tests.data.base_data import get_fake_test_data


@pytest.fixture(name="sqlite_session", scope="session")
def sqlite_session():
    # Import Sqlite only when needed to avoid early module loading
    from ddcDatabases import Sqlite
    
    extra_engine_args = {"poolclass": StaticPool}
    with Sqlite(
        filepath=":memory:",
        extra_engine_args=extra_engine_args,
    ) as session:
        yield session


@pytest.fixture(name="fake_test_data", scope="session")
def fake_test_data(sqlite_session):
    fdata = get_fake_test_data()
    yield fdata


@pytest.fixture(autouse=True)
def clear_settings_cache():
    """Clear all settings caches before and after each test to ensure isolation."""
    # Import settings functions dynamically to avoid import-time caching
    try:
        from ddcDatabases.settings import (
            get_sqlite_settings,
            get_postgresql_settings,
            get_mssql_settings,
            get_mysql_settings,
            get_mongodb_settings,
            get_oracle_settings,
        )
        
        # Multiple rounds of aggressive cache clearing
        for _ in range(10):
            get_sqlite_settings.cache_clear()
            get_postgresql_settings.cache_clear()
            get_mssql_settings.cache_clear()
            get_mysql_settings.cache_clear()
            get_mongodb_settings.cache_clear()
            get_oracle_settings.cache_clear()
        
        # Reset dotenv loaded flag to ensure clean state
        import ddcDatabases.settings
        ddcDatabases.settings._dotenv_loaded = False
        
        # Force garbage collection to clear any references
        import gc
        gc.collect()
        
    except ImportError:
        # Settings module not yet imported, that's fine
        pass
    
    yield
    
    # Clear again after test to prevent interference
    try:
        # Multiple rounds of aggressive cache clearing
        for _ in range(10):
            get_sqlite_settings.cache_clear()
            get_postgresql_settings.cache_clear()
            get_mssql_settings.cache_clear()
            get_mysql_settings.cache_clear()
            get_mongodb_settings.cache_clear()
            get_oracle_settings.cache_clear()
        ddcDatabases.settings._dotenv_loaded = False
        gc.collect()
    except (NameError, ImportError):
        # Settings functions may not be available import failed
        pass
