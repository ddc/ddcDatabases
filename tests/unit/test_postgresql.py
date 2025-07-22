# -*- coding: utf-8 -*-
import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession

from ddcDatabases.postgresql import PostgreSQL


class TestPostgreSQL:
    """Test PostgreSQL database connection class"""
    
    @patch('ddcDatabases.postgresql.get_postgresql_settings')
    def test_init_basic(self, mock_get_settings):
        """Test PostgreSQL basic initialization"""
        mock_settings = MagicMock()
        mock_settings.user = "postgres"
        mock_settings.password = "password"
        mock_settings.host = "localhost"
        mock_settings.port = 5432
        mock_settings.database = "postgres"
        mock_settings.echo = False
        mock_settings.sync_driver = "postgresql+psycopg2"
        mock_settings.async_driver = "postgresql+asyncpg"
        mock_get_settings.return_value = mock_settings
        
        postgresql = PostgreSQL()
        
        assert postgresql.connection_url["host"] == "localhost"
        assert postgresql.connection_url["port"] == 5432
        assert postgresql.connection_url["database"] == "postgres"
        assert postgresql.connection_url["username"] == "postgres"
        assert postgresql.connection_url["password"] == "password"
        assert postgresql.sync_driver == "postgresql+psycopg2"
        assert postgresql.async_driver == "postgresql+asyncpg"
        
    @patch('ddcDatabases.postgresql.get_postgresql_settings')
    def test_init_missing_credentials(self, mock_get_settings):
        """Test PostgreSQL initialization with missing credentials"""
        mock_settings = MagicMock()
        mock_settings.user = None
        mock_settings.password = "password"
        mock_get_settings.return_value = mock_settings
        
        with pytest.raises(RuntimeError, match="Missing username/password"):
            PostgreSQL()
    
    @patch('ddcDatabases.postgresql.get_postgresql_settings')
    def test_init_with_parameters(self, mock_get_settings):
        """Test PostgreSQL initialization with override parameters"""
        mock_settings = MagicMock()
        mock_settings.user = "defaultuser"
        mock_settings.password = "defaultpass"
        mock_settings.host = "defaulthost"
        mock_settings.port = 5432
        mock_settings.database = "defaultdb"
        mock_settings.echo = False
        mock_settings.sync_driver = "postgresql+psycopg2"
        mock_settings.async_driver = "postgresql+asyncpg"
        mock_get_settings.return_value = mock_settings
        
        postgresql = PostgreSQL(
            host="customhost",
            port=5433,
            user="customuser",
            password="custompass",
            database="customdb",
            echo=True
        )
        
        assert postgresql.connection_url["host"] == "customhost"
        assert postgresql.connection_url["port"] == 5433
        assert postgresql.connection_url["database"] == "customdb"
        assert postgresql.connection_url["username"] == "customuser"
        assert postgresql.connection_url["password"] == "custompass"
        assert postgresql.echo == True
        
    @patch('ddcDatabases.postgresql.get_postgresql_settings')
    @patch('ddcDatabases.db_utils.create_async_engine')  
    @patch('ddcDatabases.db_utils.sessionmaker')
    @pytest.mark.asyncio
    async def test_async_context_manager(self, mock_sessionmaker, mock_create_engine, mock_get_settings):
        """Test PostgreSQL async context manager"""
        mock_settings = MagicMock()
        mock_settings.user = "testuser"
        mock_settings.password = "testpass"
        mock_settings.host = "localhost"
        mock_settings.port = 5432
        mock_settings.database = "testdb"
        mock_settings.echo = False
        mock_settings.async_driver = "postgresql+asyncpg"
        mock_settings.sync_driver = "postgresql+psycopg2"
        mock_get_settings.return_value = mock_settings
        
        # Mock engine and session
        mock_engine = AsyncMock()
        mock_create_engine.return_value = mock_engine
        mock_session_class = MagicMock()
        mock_session = AsyncMock()
        mock_session_class.return_value = mock_session
        mock_sessionmaker.return_value = mock_session_class
        
        pg = PostgreSQL()
        
        async with pg as session:
            assert session is not None  # Session is wrapped in begin() context
            
        mock_create_engine.assert_called_once()
        assert mock_engine.dispose.call_count >= 1  # May be called multiple times
        
    @patch('ddcDatabases.postgresql.get_postgresql_settings')
    def test_extra_engine_args(self, mock_get_settings):
        """Test PostgreSQL with extra engine arguments"""
        mock_settings = MagicMock()
        mock_settings.user = "postgres"
        mock_settings.password = "password"
        mock_settings.host = "localhost"
        mock_settings.port = 5432
        mock_settings.database = "postgres"
        mock_settings.echo = False
        mock_settings.sync_driver = "postgresql+psycopg2"
        mock_settings.async_driver = "postgresql+asyncpg"
        mock_get_settings.return_value = mock_settings
        
        extra_args = {"pool_timeout": 60, "connect_timeout": 30}
        postgresql = PostgreSQL(extra_engine_args=extra_args)
        
        # Test that extra args are included in engine_args
        assert "pool_timeout" in postgresql.engine_args
        assert postgresql.engine_args["pool_timeout"] == 60
        assert postgresql.engine_args["connect_timeout"] == 30
        
        # Test that default args are still present
        assert postgresql.engine_args["echo"] == False
        
    @patch('ddcDatabases.postgresql.get_postgresql_settings')
    def test_autoflush_and_expire_on_commit(self, mock_get_settings):
        """Test PostgreSQL autoflush and expire_on_commit parameters"""
        mock_settings = MagicMock()
        mock_settings.user = "postgres"
        mock_settings.password = "password"
        mock_settings.host = "localhost"
        mock_settings.port = 5432
        mock_settings.database = "postgres"
        mock_settings.echo = False
        mock_settings.sync_driver = "postgresql+psycopg2"
        mock_settings.async_driver = "postgresql+asyncpg"
        mock_get_settings.return_value = mock_settings
        
        postgresql = PostgreSQL(autoflush=False, expire_on_commit=False)
        
        assert postgresql.autoflush == False
        assert postgresql.expire_on_commit == False