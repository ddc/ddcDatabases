# -*- coding: utf-8 -*-
import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession

from ddcDatabases.oracle import Oracle
from ddcDatabases.db_utils import ConnectionTester


class TestOracle:
    """Test Oracle database connection class"""
    
    @patch('ddcDatabases.oracle.get_oracle_settings')
    def test_init_basic(self, mock_get_settings):
        """Test Oracle basic initialization"""
        mock_settings = MagicMock()
        mock_settings.user = "system"
        mock_settings.password = "oracle"
        mock_settings.host = "localhost"
        mock_settings.port = 1521
        mock_settings.servicename = "xe"
        mock_settings.echo = False
        mock_settings.sync_driver = "oracle+cx_oracle"
        mock_get_settings.return_value = mock_settings
        
        oracle = Oracle()
        
        assert oracle.connection_url["host"] == "localhost"
        assert oracle.connection_url["port"] == 1521
        assert oracle.connection_url["username"] == "system"
        assert oracle.connection_url["password"] == "oracle"
        assert oracle.connection_url["query"]["service_name"] == "xe"
        assert oracle.sync_driver == "oracle+cx_oracle"
        
    @patch('ddcDatabases.oracle.get_oracle_settings')
    def test_init_missing_credentials(self, mock_get_settings):
        """Test Oracle initialization with missing credentials"""
        mock_settings = MagicMock()
        mock_settings.user = None
        mock_settings.password = "oracle"
        mock_get_settings.return_value = mock_settings
        
        with pytest.raises(RuntimeError, match="Missing username/password"):
            Oracle()
            
    @patch('ddcDatabases.oracle.get_oracle_settings')
    def test_init_with_parameters(self, mock_get_settings):
        """Test Oracle initialization with override parameters"""
        mock_settings = MagicMock()
        mock_settings.user = "defaultuser"
        mock_settings.password = "defaultpass"
        mock_settings.host = "defaulthost"
        mock_settings.port = 1521
        mock_settings.servicename = "defaultxe"
        mock_settings.echo = False
        mock_settings.sync_driver = "oracle+cx_oracle"
        mock_get_settings.return_value = mock_settings
        
        oracle = Oracle(
            host="customhost",
            port=1522,
            user="customuser",
            password="custompass",
            servicename="customxe",
            echo=True
        )
        
        assert oracle.connection_url["host"] == "customhost"
        assert oracle.connection_url["port"] == 1522
        assert oracle.connection_url["username"] == "customuser"
        assert oracle.connection_url["password"] == "custompass"
        assert oracle.connection_url["query"]["service_name"] == "customxe"
        assert oracle.echo == True
        
    @patch('ddcDatabases.oracle.get_oracle_settings')  
    def test_minimal_init(self, mock_get_settings):
        """Test Oracle minimal initialization"""
        mock_settings = MagicMock()
        mock_settings.user = "system"
        mock_settings.password = "oracle"
        mock_settings.host = "localhost"
        mock_settings.port = 1521
        mock_settings.servicename = "xe"
        mock_settings.echo = False
        mock_settings.sync_driver = "oracle+cx_oracle"
        mock_get_settings.return_value = mock_settings
        
        oracle = Oracle()
        
        assert oracle.connection_url["host"] == "localhost"
        assert oracle.connection_url["port"] == 1521
        # Oracle uses 'query' field for servicename
        assert "query" in oracle.connection_url
        assert oracle.connection_url["query"]["service_name"] == "xe"
        assert oracle.sync_driver == "oracle+cx_oracle"
        
    def test_test_connection_sync_oracle(self):
        """Test connection test for Oracle database"""
        mock_session = MagicMock()
        mock_session.bind.url = "oracle://user@host/xe"
        
        test_conn = ConnectionTester(sync_session=mock_session)
        result = test_conn.test_connection_sync()
        
        assert result == True
        mock_session.execute.assert_called_once()
        # Check that Oracle-specific query was used
        call_args = mock_session.execute.call_args[0][0]
        assert "SELECT 1 FROM dual" in str(call_args)
        
    @pytest.mark.asyncio
    async def test_test_connection_async_oracle(self):
        """Test async connection test for Oracle database"""
        mock_session = AsyncMock()
        mock_session.bind.url = "oracle://user@host/xe"
        
        test_conn = ConnectionTester(async_session=mock_session)
        result = await test_conn.test_connection_async()
        
        assert result == True
        mock_session.execute.assert_called_once()
        
    @patch('ddcDatabases.oracle.get_oracle_settings')
    def test_extra_engine_args(self, mock_get_settings):
        """Test Oracle with extra engine arguments"""
        mock_settings = MagicMock()
        mock_settings.user = "system"
        mock_settings.password = "oracle"
        mock_settings.host = "localhost"
        mock_settings.port = 1521
        mock_settings.servicename = "xe"
        mock_settings.echo = False
        mock_settings.sync_driver = "oracle+cx_oracle"
        mock_get_settings.return_value = mock_settings
        
        extra_args = {"encoding": "UTF-8", "nencoding": "UTF-8"}
        oracle = Oracle(extra_engine_args=extra_args)
        
        # Test that extra args are included in engine_args
        assert "encoding" in oracle.engine_args
        assert oracle.engine_args["encoding"] == "UTF-8"
        assert oracle.engine_args["nencoding"] == "UTF-8"
        
        # Test that default args are still present
        assert oracle.engine_args["echo"] == False
        
    @patch('ddcDatabases.oracle.get_oracle_settings')
    def test_autoflush_and_expire_on_commit(self, mock_get_settings):
        """Test Oracle autoflush and expire_on_commit parameters"""
        mock_settings = MagicMock()
        mock_settings.user = "system"
        mock_settings.password = "oracle"
        mock_settings.host = "localhost"
        mock_settings.port = 1521
        mock_settings.servicename = "xe"
        mock_settings.echo = False
        mock_settings.sync_driver = "oracle+cx_oracle"
        mock_get_settings.return_value = mock_settings
        
        oracle = Oracle(autoflush=False, expire_on_commit=False)
        
        assert oracle.autoflush == False
        assert oracle.expire_on_commit == False
        
    @patch('ddcDatabases.oracle.get_oracle_settings')
    def test_service_name_in_query(self, mock_get_settings):
        """Test that Oracle service name is properly set in query parameters"""
        mock_settings = MagicMock()
        mock_settings.user = "system"
        mock_settings.password = "oracle"
        mock_settings.host = "testhost"
        mock_settings.port = 1521
        mock_settings.servicename = "testxe"
        mock_settings.echo = False
        mock_settings.sync_driver = "oracle+cx_oracle"
        mock_get_settings.return_value = mock_settings
        
        oracle = Oracle()
        
        # Test connection URL structure for Oracle
        assert oracle.connection_url["host"] == "testhost"
        assert oracle.connection_url["port"] == 1521
        
        # Oracle should include service_name in query parameters
        assert "query" in oracle.connection_url
        assert oracle.connection_url["query"]["service_name"] == "testxe"