# -*- coding: utf-8 -*-
import pytest
from unittest.mock import patch, MagicMock

from ddcDatabases.mysql import MySQL


class TestMySQL:
    """Test MySQL database connection class"""
    
    @patch('ddcDatabases.mysql.get_mysql_settings')
    def test_init_basic(self, mock_get_settings):
        """Test MySQL basic initialization"""
        mock_settings = MagicMock()
        mock_settings.user = "root"
        mock_settings.password = "password"
        mock_settings.host = "localhost"
        mock_settings.port = 3306
        mock_settings.database = "dev"
        mock_settings.echo = False
        mock_settings.sync_driver = "mysql+pymysql"
        mock_settings.async_driver = "mysql+aiomysql"
        mock_get_settings.return_value = mock_settings
        
        mysql = MySQL()
        
        assert mysql.connection_url["host"] == "localhost"
        assert mysql.connection_url["port"] == 3306
        assert mysql.connection_url["database"] == "dev"
        assert mysql.connection_url["username"] == "root"
        assert mysql.connection_url["password"] == "password"
        assert mysql.sync_driver == "mysql+pymysql"
        assert mysql.async_driver == "mysql+aiomysql"
        
    @patch('ddcDatabases.mysql.get_mysql_settings')
    def test_init_missing_credentials(self, mock_get_settings):
        """Test MySQL initialization with missing credentials"""
        mock_settings = MagicMock()
        mock_settings.user = None
        mock_settings.password = "password"
        mock_get_settings.return_value = mock_settings
        
        with pytest.raises(RuntimeError, match="Missing username/password"):
            MySQL()
            
    @patch('ddcDatabases.mysql.get_mysql_settings')
    def test_init_with_parameters(self, mock_get_settings):
        """Test MySQL initialization with override parameters"""
        mock_settings = MagicMock()
        mock_settings.user = "defaultuser"
        mock_settings.password = "defaultpass"
        mock_settings.host = "defaulthost"
        mock_settings.port = 3306
        mock_settings.database = "defaultdb"
        mock_settings.echo = False
        mock_settings.sync_driver = "mysql+pymysql"
        mock_settings.async_driver = "mysql+aiomysql"
        mock_get_settings.return_value = mock_settings
        
        mysql = MySQL(
            host="customhost",
            port=3307,
            user="customuser",
            password="custompass",
            database="customdb",
            echo=True
        )
        
        assert mysql.connection_url["host"] == "customhost"
        assert mysql.connection_url["port"] == 3307
        assert mysql.connection_url["database"] == "customdb"
        assert mysql.connection_url["username"] == "customuser"
        assert mysql.connection_url["password"] == "custompass"
        assert mysql.echo == True
        
    @patch('ddcDatabases.mysql.get_mysql_settings')
    def test_minimal_init(self, mock_get_settings):
        """Test MySQL minimal initialization"""
        mock_settings = MagicMock()
        mock_settings.user = "root"
        mock_settings.password = "password"
        mock_settings.host = "localhost"
        mock_settings.port = 3306
        mock_settings.database = "test"
        mock_settings.echo = False
        mock_settings.async_driver = "mysql+aiomysql"
        mock_settings.sync_driver = "mysql+pymysql"
        mock_get_settings.return_value = mock_settings
        
        mysql = MySQL()
        
        assert mysql.connection_url["host"] == "localhost"
        assert mysql.connection_url["port"] == 3306
        assert mysql.sync_driver == "mysql+pymysql"
        
    @patch('ddcDatabases.mysql.get_mysql_settings')
    def test_extra_engine_args(self, mock_get_settings):
        """Test MySQL with extra engine arguments"""
        mock_settings = MagicMock()
        mock_settings.user = "root"
        mock_settings.password = "password"
        mock_settings.host = "localhost"
        mock_settings.port = 3306
        mock_settings.database = "dev"
        mock_settings.echo = False
        mock_settings.sync_driver = "mysql+pymysql"
        mock_settings.async_driver = "mysql+aiomysql"
        mock_get_settings.return_value = mock_settings
        
        extra_args = {"charset": "utf8mb4", "connect_timeout": 30}
        mysql = MySQL(extra_engine_args=extra_args)
        
        # Test that extra args are included in engine_args
        assert "charset" in mysql.engine_args
        assert mysql.engine_args["charset"] == "utf8mb4"
        assert mysql.engine_args["connect_timeout"] == 30
        
        # Test that default args are still present
        assert mysql.engine_args["echo"] == False
        
    @patch('ddcDatabases.mysql.get_mysql_settings')
    def test_autoflush_and_expire_on_commit(self, mock_get_settings):
        """Test MySQL autoflush and expire_on_commit parameters"""
        mock_settings = MagicMock()
        mock_settings.user = "root"
        mock_settings.password = "password"
        mock_settings.host = "localhost"
        mock_settings.port = 3306
        mock_settings.database = "dev"
        mock_settings.echo = False
        mock_settings.sync_driver = "mysql+pymysql"
        mock_settings.async_driver = "mysql+aiomysql"
        mock_get_settings.return_value = mock_settings
        
        mysql = MySQL(autoflush=False, expire_on_commit=False)
        
        assert mysql.autoflush == False
        assert mysql.expire_on_commit == False
        
    @patch('ddcDatabases.mysql.get_mysql_settings')
    def test_connection_drivers(self, mock_get_settings):
        """Test MySQL connection drivers configuration"""
        mock_settings = MagicMock()
        mock_settings.user = "root"
        mock_settings.password = "password"
        mock_settings.host = "localhost"
        mock_settings.port = 3306
        mock_settings.database = "dev"
        mock_settings.echo = False
        mock_settings.sync_driver = "mysql+mysqlclient"
        mock_settings.async_driver = "mysql+asyncmy"
        mock_get_settings.return_value = mock_settings
        
        mysql = MySQL()
        
        assert mysql.sync_driver == "mysql+mysqlclient"
        assert mysql.async_driver == "mysql+asyncmy"