"""Integration tests for MongoDB TLS configuration."""

import pytest

pytestmark = pytest.mark.integration


class TestMongoDBTLSIntegration:
    """Integration tests for MongoDB TLS configuration."""

    def test_connection_with_tls_disabled(self, mongodb_container):
        """Test MongoDB connection with TLS disabled."""
        from ddcdatabases import MongoDB
        from ddcdatabases.mongodb import MongoDBTLSConfig

        port = mongodb_container.get_exposed_port(27017)
        host = f"{mongodb_container.get_container_host_ip()}:{port}"

        mongo = MongoDB(
            host=host,
            user="test",
            password="test",
            database="admin",
            collection="tls_test",
            tls_config=MongoDBTLSConfig(tls_enabled=False),
        )

        with mongo:
            assert mongo.is_connected is True
            # Simple operation to verify connection
            db = mongo.client["admin"]
            collection = db["tls_test"]
            collection.delete_many({})
            collection.insert_one({"test": "value"})
            doc = collection.find_one({"test": "value"})
            assert doc["test"] == "value"
            collection.delete_many({})

    def test_tls_config_is_accessible(self, mongodb_container):
        """Test that MongoDB TLS config is accessible."""
        from ddcdatabases import MongoDB
        from ddcdatabases.mongodb import MongoDBTLSConfig

        port = mongodb_container.get_exposed_port(27017)
        host = f"{mongodb_container.get_container_host_ip()}:{port}"

        mongo = MongoDB(
            host=host,
            user="test",
            password="test",
            database="admin",
            collection="tls_config_test",
            tls_config=MongoDBTLSConfig(
                tls_enabled=False,
                tls_ca_cert_path="/path/to/ca.pem",
            ),
        )

        tls_info = mongo.get_tls_info()
        assert tls_info.tls_enabled is False
        assert tls_info.tls_ca_cert_path == "/path/to/ca.pem"

    def test_tls_config_immutable(self, mongodb_container):
        """Test that MongoDB TLS config is immutable."""
        from ddcdatabases import MongoDB
        from ddcdatabases.mongodb import MongoDBTLSConfig

        port = mongodb_container.get_exposed_port(27017)
        host = f"{mongodb_container.get_container_host_ip()}:{port}"

        mongo = MongoDB(
            host=host,
            user="test",
            password="test",
            database="admin",
            collection="tls_immutable_test",
            tls_config=MongoDBTLSConfig(tls_enabled=False),
        )

        tls_info = mongo.get_tls_info()
        with pytest.raises(AttributeError):
            tls_info.tls_enabled = True  # noqa
