"""Integration tests for MongoDB persistent connections."""

import pytest

# noinspection PyProtectedMember
from ddcdatabases.core.persistent import (
    MongoDBPersistent,
    close_all_persistent_connections,
)

pytestmark = pytest.mark.integration


class TestMongoDBPersistentIntegration:
    """Integration tests for MongoDB persistent connections."""

    def setup_method(self):
        """Clear persistent connections before each test."""
        close_all_persistent_connections()

    def teardown_method(self):
        """Clean up after each test."""
        close_all_persistent_connections()

    def test_persistent_connection(self, mongodb_container):
        """Test MongoDB persistent connection."""
        port = mongodb_container.get_exposed_port(27017)
        host = mongodb_container.get_container_host_ip()

        conn = MongoDBPersistent(
            host=host,
            port=int(port),
            user="test",
            password="test",
            database="admin",
        )

        with conn as db:
            assert conn.is_connected

            # Insert a document
            collection = db["persistent_test"]
            collection.delete_many({})  # Clean up first
            collection.insert_one({"name": "test_doc", "value": 42})

            # Query
            doc = collection.find_one({"name": "test_doc"})
            assert doc["value"] == 42

            # Cleanup
            collection.delete_many({})

        conn.shutdown()

    def test_persistent_connection_singleton(self, mongodb_container):
        """Test MongoDB persistent connection singleton pattern."""
        port = mongodb_container.get_exposed_port(27017)
        host = mongodb_container.get_container_host_ip()

        conn1 = MongoDBPersistent(
            host=host,
            port=int(port),
            user="test",
            password="test",
            database="admin",
        )
        conn2 = MongoDBPersistent(
            host=host,
            port=int(port),
            user="test",
            password="test",
            database="admin",
        )

        assert conn1 is conn2

        conn1.shutdown()

    def test_persistent_connection_multiple_uses(self, mongodb_container):
        """Test MongoDB persistent connection can be used multiple times."""
        port = mongodb_container.get_exposed_port(27017)
        host = mongodb_container.get_container_host_ip()

        conn = MongoDBPersistent(
            host=host,
            port=int(port),
            user="test",
            password="test",
            database="admin",
        )

        # First use
        with conn as db:
            collection = db["multi_use_test"]
            collection.delete_many({})
            collection.insert_one({"iteration": 1})

        # Second use
        with conn as db:
            collection = db["multi_use_test"]
            collection.insert_one({"iteration": 2})

        # Third use - verify
        with conn as db:
            collection = db["multi_use_test"]
            count = collection.count_documents({})
            assert count == 2
            collection.delete_many({})

        conn.shutdown()
