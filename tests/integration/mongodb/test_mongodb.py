import pytest

pytestmark = pytest.mark.integration


class TestMongoDBIntegration:
    """Integration tests for MongoDB using Testcontainers."""

    def test_connection_and_query(self, mongodb_container):
        """Test MongoDB connection and basic query operations."""
        from ddcDatabases import MongoDB

        port = mongodb_container.get_exposed_port(27017)
        host = f"{mongodb_container.get_container_host_ip()}:{port}"

        from ddcDatabases.mongodb import MongoDBQueryConfig

        mongo = MongoDB(
            host=host,
            user="test",
            password="test",
            database="admin",
            collection="test_collection",
            query_config=MongoDBQueryConfig(query={"name": "nonexistent"}),
        )

        with mongo:
            # Connection should be established
            assert mongo.is_connected is True

            # Insert documents
            db = mongo.client["admin"]
            collection = db["test_collection"]
            collection.insert_many(
                [
                    {"name": "Alice", "age": 30, "active": True},
                    {"name": "Bob", "age": 25, "active": True},
                    {"name": "Charlie", "age": 35, "active": False},
                ]
            )

            # Query using cursor
            cursor = mongo._create_cursor("test_collection", {"active": True})
            results = list(cursor)
            assert len(results) == 2

    def test_query_with_sort(self, mongodb_container):
        """Test MongoDB query with sort parameters."""
        from ddcDatabases import MongoDB

        port = mongodb_container.get_exposed_port(27017)
        host = f"{mongodb_container.get_container_host_ip()}:{port}"

        mongo = MongoDB(
            host=host,
            user="test",
            password="test",
            database="admin",
            collection="sort_test",
        )

        with mongo:
            # Insert documents
            db = mongo.client["admin"]
            collection = db["sort_test"]
            collection.delete_many({})  # Clean up
            collection.insert_many(
                [
                    {"name": "Zebra", "order": 3},
                    {"name": "Apple", "order": 1},
                    {"name": "Mango", "order": 2},
                ]
            )

            # Query with ascending sort
            cursor = mongo._create_cursor("sort_test", {}, "order", "asc")
            results = list(cursor)
            assert results[0]["name"] == "Apple"
            assert results[2]["name"] == "Zebra"

            # Query with descending sort
            cursor = mongo._create_cursor("sort_test", {}, "order", "desc")
            results = list(cursor)
            assert results[0]["name"] == "Zebra"
            assert results[2]["name"] == "Apple"

    def test_batch_size_and_limit(self, mongodb_container):
        """Test MongoDB batch_size and limit parameters."""
        from ddcDatabases import MongoDB

        port = mongodb_container.get_exposed_port(27017)
        host = f"{mongodb_container.get_container_host_ip()}:{port}"

        from ddcDatabases.mongodb import MongoDBQueryConfig

        mongo = MongoDB(
            host=host,
            user="test",
            password="test",
            database="admin",
            collection="limit_test",
            query_config=MongoDBQueryConfig(batch_size=10, limit=2),
        )

        with mongo:
            # Insert documents
            db = mongo.client["admin"]
            collection = db["limit_test"]
            collection.delete_many({})  # Clean up
            collection.insert_many([{"name": f"item_{i}"} for i in range(10)])

            # Query with limit
            cursor = mongo._create_cursor("limit_test", {})
            results = list(cursor)
            assert len(results) == 2  # Limited to 2

    def test_empty_query_returns_all(self, mongodb_container):
        """Test MongoDB with empty query returns all documents."""
        from ddcDatabases import MongoDB

        port = mongodb_container.get_exposed_port(27017)
        host = f"{mongodb_container.get_container_host_ip()}:{port}"

        mongo = MongoDB(
            host=host,
            user="test",
            password="test",
            database="admin",
            collection="all_docs_test",
        )

        with mongo:
            # Insert documents
            db = mongo.client["admin"]
            collection = db["all_docs_test"]
            collection.delete_many({})  # Clean up
            collection.insert_many(
                [
                    {"name": "doc1"},
                    {"name": "doc2"},
                    {"name": "doc3"},
                ]
            )

            # Empty query should return all
            cursor = mongo._create_cursor("all_docs_test", {})
            results = list(cursor)
            assert len(results) == 3

    def test_context_manager_cleanup(self, mongodb_container):
        """Test MongoDB context manager properly cleans up."""
        from ddcDatabases import MongoDB

        port = mongodb_container.get_exposed_port(27017)
        host = f"{mongodb_container.get_container_host_ip()}:{port}"

        mongo = MongoDB(
            host=host,
            user="test",
            password="test",
            database="admin",
            collection="cleanup_test",
        )

        with mongo:
            assert mongo.is_connected is True

        assert mongo.is_connected is False
