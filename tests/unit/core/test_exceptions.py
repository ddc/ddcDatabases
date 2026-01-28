import pytest
from ddcDatabases.core.exceptions import (
    CustomBaseException,
    DBDeleteAllDataException,
    DBExecuteException,
    DBFetchAllException,
    DBFetchValueException,
    DBInsertBulkException,
    DBInsertSingleException,
)


class TestExceptions:
    """Test custom exception classes"""

    def test_db_delete_all_data_exception(self):
        """Test DBDeleteAllDataException"""
        original_error = ValueError("Original error message")

        with pytest.raises(ValueError):
            DBDeleteAllDataException(original_error)

    def test_db_execute_exception(self):
        """Test DBExecuteException"""
        original_error = RuntimeError("Execution failed")

        with pytest.raises(RuntimeError):
            DBExecuteException(original_error)

    def test_db_fetch_all_exception(self):
        """Test DBFetchAllException"""
        original_error = ConnectionError("Connection lost")

        with pytest.raises(ConnectionError):
            DBFetchAllException(original_error)

    def test_db_fetch_value_exception(self):
        """Test DBFetchValueException"""
        original_error = TimeoutError("Query timeout")

        with pytest.raises(TimeoutError):
            DBFetchValueException(original_error)

    def test_db_insert_bulk_exception(self):
        """Test DBInsertBulkException"""
        original_error = ValueError("Invalid bulk data")

        with pytest.raises(ValueError):
            DBInsertBulkException(original_error)

    def test_db_insert_single_exception(self):
        """Test DBInsertSingleException"""
        original_error = KeyError("Missing required field")

        with pytest.raises(KeyError):
            DBInsertSingleException(original_error)

    def test_exception_inheritance(self):
        """Test that all custom exceptions inherit from CustomBaseException and Exception"""
        exceptions = [
            DBDeleteAllDataException,
            DBExecuteException,
            DBFetchAllException,
            DBFetchValueException,
            DBInsertBulkException,
            DBInsertSingleException,
        ]

        # Test base exception inherits from Exception
        assert issubclass(CustomBaseException, Exception)

        # Test all custom exceptions inherit from CustomBaseException
        for exc_class in exceptions:
            assert issubclass(exc_class, CustomBaseException)
            assert issubclass(exc_class, Exception)

    def test_exceptions_with_string_messages(self):
        """Test exceptions with string error messages"""
        message = "Test error message"

        # Since custom exceptions re-raise the original, we expect the string to be raised
        # Note: str is not an Exception subclass, so pytest will expect TypeError
        with pytest.raises(TypeError):
            DBDeleteAllDataException(message)

        with pytest.raises(TypeError):
            DBExecuteException(message)

        with pytest.raises(TypeError):
            DBFetchAllException(message)

        with pytest.raises(TypeError):
            DBFetchValueException(message)

        with pytest.raises(TypeError):
            DBInsertBulkException(message)

        with pytest.raises(TypeError):
            DBInsertSingleException(message)

    def test_exceptions_with_none(self):
        """Test exceptions with None as argument"""
        # Since custom exceptions re-raise the original, None will be raised
        with pytest.raises(TypeError):  # None is not an exception
            DBDeleteAllDataException(None)

    def test_exception_behavior(self):
        """Test the special behavior of custom exceptions"""
        # Test that the custom exception re-raises the original error
        original_error = RuntimeError("Test error")

        with pytest.raises(RuntimeError):
            DBFetchAllException(original_error)
