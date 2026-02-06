"""Smoke test to verify the built package works correctly."""

from ddcDatabases import DBUtils, DBUtilsAsync, __version__

assert __version__, "Version should not be empty"
assert DBUtils, "DBUtils should be importable"
assert DBUtilsAsync, "DBUtilsAsync should be importable"

print(f"ddcDatabases {__version__} OK")
