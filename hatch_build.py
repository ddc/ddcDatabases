"""Custom build hook for creating Python version-specific wheels."""

import compileall
import sys
from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class CustomBuildHook(BuildHookInterface):
    """Build hook that sets wheel tags based on the current Python version."""

    PLUGIN_NAME = "custom"

    @staticmethod
    def initialize(version, build_data):
        """Set wheel tags to match the current Python interpreter."""
        major = sys.version_info.major
        minor = sys.version_info.minor
        python_tag = f"cp{major}{minor}"

        # Set the wheel tag: cp314-cp314-any (Python version specific, platform independent)
        build_data["tag"] = f"{python_tag}-{python_tag}-any"

        # Compile bytecode for the package
        compileall.compile_dir("ddcDatabases", force=True, quiet=1)
