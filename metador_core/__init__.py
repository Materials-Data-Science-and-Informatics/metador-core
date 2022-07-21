"""metador_core package."""
import importlib_metadata
from typing_extensions import Final

# import forces initialization of entrypoints, re-export the class managing them
# (it should be imported from the top level)
from .plugins import bootstrap  # noqa: F401

# Set version, will use version from pyproject.toml if defined
__version__: Final[str] = importlib_metadata.version(__package__ or __name__)
