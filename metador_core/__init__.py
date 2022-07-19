"""metador_core package."""
import importlib_metadata
from typing_extensions import Final

# import forces initialization of entrypoints, re-export the class managing them
# (it should be imported from the top level)
from .plugins.bootstrap import _pgb_package_meta
from .schema.core import EnvMeta

# Set version, will use version from pyproject.toml if defined
__version__: Final[str] = importlib_metadata.version(__package__ or __name__)


def metador_env():
    """Return information about current metador environment."""
    return EnvMeta(packages=_pgb_package_meta)
