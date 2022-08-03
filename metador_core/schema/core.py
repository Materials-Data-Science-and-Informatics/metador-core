"""Core Metadata schemas for Metador that are essential for the container API."""

from __future__ import annotations

from typing import Dict, List, Optional

from pydantic import AnyHttpUrl

from . import MetadataSchema
from .types import SemVerTuple, nonempty_str


class PluginRef(MetadataSchema):
    """(Partial) reference to a metador plugin.

    This class can be subclassed for specific "marker schemas".

    It is not registered as a schema plugin, because it is too general on its own.
    """

    pkg: nonempty_str
    """Name of the Python package containing the plugin."""
    # NOTE: must be part of full name, because package provides versioning
    # still, plugin names must be globally unique (NOTE: or we must load EPs differently)

    pkg_version: SemVerTuple
    """Version of the Python package."""

    group: nonempty_str
    """Metador pluggable group name, i.e. name of the entry point group."""

    name: nonempty_str
    """Registered entry point name inside an entry point group."""

    def supports(self, other: PluginRef) -> bool:
        """Return whether this plugin supports objects marked by given reference.

        True iff the package name, plugin group and plugin name agree,
        and the package of this reference has equal or larger minor version.
        """
        if self.pkg != other.pkg:
            return False
        if self.group != other.group:
            return False
        if self.name != other.name:
            return False
        if self.pkg_version[0] != other.pkg_version[0]:  # major
            return False
        if self.pkg_version[1] < other.pkg_version[1]:  # minor
            return False
        return True

    def __hash__(self):
        return hash((self.pkg, self.pkg_version, self.group, self.name))


class PluginPkgMeta(MetadataSchema):
    """Metadata of a Python package containing Metador plugins."""

    name: nonempty_str
    """Name of the Python package."""

    version: SemVerTuple
    """Version of the Python package."""

    repository_url: Optional[AnyHttpUrl] = None
    """Python package source location (pip-installable / git-clonable)."""

    plugins: Dict[str, List[str]] = {}
    """Dict from metador plugin groups to list of entrypoint names (provided plugins)."""

    @classmethod
    def for_package(cls, package_name: str) -> PluginPkgMeta:
        """Get metadata about a Metador plugin package."""
        # avoid circular import by importing here
        from importlib_metadata import distribution

        from ..plugins.bootstrap import _pgb_package_meta
        from ..plugins.utils import pkgmeta_from_dist

        ret = _pgb_package_meta.get(package_name)  # look up in registered
        if ret is None:  # won't be there if its not registering plugins (yet)...
            ret = pkgmeta_from_dist(distribution(package_name))
        return ret
