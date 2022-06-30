"""Core Metadata schemas for Metador that are essential for the container API."""

from __future__ import annotations

from typing import Dict, List, Optional

from pydantic import AnyHttpUrl

from .interface import MetadataSchema
from .types import nonempty_str, SemVerTuple


class PluginRef(MetadataSchema):
    """(Partial) reference to a metador plugin.

    This class can be subclassed for specific "marker schemas".

    It is not registered as a schema plugin, because it is too general on its own.
    """

    pkg: Optional[nonempty_str]
    """Name of the Python package containing the plugin."""

    pkg_version: Optional[SemVerTuple]
    """Version of the Python package."""

    group: Optional[nonempty_str]
    """Metador pluggable group name, i.e. name of the entry point group."""

    name: nonempty_str
    """Registered entry point name inside an entry point group."""


class FullPluginRef(PluginRef):
    """Fully qualified plugin reference."""

    # make entries non-optional
    pkg: nonempty_str
    pkg_version: SemVerTuple
    group: nonempty_str


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
        from ..pluggable.bootstrap import _pgb_package_meta
        from ..pluggable.utils import pkgmeta_from_dist
        from importlib_metadata import distribution

        ret = _pgb_package_meta.get(package_name)  # look up in registered
        if ret is None:  # won't be there if its not registering plugins (yet)...
            ret = pkgmeta_from_dist(distribution(package_name))
        return ret


class EnvMeta(MetadataSchema):
    """Metadata about the Metador environment at a point in time.

    The environment during container creation is embedded into the container
    in order to be able to recover missing dependencies, given an unknown container.

    Only absolutely generic info should go here. Could be extended to contain more
    infos, like Python version, OS, hardware, etc., if desired.
    """

    packages: Dict[str, PluginPkgMeta]
    """Metadata of all Python packages that register metador plugins."""
