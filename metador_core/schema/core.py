"""Metadata models for Metador."""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple

from pydantic import AnyHttpUrl, NonNegativeInt

from .interface import MetadataSchema
from .types import nonempty_str


class PluginRef(MetadataSchema):
    """Reference to a metador plugin.

    This class is only there to be subclassed for specific "marker schemas".
    Therefore it is not registered as a schema plugin (which are assumed to make sense).

    An abstract PluginRef is not a useful schema to register at a container node.
    """

    python_pkg: Optional[nonempty_str]
    """Name of the Python package containing the plugin."""

    python_pkg_version: Optional[Tuple[int, int, int]]
    """Version of Python package."""

    plugin_group: Optional[nonempty_str]
    """Metador pluggable, i.e. entry point group."""

    plugin_name: nonempty_str
    """Registered entry point."""


class PluginPkgMeta(MetadataSchema):
    """Metadata of a Python package containing Metador plugins."""

    name: nonempty_str
    """Name of the Python package."""

    version: Tuple[NonNegativeInt, NonNegativeInt, NonNegativeInt]
    """Version of the Python package."""

    repository_url: Optional[AnyHttpUrl] = None
    """Python package source location (pip-installable / git-clonable)."""

    plugins: Dict[str, List[str]] = {}
    """Dict from metador plugin groups to list of entrypoint names (provided plugins)."""


class EnvMeta(MetadataSchema):
    """Metadata about the Metador environment at a point in time.

    The environment during container creation is embedded into the container
    in order to be able to recover missing dependencies, given an unknown container.

    Only absolutely generic info should go here. Could be extended to contain more
    infos, like Python version, OS, hardware, etc., if desired.
    """

    packages: Dict[str, PluginPkgMeta]
    """Metadata of all Python packages that register metador plugins."""
