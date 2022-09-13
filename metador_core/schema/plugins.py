"""Schemas needed for the plugin system."""
from __future__ import annotations

from typing import ClassVar, Dict, List, Literal, Optional, Protocol, Set

from pydantic import AnyHttpUrl, ValidationError, create_model

from .core import BaseModelPlus, MetadataSchema
from .types import NonEmptyStr, SemVerTuple


class PluginRef(MetadataSchema):
    """Reference to a metador plugin.

    This class can be subclassed for specific "marker schemas".

    It is not registered as a schema plugin, because it is too general on its own.
    """

    class Config:
        frozen = True

    group: str
    """Metador pluggable group name, i.e. name of the entry point group."""

    name: NonEmptyStr
    """Registered entry point name inside an entry point group."""

    version: SemVerTuple
    """Version of the Python package."""

    def supports(self, other: PluginRef) -> bool:
        """Return whether this plugin supports objects marked by given reference.

        True iff the package name, plugin group and plugin name agree,
        and the package of this reference has equal or larger minor version.
        """
        if self.group != other.group:
            return False
        if self.name != other.name:
            return False
        if self.version[0] != other.version[0]:  # major
            return False
        if self.version[1] < other.version[1]:  # minor
            return False
        return True

    def __hash__(self):
        # needed because otherwise would differ in subclass,
        # causing problems for equality based on __hash__
        return hash((self.group, self.name, self.version))

    @classmethod
    def subclass_for(cls, group: str):
        # lit = Literal[group_name] # type: ignore
        # class GroupPluginRef(cls):
        #     group: lit = group_name # type: ignore
        # return GroupPluginRef
        return create_model(f"PG{group.capitalize()}.PluginRef", __base__=cls, group=(Literal[group], group))  # type: ignore


class PluginBase(BaseModelPlus):
    """All Plugin inner classes must be called `Plugin` and inherit from this class."""

    group: ClassVar[str] = ""  # auto-set during plugin group init

    # for type checking (mirrors Fields)
    name: str
    version: SemVerTuple
    requires: List[str] = []

    def ref(self, *, version: SemVerTuple):
        from ..plugins import plugingroups

        return plugingroups[self.group].PluginRef(
            name=self.name, version=version or self.version
        )

    @classmethod
    def parse_info(cls, info, *, ep_name: str = ""):
        ep_name = ep_name or info.name
        if ep_name != info.name:
            msg = f"{ep_name}: Plugin.name ('{info.name}') != entry point ('{ep_name}')"
            raise TypeError(msg)
        try:
            # validate
            public_attrs = {k: v for k, v in info.__dict__.items() if k[0] != "_"}
            return cls(**public_attrs)
        except ValidationError as e:
            raise TypeError(f"{ep_name}: {ep_name}.Plugin validation error: \n{str(e)}")


class PluginLike(Protocol):
    Plugin: PluginBase


# NOTE: if we would like pluginrefs with versions, this would force loading all plugins
# and as this sucks we just don't do it and only list entry points
# Plugins = Dict[str, Dict[str, PluginRef]]
# """Dict from metador plugin groups to list of entrypoint names (provided plugins)."""

Plugins = Dict[str, Set[str]]
"""Dict from group name to entry point names."""


class PluginPkgMeta(MetadataSchema):
    """Metadata of a Python package containing Metador plugins."""

    name: NonEmptyStr
    """Name of the Python package."""

    version: SemVerTuple
    """Version of the Python package."""

    repository_url: Optional[AnyHttpUrl] = None
    """Python package source location (pip-installable / git-clonable)."""

    plugins: Plugins = {}

    @classmethod
    def for_package(cls, package_name: str) -> PluginPkgMeta:
        """Extract metadata about a Metador plugin providing Python package."""
        # avoid circular import by importing here
        from importlib_metadata import distribution

        from ..plugins.entrypoints import DistMeta, distmeta_for

        dm: DistMeta = distmeta_for(distribution(package_name))
        # from ..plugin import plugingroups

        plugins: Plugins = {}
        for group, names in dm.plugins.items():
            plugins[group] = set(names)
            # for name in names:
            #     plugins[group][name] = plugingroups[group].fullname(name)

        return cls(
            name=dm.name,
            version=dm.version,
            repository_url=dm.repository_url,
            plugins=plugins,
        )
