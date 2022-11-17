"""Schemas needed for the plugin system."""
from __future__ import annotations

import json
from collections import ChainMap
from typing import (
    Any,
    ClassVar,
    Dict,
    List,
    Literal,
    Optional,
    Protocol,
    Tuple,
    runtime_checkable,
)

from pydantic import AnyHttpUrl, Extra, ValidationError, create_model

from .core import BaseModelPlus, MetadataSchema
from .types import NonEmptyStr, SemVerTuple, semver_str


@runtime_checkable
class PluginLike(Protocol):
    """A Plugin has a Plugin inner class with plugin infos."""

    Plugin: ClassVar[Any]  # actually its PluginBase, but this happens at runtime


class PluginRef(MetadataSchema):
    """Reference to a metador plugin.

    This class can be subclassed for specific "marker schemas".

    It is not registered as a schema plugin, because it is too general on its own.
    """

    class Config:
        extra = Extra.forbid
        allow_mutation = False

    group: NonEmptyStr
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

    def __str__(self) -> str:
        return self.json()  # no indent

    @classmethod
    def subclass_for(cls, group: str):
        # lit = Literal[group_name] # type: ignore
        # class GroupPluginRef(cls):
        #     group: lit = group_name # type: ignore
        # return GroupPluginRef
        return create_model(f"PG{group.capitalize()}.PluginRef", __base__=cls, group=(Literal[group], group))  # type: ignore


def plugin_args(
    plugin="",  # actually: Union[str, PluginRef, PluginLike]
    version: Optional[SemVerTuple] = None,
    *,
    require_version: bool = False,
    # group: Optional[str]
) -> Tuple[str, Optional[SemVerTuple]]:
    """Return requested plugin name and version based on passed arguments.

    Helper for function argument parsing.
    """
    name: str
    vers: Optional[SemVerTuple] = version
    if isinstance(plugin, str):
        name = plugin
    elif isinstance(plugin, PluginRef):
        name = plugin.name
        if not vers:
            vers = plugin.version
    elif isinstance(plugin, PluginLike):
        name = plugin.Plugin.name
        if not vers:
            vers = plugin.Plugin.version
    if require_version and vers is None:
        raise ValueError(f"No version of {name} specified, but is required!")
    return (name, vers)


class PluginBase(BaseModelPlus):
    """All Plugin inner classes must be called `Plugin` and inherit from this class."""

    group: ClassVar[str] = ""  # auto-set during plugin group init

    # for type checking (mirrors Fields)
    name: str
    version: SemVerTuple
    requires: List[PluginRef] = []

    def ref(self, *, version: Optional[SemVerTuple] = None):
        from ..plugins import plugingroups

        return plugingroups[self.group].PluginRef(
            name=self.name, version=version or self.version
        )

    def plugin_string(self):
        return f"metador.{self.group}.{self.name}.{semver_str(self.version)}"

    def __str__(self) -> str:
        # pretty-print semver in user-facing representation
        dct = dict(group=self.group, name=self.name, version=semver_str(self.version))
        dct.update(
            self.json_dict(exclude_defaults=True, exclude={"name", "group", "version"})
        )
        return json.dumps(dct, indent=2)

    @classmethod
    def parse_info(cls, info, *, ep_name: str = ""):
        if isinstance(info, cls):
            return info  # nothing to do, already converted info class to PluginBase (sub)model

        expected_ep_name = f"{info.name}__{semver_str(info.version)}"
        ep_name = ep_name or expected_ep_name
        if ep_name != expected_ep_name:
            msg = f"{ep_name}: Based on plugin info, entrypoint must be called '{expected_ep_name}'!"
            raise TypeError(msg)
        try:
            fields = ChainMap(
                *(c.__dict__ for c in info.__mro__)
            )  # this will treat inheritance well
            # validate
            public_attrs = {k: v for k, v in fields.items() if k[0] != "_"}
            return cls(**public_attrs)
        except ValidationError as e:
            raise TypeError(f"{ep_name}: {ep_name}.Plugin validation error: \n{str(e)}")


PkgPlugins = Dict[str, List[PluginRef]]
"""Dict from plugin group name to plugins provided by a package."""


class PluginPkgMeta(MetadataSchema):
    """Metadata of a Python package containing Metador plugins."""

    name: NonEmptyStr
    """Name of the Python package."""

    version: SemVerTuple
    """Version of the Python package."""

    repository_url: Optional[AnyHttpUrl] = None
    """Python package source location (pip-installable / git-clonable)."""

    plugins: PkgPlugins = {}

    @classmethod
    def for_package(cls, package_name: str) -> PluginPkgMeta:
        """Extract metadata about a Metador plugin providing Python package."""
        # avoid circular import by importing here
        from importlib_metadata import distribution

        from ..plugin.entrypoints import DistMeta, distmeta_for
        from ..plugin.interface import _from_ep_name

        dm: DistMeta = distmeta_for(distribution(package_name))

        plugins: PkgPlugins = {}
        for group, ep_names in dm.plugins.items():
            plugins[group] = []
            for ep_name in ep_names:
                name, version = _from_ep_name(ep_name)
                ref = PluginRef(group=group, name=name, version=version)
                plugins[group].append(ref)

        return cls(
            name=dm.name,
            version=dm.version,
            repository_url=dm.repository_url,
            plugins=plugins,
        )
