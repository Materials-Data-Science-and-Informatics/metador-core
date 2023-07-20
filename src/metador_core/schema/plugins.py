"""Schemas needed for the plugin system."""
from __future__ import annotations

import json
from collections import ChainMap
from functools import total_ordering
from typing import ClassVar, Dict, List, Literal, Optional

from pydantic import AnyHttpUrl, Extra, ValidationError, create_model

from ..util import is_public_name
from .core import BaseModelPlus, MetadataSchema
from .types import NonEmptyStr, SemVerTuple, to_semver_str


@total_ordering
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

    def __eq__(self, other):
        return (
            self.group == other.group
            and self.name == other.name
            and self.version == other.version
        )

    def __ge__(self, other):
        if self.group != other.group:
            return self.group >= other.group
        if self.name != other.name:
            return self.name >= other.name
        if self.version != other.version:
            return self.version >= other.version

    def __hash__(self):
        # needed because otherwise would differ in subclass,
        # causing problems for equality based on __hash__
        return hash((self.group, self.name, self.version))

    def __str__(self) -> str:
        return self.json()  # no indent (in contrast to base model)

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

    @classmethod
    def _subclass_for(cls, group: NonEmptyStr):
        """Create a subclass of PluginRef with group field pre-set."""
        return create_model(f"PG{group.capitalize()}.PluginRef", __base__=cls, group=(Literal[group], group))  # type: ignore


class PluginBase(BaseModelPlus):
    """All Plugin inner classes must be called `Plugin` and inherit from this class."""

    group: ClassVar[str] = ""  # auto-set during plugin group init

    # for type checking (mirrors Fields)
    name: str
    version: SemVerTuple
    requires: List[PluginRef] = []

    def ref(self, *, version: Optional[SemVerTuple] = None):
        from ..plugins import plugingroups

        assert self.group, "Must be called from a subclass that has group set!"
        return plugingroups[self.group].PluginRef(
            name=self.name, version=version or self.version
        )

    def plugin_string(self):
        return f"metador.{self.group}.{self.name}.{to_semver_str(self.version)}"

    def __str__(self) -> str:
        # pretty-print semver in user-facing representation
        dct = dict(
            group=self.group, name=self.name, version=to_semver_str(self.version)
        )
        dct.update(
            self.json_dict(exclude_defaults=True, exclude={"name", "group", "version"})
        )
        return json.dumps(dct, indent=2)

    @classmethod
    def parse_info(cls, info, *, ep_name: str = ""):
        if isinstance(info, cls):
            return info  # nothing to do, already converted info class to PluginBase (sub)model

        expected_ep_name = f"{info.name}__{to_semver_str(info.version)}"
        ep_name = ep_name or expected_ep_name
        if ep_name != expected_ep_name:
            msg = f"{ep_name}: Based on plugin info, entrypoint must be called '{expected_ep_name}'!"
            raise ValueError(msg)
        try:
            fields = ChainMap(
                *(c.__dict__ for c in info.__mro__)
            )  # this will treat inheritance well
            # validate
            public_attrs = {k: v for k, v in fields.items() if is_public_name(k)}
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
        from ..plugin.types import EPName, from_ep_name

        dm: DistMeta = distmeta_for(distribution(package_name))

        plugins: PkgPlugins = {}
        for group, ep_names in dm.plugins.items():
            plugins[group] = []
            for ep_name in ep_names:
                name, version = from_ep_name(EPName(ep_name))
                ref = PluginRef(group=group, name=name, version=version)
                plugins[group].append(ref)

        return cls(
            name=dm.name,
            version=dm.version,
            repository_url=dm.repository_url,
            plugins=plugins,
        )
