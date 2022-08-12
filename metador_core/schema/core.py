"""Core Metadata schemas for Metador that are essential for the container API."""

from __future__ import annotations

from typing import Dict, Optional

from pydantic import AnyHttpUrl, BaseModel, Extra, Field, ValidationError
from typing_extensions import Annotated

from . import MetadataSchema
from .types import SemVerTuple, nonempty_str


class PluginRef(MetadataSchema):
    """Reference to a metador plugin.

    This class can be subclassed for specific "marker schemas".

    It is not registered as a schema plugin, because it is too general on its own.
    """

    class Config:
        frozen = True

    group: nonempty_str
    """Metador pluggable group name, i.e. name of the entry point group."""

    name: nonempty_str
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


class PluginBase:
    """All Plugin inner classes must be called `Plugin` and inherit from this class."""

    # set during plugin bootstrap to providing python package
    _provided_by: str = ""
    group: str  # fixed in subclasses for specific plugin groups

    # for type checking (mirrors Fields)
    name: str
    version: SemVerTuple

    class Fields(BaseModel):
        """Minimal info to be declared by plugin author in a inner class called `Plugin`."""

        class Config:
            extra = Extra.forbid

        name: Annotated[str, Field(alias="name")]
        """Name of plugin (must equal to listed entry point and unique per plugingroup)."""

        version: Annotated[SemVerTuple, Field(alias="version")]
        """Semantic version of plugin."""

    @classmethod
    def ref(cls, *, version=None):
        return PluginRef(group=cls.group, name=cls.name, version=version or cls.version)

    @classmethod
    def _check(cls, *, ep_name: str = ""):
        ep_name = ep_name or cls.name
        if ep_name != cls.name:
            msg = f"{ep_name}: Plugin.name ('{cls.name}') != entry point ('{ep_name}')"
            raise TypeError(msg)
        try:
            # validate and fill default values
            public_attrs = {
                k: v for k, v in cls.__dict__.items() if not k.startswith("_")
            }
            plugininfo = cls.Fields(**public_attrs)
            for atrname, val in plugininfo.dict().items():
                if not hasattr(cls, atrname):
                    setattr(cls, atrname, val)
        except ValidationError as e:
            raise TypeError(f"{ep_name}: {ep_name}.Plugin validation error: \n{str(e)}")


Plugins = Dict[str, Dict[str, PluginRef]]


class PluginPkgMeta(MetadataSchema):
    """Metadata of a Python package containing Metador plugins."""

    name: nonempty_str
    """Name of the Python package."""

    version: SemVerTuple
    """Version of the Python package."""

    repository_url: Optional[AnyHttpUrl] = None
    """Python package source location (pip-installable / git-clonable)."""

    plugins: Plugins = {}
    """Dict from metador plugin groups to list of entrypoint names (provided plugins)."""

    @classmethod
    def for_package(cls, package_name: str) -> PluginPkgMeta:
        """Extract metadata about a Metador plugin providing Python package."""
        # avoid circular import by importing here
        from importlib_metadata import distribution

        from ..plugins import installed
        from ..plugins.utils import DistMeta, distmeta_for

        dm: DistMeta = distmeta_for(distribution(package_name))
        plugins: Plugins = {}
        for group, names in dm.plugins.items():
            plugins[group] = {}
            for name in names:
                plugins[group][name] = installed[group][name].Plugin.ref()
        return cls(
            name=dm.name,
            version=dm.version,
            repository_url=dm.repository_url,
            plugins=plugins,
        )
