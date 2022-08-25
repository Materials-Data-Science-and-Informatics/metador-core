"""Core Metadata schemas for Metador that are essential for the container API."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Dict, List, Literal, Optional, Set, Type

import isodate
from pydantic import AnyHttpUrl, BaseConfig, BaseModel, Extra, ValidationError
from pydantic_yaml import YamlModelMixin

from .types import NonEmptyStr, PintQuantity, PintUnit, SemVerTuple


def _mod_def_dump_args(kwargs):
    """Set `by_alias=True` in given kwargs dict, if not set explicitly."""
    if "by_alias" not in kwargs:
        kwargs["by_alias"] = True  # e.g. so we get correct @id, etc fields
    if "exclude_none" not in kwargs:
        kwargs["exclude_none"] = True  # we treat None as "missing" so leave it out
    return kwargs


class MetadataSchema(YamlModelMixin, BaseModel):
    """Extends Pydantic models with custom serializers and functions."""

    # important! breaks field hint inspection, if we add hints here without the guard!
    if TYPE_CHECKING:
        # user-defined:
        Plugin: SchemaPlugin
        # auto-generated:
        Schemas: Type  # used subschemas, for import-less access
        Partial: MetadataSchema  # partial schemas for harvesters

    # These are fine (ClassVar is ignored by pydantic):

    # fields overriding immediate parent (for testing)
    __overrides__: ClassVar[Set[str]]
    # fields with constant values added by add_annotations
    __constants__: ClassVar[Set[str]]

    # Pydantic configuration
    class Config(BaseConfig):
        underscore_attrs_are_private = True  # avoid using PrivateAttr all the time
        use_enum_values = True  # to serialize enums properly
        allow_population_by_field_name = (
            True  # when alias is set, still allow using field name
        )
        validate_assignment = True
        # https://pydantic-docs.helpmanual.io/usage/exporting_models/#json_encoders
        json_encoders = {
            PintUnit: lambda x: str(x),
            PintQuantity: lambda x: str(x),
            isodate.Duration: lambda x: isodate.duration_isoformat(x),
        }

    @classmethod
    def is_plugin(cls):
        """Return whether this schema is a registered plugin."""
        return hasattr(cls, "Plugin") and issubclass(cls.Plugin, SchemaPlugin)

    def dict(self, *args, **kwargs):
        return super().dict(*args, **_mod_def_dump_args(kwargs))

    def json(self, *args, **kwargs):
        return super().json(*args, **_mod_def_dump_args(kwargs))

    def yaml(self, *args, **kwargs):
        return super().yaml(*args, **_mod_def_dump_args(kwargs))

    def __bytes__(self) -> bytes:
        """Serialize to JSON and return UTF-8 encoded bytes to be written in a file."""
        # add a newline, as otherwise behaviour with text editors will be confusing
        # (e.g. vim automatically adds a trailing newline that it hides)
        # https://stackoverflow.com/questions/729692/why-should-text-files-end-with-a-newline
        return (self.json() + "\n").encode(encoding="utf-8")


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


class PluginBase:
    """All Plugin inner classes must be called `Plugin` and inherit from this class."""

    # set during plugin bootstrap to providing python package
    _provided_by: str = ""
    group: str  # fixed in subclasses for specific plugin groups

    # for type checking (mirrors Fields)
    name: str
    version: SemVerTuple
    requires: List[str] = []  # set default here (needed before actual checking is done)

    class Fields(BaseModel):
        """Minimal info to be declared by plugin author in a inner class called `Plugin`."""

        class Config:
            extra = Extra.forbid

        name: str
        """Name of plugin (must equal to listed entry point and unique per plugingroup)."""

        version: SemVerTuple
        """Semantic version of plugin."""

        requires: List[str] = []
        """List of plugin names of same kind that must be loaded before this one."""

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

    name: NonEmptyStr
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

        from ..plugin import plugingroups
        from ..plugin.utils import DistMeta, distmeta_for

        dm: DistMeta = distmeta_for(distribution(package_name))
        plugins: Plugins = {}
        for group, names in dm.plugins.items():
            plugins[group] = {}
            for name in names:
                plugins[group][name] = plugingroups[group][name].Plugin.ref()
        return cls(
            name=dm.name,
            version=dm.version,
            repository_url=dm.repository_url,
            plugins=plugins,
        )


# ---- subclasses for schema plugin group ----

SCHEMA_GROUP_NAME = "schema"


class SchemaPluginRef(PluginRef):
    group: Literal["schema"]


class SchemaPlugin(PluginBase):
    group = SCHEMA_GROUP_NAME
    parent_schema: Optional[SchemaPluginRef]

    class Fields(PluginBase.Fields):
        parent_schema: Optional[SchemaPluginRef]
        """Declares a parent schema plugin.

        By declaring a parent schema you agree to the following contract:
        Any data that can be loaded using this schema MUST also be
        loadable by the parent schema (with possible information loss).
        """
