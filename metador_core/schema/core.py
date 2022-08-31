"""Core Metadata schemas for Metador that are essential for the container API."""

from __future__ import annotations

from typing import ClassVar, Dict, List, Literal, Optional, Set, Type

import isodate
from pydantic import AnyHttpUrl, BaseModel, ValidationError, create_model
from pydantic.main import ModelMetaclass
from pydantic_yaml import YamlModelMixin

from .types import NonEmptyStr, PintQuantity, PintUnit, SemVerTuple

SCHEMA_GROUP_NAME = "schema"  # name of schema plugin group


def _mod_def_dump_args(kwargs):
    """Set `by_alias=True` in given kwargs dict, if not set explicitly."""
    if "by_alias" not in kwargs:
        kwargs["by_alias"] = True  # e.g. so we get correct @id, etc fields
    if "exclude_none" not in kwargs:
        kwargs["exclude_none"] = True  # we treat None as "missing" so leave it out
    return kwargs


class BaseModelPlus(YamlModelMixin, BaseModel):
    class Config:
        underscore_attrs_are_private = True  # make PrivateAttr not needed
        use_enum_values = True  # serialize enums properly

        # when alias is set, still allow using field name
        # (we use aliases for invalid attribute names in Python)
        allow_population_by_field_name = True
        # users should jump through hoops to add invalid stuff
        validate_assignment = True

        # https://pydantic-docs.helpmanual.io/usage/exporting_models/#json_encoders
        json_encoders = {
            PintUnit: lambda x: str(x),
            PintQuantity: lambda x: str(x),
            isodate.Duration: lambda x: isodate.duration_isoformat(x),
        }

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


class ModelMetaPlus(ModelMetaclass):
    """Metaclass for doing some magic."""

    # NOTE: generating partial schemas here already is not good
    # leads to problems with forward refs

    def __init__(self, name, bases, dct):
        ...


class MetadataSchema(BaseModelPlus, metaclass=ModelMetaPlus):
    """Extends Pydantic models with custom serializers and functions."""

    # user-defined (for schema plugins)
    Plugin: ClassVar[Type]

    # auto-generated:
    Schemas: ClassVar[Type]  # subschemas used in annotations, for import-less access
    Partial: ClassVar[MetadataSchema]  # partial schemas for harvesters

    # These are fine (ClassVar is ignored by pydantic):

    # fields overriding immediate parent (for testing)
    __overrides__: ClassVar[Set[str]]
    # fields with constant values added by add_annotations
    __constants__: ClassVar[Set[str]]

    @classmethod
    def is_plugin(cls):
        """Return whether this schema is a plugin."""
        return hasattr(cls, "Plugin")


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

    _provided_by: str = ""  # set during plugin bootstrap based on source package
    group: ClassVar[str] = ""  # auto-set during plugin group init

    # for type checking (mirrors Fields)
    name: str
    version: SemVerTuple
    requires: List[str] = []

    @classmethod
    def ref(cls, *, version=None):
        return PluginRef(group=cls.group, name=cls.name, version=version or cls.version)

    @classmethod
    def parse_info(cls, info, *, ep_name: str = ""):
        ep_name = ep_name or info.name
        if ep_name != info.name:
            msg = f"{ep_name}: Plugin.name ('{info.name}') != entry point ('{ep_name}')"
            raise TypeError(msg)
        try:
            # validate
            public_attrs = {k: v for k, v in info.__dict__.items() if k[0] != "_"}
            validated = cls(**public_attrs)
            validated._provided_by = getattr(info, "_provided_by", None)
            return validated
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
                plugins[group][name] = plugingroups[group].fullname(name)
        return cls(
            name=dm.name,
            version=dm.version,
            repository_url=dm.repository_url,
            plugins=plugins,
        )
