"""Core Metadata schemas for Metador that are essential for the container API."""

from __future__ import annotations

from collections import ChainMap
from typing import Any, ClassVar, Dict, Set, Type

import isodate
from overrides import overrides
from pydantic import BaseModel
from pydantic.main import ModelMetaclass
from pydantic_yaml import YamlModelMixin

from .partial import DeepPartialModel
from .types import PintQuantity, PintUnit
from .utils import get_type_hints


def _mod_def_dump_args(kwargs):
    """Set `by_alias=True` in given kwargs dict, if not set explicitly."""
    if "by_alias" not in kwargs:
        kwargs["by_alias"] = True  # e.g. so we get correct @id, etc fields
    if "exclude_none" not in kwargs:
        kwargs["exclude_none"] = True  # we treat None as "missing" so leave it out
    return kwargs


class BaseModelPlus(YamlModelMixin, BaseModel):
    """Extended pydantic BaseModel with some good defaults.

    Used as basis for various entities, including:
    * Metadata schemas
    * Harvester arguments
    """

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


class SchemaBase(BaseModelPlus):
    # auto-generated:
    Fields: ClassVar[Type]  # introspection of fields
    Partial: ClassVar[MetadataSchema]  # partial schemas for harvesters

    __typehints__: ClassVar[Dict[str, Any]]  # cached type hints of this class
    __base_typehints__: ClassVar[Dict[str, Any]]  # cached type hints of parents

    # decorator markers (checked on plugin load)

    __overrides__: ClassVar[Set[str]]  # fields overriding immediate parent
    __specialized__: ClassVar[Set[str]]  # fields overriding + claimed to be narrower
    __constants__: ClassVar[Set[str]]  # constant fields (added with `const` decorator)


class SchemaMeta(ModelMetaclass):
    """Metaclass for doing some magic."""

    # NOTE: generating partial schemas here already is not good,
    # leads to problems with forward refs, so we do it afterwards

    @property
    def is_plugin(self):
        """Return whether this schema is a installed schema plugin."""
        from ..plugins import schemas

        if info := self.__dict__.get("Plugin"):
            return self == schemas.get(info.name)
        return False

    @property
    def _typehints(self):
        if not self.__typehints__ and self.__dict__.get("__annotations__"):
            self.__typehints__ = get_type_hints(self)
        return self.__typehints__

    @property
    def _base_typehints(self):
        if not self.__base_typehints__:
            self.__base_typehints__ = ChainMap(
                *(b._typehints for b in self.__bases__ if issubclass(b, SchemaBase))
            )
        return self.__base_typehints__

    def __new__(cls, name, bases, dct):
        # only allow inheriting from other schemas
        # TODO: fix parser mixin first!
        # for b in bases:
        #    if not issubclass(b, SchemaBase):
        #        raise TypeError(f"Base class {b} is not a MetadataSchema!")

        # prevent user from defining special names by hand
        for atr in SchemaBase.__annotations__.keys():
            if atr in dct:
                raise TypeError(f"{name}: Invalid attribute '{atr}'")
        return super().__new__(cls, name, bases, dct)

    def __init__(self, name, bases, dct):
        # prevent implicit inheritance of class-specific stuff
        if "Plugin" not in dct:
            self.Plugin = None
        if "Fields" not in dct:
            self.Fields = None

        self.__typehints__ = {}
        self.__base_typehints__ = {}
        self.__overrides__ = set()
        self.__specialized__ = set()
        self.__constants__ = set.union(
            set(), *(getattr(b, "__constants__", set()) for b in bases)
        )


class MetadataSchema(SchemaBase, metaclass=SchemaMeta):
    """Extends Pydantic models with custom serializers and functions."""

    Plugin: ClassVar[Type]  # user-defined (for schema plugins)


class PartialSchema(DeepPartialModel, SchemaBase):
    """Partial model for MetadataSchema model."""

    # MetadataSchema-specific adaptations:
    @classmethod
    @overrides
    def _partial_name(cls, mcls):
        return f"{mcls.__qualname__}.Partial"

    @classmethod
    @overrides
    def _get_fields(cls, obj):
        # exclude the "annotated" fields that we support
        constants = obj.__dict__.get("__constants__", set())
        return ((k, v) for k, v in super()._get_fields(obj) if k not in constants)
