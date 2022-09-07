"""Core Metadata schemas for Metador that are essential for the container API."""

from __future__ import annotations

from collections import ChainMap
from typing import Any, ClassVar, Dict, Set, Type

from overrides import overrides
from pydantic import BaseModel
from pydantic_yaml import YamlModelMixin

from .encoder import DynEncoderModelMeta
from .inspect import FieldInspector, LiftedRODict, get_field_inspector
from .parser import ParserMixin
from .partial import DeepPartialModel
from .utils import (
    field_model_types,
    get_type_hints,
    is_classvar,
    is_public_name,
    issubtype,
)


def _mod_def_dump_args(kwargs):
    """Set `by_alias=True` in given kwargs dict, if not set explicitly."""
    if "by_alias" not in kwargs:
        kwargs["by_alias"] = True  # e.g. so we get correct @id, etc fields
    if "exclude_none" not in kwargs:
        kwargs["exclude_none"] = True  # we treat None as "missing" so leave it out
    return kwargs


class BaseModelPlus(
    ParserMixin, YamlModelMixin, BaseModel, metaclass=DynEncoderModelMeta
):
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
    Partial: ClassVar[MetadataSchema]  # partial schemas for harvesters

    __inspect__: ClassVar[Type]
    """Field introspection interface."""

    __typehints__: ClassVar[Dict[str, Any]]
    """Type hints of the schema class."""

    __base_typehints__: ClassVar[Dict[str, Any]]
    """Combined type hints of the base classes."""

    __constants__: ClassVar[Set[str]]
    """Constant model fields, added with (derivatives of) the @const decorator."""

    __overrides__: ClassVar[Set[str]]
    """Field names explicitly overriding inherited field type.

    Those not listed (by @overrides decorator) must, if they are overridden,
    be strict subtypes of the inherited type."""

    __types_checked__: ClassVar[bool]
    """Helper flag used by check_overrides to avoid re-checking."""


class SchemaMeta(DynEncoderModelMeta):
    """Metaclass for doing some magic."""

    # NOTE: generating partial schemas here already is not good,
    # leads to problems with forward refs, so we do it afterwards
    # NOTE 2: using a deferred property-based approach like with Fields could be worth a try

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

        self.__typehints__ = {}
        self.__base_typehints__ = {}
        self.__overrides__ = set()
        self.__types_checked__ = False
        self.__constants__ = set.union(
            set(), *(getattr(b, "__constants__", set()) for b in bases)
        )

    # ---- for public use ----

    @property
    def is_plugin(self):
        """Return whether this schema is a installed schema plugin."""
        from ..plugins import schemas

        if info := self.__dict__.get("Plugin"):
            return self == schemas.get(info.name)
        return False

    @property
    def Fields(self):
        """Access the field introspection interface."""
        return get_field_inspector(  # class created on demand and cached
            self,
            "Fields",
            "__inspect__",
            bound=MetadataSchema,
            key_filter=lambda k: k in self.__fields__ and k not in self.__constants__,
            i_cls=SchemaFieldInspector,
        )


class MetadataSchema(SchemaBase, metaclass=SchemaMeta):
    """Extends Pydantic models with custom serializers and functions."""

    Plugin: ClassVar[Type]  # user-defined inner class (for schema plugins)


def _indent_text(txt, prefix="\t"):
    return "\n".join(map(lambda l: f"{prefix}{l}", txt.split("\n")))


class SchemaFieldInspector(FieldInspector):
    """MetadataSchema-specific field inspector.

    It adds a user-friendly repr and access to nested subschemas.
    """

    schemas: LiftedRODict
    _origin_name: str

    def __init__(self, model: Type[BaseModel], name: str, hint: str):
        super().__init__(model, name, hint)

        # to show plugin name and version in case of registered plugin schemas:
        og = self.origin
        self._origin_name = f"{og.__module__}.{og.__qualname__}"
        if og.is_plugin:
            self._origin_name += (
                f" (plugin: {og.Plugin.name} {'.'.join(map(str, og.Plugin.version))})"
            )

        # access to sub-entities/schemas:
        subschemas = set(field_model_types(og.__fields__[name], bound=MetadataSchema))
        self.schemas = LiftedRODict(
            "Schemas", (), dict(_dict={s.__name__: s for s in subschemas})
        )

    def __repr__(self) -> str:
        desc_str = ""
        if self.description:
            desc_str = f"description:\n{_indent_text(self.description)}\n"
        schemas_str = (
            f"schemas: {', '.join(self.schemas.keys())}\n" if self.schemas else ""
        )
        info = f"type: {str(self.type)}\norigin: {self._origin_name}\n{schemas_str}{desc_str}"
        return f"{self.name}\n{_indent_text(info)}"


class PartialSchema(DeepPartialModel, SchemaBase):
    """Partial model for MetadataSchema model.

    Needed for harvesters to work (which can provide validated but partial metadata).
    """

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


# --- delayed checks (triggered during schema loading) ---
# if we do it earlier, might lead to problems with forward refs and circularity


def check_types(schema: Type[MetadataSchema], *, recheck: bool = False):
    if schema is MetadataSchema or schema.__types_checked__ and not recheck:
        return

    # recursively check compositional and inheritance dependencies
    for b in schema.__bases__:
        if issubclass(b, MetadataSchema):
            check_types(b, recheck=recheck)
    for f in schema.Fields:
        for sname in schema.Fields[f].schemas:
            s = schema.Fields[f].schemas[sname]
            if s is not schema and issubclass(s, MetadataSchema):
                check_types(s, recheck=recheck)

    check_allowed_types(schema)
    check_overrides(schema)

    schema.__types_checked__ = True


def check_allowed_types(schema: Type[MetadataSchema]):
    """Check that shape of defined fields is suitable for deep merging."""
    hints = schema._typehints
    for field, hint in hints.items():
        if field[0] == "_":
            continue  # private field
        if not PartialSchema._is_mergeable_type(hint):
            raise TypeError(f"{schema}: '{field}' type contains a forbidden pattern!")


def is_pub_instance_field(schema, name, hint):
    """Return whether field `name` in `schema` is a non-constant, public schema instance field."""
    return (
        is_public_name(name)
        and not is_classvar(hint)
        and name not in schema.__constants__
    )


def check_overrides(schema: Type[MetadataSchema]):
    """Check that fields are overridden to subtypes or explicitly declared as overridden."""
    new_hints = {
        n for n, h in schema._typehints.items() if is_pub_instance_field(schema, n, h)
    }
    actual_overrides = set(schema._base_typehints.keys()).intersection(new_hints)
    miss_override = schema.__overrides__ - actual_overrides
    undecl_override = actual_overrides - schema.__overrides__
    if miss_override:
        raise TypeError(f"{schema}: Missing claimed field overrides: {miss_override}")

    # all undeclared overrides must be strict subtypes of the inherited type:
    for fname in undecl_override:
        hint, parent_hint = schema._typehints[fname], schema._base_typehints[fname]
        if not issubtype(hint, parent_hint):
            msg = f"""{schema}:
The assigned type for '{fname}'
    {hint}
does not look like a valid subtype of the inherited type
    {parent_hint}

If you are ABSOLUTELY sure that this is a false alarm,
use the @overrides decorator to silence this error
and live with the burden of responsibility.
"""
            raise TypeError(msg)
