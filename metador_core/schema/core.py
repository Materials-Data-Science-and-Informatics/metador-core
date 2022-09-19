"""Core Metadata schemas for Metador that are essential for the container API."""

from __future__ import annotations

import json
from collections import ChainMap
from functools import partial
from typing import Any, ClassVar, Dict, Optional, Set, Type, cast

import wrapt
from overrides import overrides
from pydantic import BaseModel, root_validator
from pydantic_yaml import YamlModelMixin
from pydantic_yaml.mixin import YamlModelMixinConfig, YamlStyle

from ..plugin.metaclass import PluginMetaclassMixin, UndefVersion
from .encoder import DynEncoderModelMetaclass
from .inspect import FieldInspector, LiftedRODict, make_field_inspector
from .parser import ParserMixin
from .partial import DeepPartialModel
from .utils import (
    cache,
    field_model_types,
    get_type_hints,
    is_classvar,
    is_instance_of,
    is_public_name,
    is_subclass_of,
    issubtype,
    traverse_typehint,
)


def _mod_def_dump_args(kwargs):
    """Set `by_alias=True` in given kwargs dict, if not set explicitly."""
    if "by_alias" not in kwargs:
        kwargs["by_alias"] = True  # e.g. so we get correct @id, etc fields
    if "exclude_none" not in kwargs:
        kwargs["exclude_none"] = True  # we treat None as "missing" so leave it out
    return kwargs


class BaseModelPlus(
    ParserMixin, YamlModelMixin, BaseModel, metaclass=DynEncoderModelMetaclass
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
        """Return a dict.

        Nota that this will eliminate all pydantic models,
        but might still contain complex value types.
        """
        return super().dict(*args, **_mod_def_dump_args(kwargs))

    def json(self, *args, **kwargs) -> str:
        """Return serialized JSON as string."""
        return super().json(*args, **_mod_def_dump_args(kwargs))

    def json_dict(self, **kwargs):
        """Return a JSON-compatible dict.

        Uses round-trip through JSON serialization.
        """
        return json.loads(self.json(**kwargs))

    def yaml(
        self,
        *,
        # sort_keys: bool = False,
        default_flow_style: bool = False,
        default_style: Optional[YamlStyle] = None,
        indent: Optional[bool] = None,
        encoding: Optional[str] = None,
        **kwargs,
    ) -> str:
        """Return serialized YAML as string."""
        # Current way: use round trip through JSON to kick out non-JSON entities
        # (more elegant: allow ruamel yaml to reuse defined custom JSON dumpers)
        tmp = self.json_dict(**_mod_def_dump_args(kwargs))
        # NOTE: yaml_dumps is defined by the used yaml mixin in the config
        cfg = cast(YamlModelMixinConfig, self.__config__)
        return cfg.yaml_dumps(
            tmp,
            # sort_keys=sort_keys, # does not work for some weird arg passing reason
            default_flow_style=default_flow_style,
            default_style=default_style,
            encoding=encoding,
            indent=indent,
        )

    def __bytes__(self) -> bytes:
        """Serialize to JSON and return UTF-8 encoded bytes to be written in a file."""
        # add a newline, as otherwise behaviour with text editors will be confusing
        # (e.g. vim automatically adds a trailing newline that it hides)
        # https://stackoverflow.com/questions/729692/why-should-text-files-end-with-a-newline
        return (self.json() + "\n").encode(encoding="utf-8")


class SchemaBase(BaseModelPlus):
    __constants__: ClassVar[Dict[str, Any]]
    """Constant model fields, usually added with a decorator, ignored on input."""

    __overrides__: ClassVar[Set[str]]
    """Field names explicitly overriding inherited field type.

    Those not listed (by @overrides decorator) must, if they are overridden,
    be strict subtypes of the inherited type."""

    __types_checked__: ClassVar[bool]
    """Helper flag used by check_overrides to avoid re-checking."""

    @root_validator(pre=True)
    def override_consts(cls, values):
        """Override/add defined schema constants.

        They must be present on dump, but are ignored on load.
        """
        values.update(cls.__constants__)
        return values


class SchemaMagic(DynEncoderModelMetaclass):
    """Metaclass for doing some magic."""

    def __new__(cls, name, bases, dct):
        for b in bases:
            # only allow inheriting from other schemas:
            if not issubclass(b, SchemaBase):
                raise TypeError(f"Base class {b} is not a MetadataSchema!")

        if len(bases) > 1:
            raise TypeError("A schema can only have one parent schema!")

        # prevent user from defining special fields by hand
        for atr in SchemaBase.__annotations__.keys():
            if atr in dct:
                raise TypeError(f"{name}: Invalid attribute '{atr}'")

        return super().__new__(cls, name, bases, dct)

    def __init__(self, name, bases, dct):
        # prevent implicit inheritance of class-specific stuff
        if "Plugin" not in dct:
            self.Plugin = None

        self.__overrides__ = set()
        self.__constants__ = {}
        for b in bases:
            self.__constants__.update(getattr(b, "__constants__", {}))

        self.__types_checked__ = False

    @property  # type: ignore
    @cache
    def _typehints(self):
        return get_type_hints(self)

    @property  # type: ignore
    @cache
    def _base_typehints(self):
        return ChainMap(
            *(b._typehints for b in self.__bases__ if issubclass(b, SchemaBase))
        )

    # ---- for public use ----

    @property
    def Fields(self: Any) -> Any:
        """Access the field introspection interface."""
        fields = make_schema_inspector(self)
        # make sure that subschemas accessed in a schema without explicit version
        # are also marked so we can check if someone uses them illegally
        if issubclass(self, UndefVersion):
            return WrappedLiftedDict(fields, UndefVersionFieldInspector)
        else:
            return fields

    @property
    def Partial(self):
        """Access the partial schema based on the current schema."""
        return PartialSchema._get_partial(self)


class SchemaMetaclass(PluginMetaclassMixin, SchemaMagic):
    """Combine schema magic with general plugin magic."""


class MetadataSchema(SchemaBase, metaclass=SchemaMetaclass):
    """Extends Pydantic models with custom serializers and functions."""

    Plugin: ClassVar[Type]  # user-defined inner class (for schema plugins)


# ----


def _indent_text(txt, prefix="\t"):
    """Add indentation at new lines in given string."""
    return "\n".join(map(lambda l: f"{prefix}{l}", txt.split("\n")))


class SchemaFieldInspector(FieldInspector):
    """MetadataSchema-specific field inspector.

    It adds a user-friendly repr and access to nested subschemas.
    """

    schemas: LiftedRODict
    _origin_name: str

    def __init__(self, model: Type[MetadataSchema], name: str, hint: str):
        super().__init__(model, name, hint)

        # to show plugin name and version in case of registered plugin schemas:
        og = self.origin
        self._origin_name = f"{og.__module__}.{og.__qualname__}"
        if og.is_plugin:
            self._origin_name += (
                f" (plugin: {og.Plugin.name} {og.Plugin.version_string()})"
            )

        # access to sub-entities/schemas:
        subschemas = list(field_model_types(og.__fields__[name], bound=MetadataSchema))
        self.schemas = LiftedRODict(
            "Schemas", (), dict(_dict={s.__name__: s for s in set(subschemas)})
        )

    def __repr__(self) -> str:
        desc_str = ""
        if self.description:
            desc_str = f"description:\n{_indent_text(self.description)}\n"
        schemas_str = (
            f"schemas: {', '.join(iter(self.schemas))}\n" if self.schemas else ""
        )
        info = f"type: {str(self.type)}\norigin: {self._origin_name}\n{schemas_str}{desc_str}"
        return f"{self.name}\n{_indent_text(info)}"


def _is_schema_field(schema, key: str):
    """Return whether a given key name is a non-constant model field."""
    return key in schema.__fields__ and key not in schema.__constants__


@cache
def make_schema_inspector(schema):
    return make_field_inspector(
        schema,
        "Fields",
        bound=MetadataSchema,
        key_filter=partial(_is_schema_field, schema),
        i_cls=SchemaFieldInspector,
    )


# ----
# some wrappers needed to "infect" nested schemas with UndefVersion


class WrappedLiftedDict(wrapt.ObjectProxy):
    """Wrap values returned by a LiftedRODict."""

    def __init__(self, obj, wrapperfun):
        if not isinstance(obj, LiftedRODict):
            raise TypeError(f"{obj} is not a LiftedRODict!")
        super().__init__(obj)
        self._self_wrapperfun = wrapperfun

    def __getitem__(self, key):
        return self._self_wrapperfun(self.__wrapped__[key])

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError as e:
            raise AttributeError(str(e))

    def __repr__(self):
        return repr(self.__wrapped__)


class UndefVersionFieldInspector(wrapt.ObjectProxy):
    @property
    def schemas(self):
        return WrappedLiftedDict(self.__wrapped__.schemas, UndefVersion._mark_class)

    def __repr__(self):
        return repr(self.__wrapped__)


# ----


class PartialSchema(DeepPartialModel, SchemaBase):
    """Partial model for MetadataSchema model.

    Needed for harvesters to work (which can provide validated but partial metadata).
    """

    __constants__: ClassVar[Dict[str, Any]] = {}

    # MetadataSchema-specific adaptations:
    @classmethod
    @overrides
    def _partial_name(cls, mcls):
        return f"{mcls.__qualname__}.Partial"

    @classmethod
    @overrides
    def _get_fields(cls, obj):
        # exclude the "annotated" fields that we support
        constants = obj.__dict__.get("__constants__", {}).keys()
        return ((k, v) for k, v in super()._get_fields(obj) if k not in constants)

    @classmethod
    @overrides
    def _create_partial(cls, mcls, *, typehints=...):
        ret = super()._create_partial(mcls, typehints=mcls._typehints)
        # copy custom parser (these are supposed to also work with the partials)
        if parser := getattr(mcls, "Parser", None):
            setattr(ret, "Parser", parser)
        return ret


# --- delayed checks (triggered during schema loading) ---
# if we do it earlier, might lead to problems with forward refs and circularity


def check_types(schema: Type[MetadataSchema], *, recheck: bool = False):
    if schema is MetadataSchema or schema.__types_checked__ and not recheck:
        return

    # recursively check compositional and inheritance dependencies
    for b in schema.__bases__:
        if issubclass(b, MetadataSchema):
            check_types(b, recheck=recheck)

    schemaFields = cast(Any, schema.Fields)
    for f in schemaFields:  # type: ignore
        for sname in schemaFields[f].schemas:
            s = schemaFields[f].schemas[sname]
            if s is not schema and issubclass(s, MetadataSchema):
                check_types(s, recheck=recheck)

    check_allowed_types(schema)
    check_overrides(schema)

    schema.__types_checked__ = True


def check_allowed_types(schema: Type[MetadataSchema]):
    """Check that shape of defined fields is suitable for deep merging."""
    hints = cast(Any, schema._typehints)
    for field, hint in hints.items():
        if field[0] == "_":
            continue  # private field
        if not PartialSchema._is_mergeable_type(hint):
            raise TypeError(
                f"{schema}:\n\ttype of '{field}' contains a forbidden pattern!"
            )

        # check that no nested schemas from undefVersion plugins are used in field definitions
        # (the Plugin metaclass cannot check this, but it checks for inheritance)
        if illegal := next(
            filter(
                is_subclass_of(UndefVersion),
                filter(is_instance_of(type), traverse_typehint(hint)),
            ),
            None,
        ):
            msg = f"{schema}:\n\ttype of '{field}' contains an illegal subschema:\n\t\t{illegal}"
            raise TypeError(msg)


def is_pub_instance_field(schema, name, hint):
    """Return whether field `name` in `schema` is a non-constant, public schema instance field."""
    return (
        is_public_name(name)
        and not is_classvar(hint)
        and name not in schema.__constants__
    )


def infer_parent(plugin: Type[MetadataSchema]) -> Optional[Type[MetadataSchema]]:
    """Return closest base schema that is a plugin, or None.

    This allows to skip over intermediate schemas and bases that are not plugins.
    """
    return next(
        filter(
            lambda c: issubclass(c, MetadataSchema) and c.is_plugin, plugin.__mro__[1:]
        ),
        None,
    )


def check_overrides(schema: Type[MetadataSchema]):
    """Check that fields are overridden to subtypes or explicitly declared as overridden."""
    hints = cast(Any, schema._typehints)
    base_hints = cast(Any, schema._base_typehints)

    new_hints = {n for n, h in hints.items() if is_pub_instance_field(schema, n, h)}
    actual_overrides = set(base_hints.keys()).intersection(new_hints)
    miss_override = schema.__overrides__ - actual_overrides
    undecl_override = actual_overrides - schema.__overrides__
    if miss_override:
        raise TypeError(f"{schema}: Missing claimed field overrides: {miss_override}")

    # all undeclared overrides must be strict subtypes of the inherited type:
    for fname in undecl_override:
        hint, parent_hint = hints[fname], base_hints[fname]
        if not issubtype(hint, parent_hint):
            msg = f"""The type assigned to field '{fname}'
in schema {schema}:

  {hint}

does not look like a valid subtype of the inherited type:

  {parent_hint}

from schema {cast(Any, infer_parent(schema)).Fields[fname]._origin_name}.

If you are ABSOLUTELY sure that this is a false alarm,
use the @overrides decorator to silence this error
and live forever with the burden of responsibility.
"""
            raise TypeError(msg)
