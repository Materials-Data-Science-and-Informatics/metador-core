"""Core Metadata schemas for Metador that are essential for the container API."""

from __future__ import annotations

from collections import ChainMap
from functools import partial
from typing import Any, ClassVar, Dict, Optional, Set, Type, cast

import wrapt
from pydantic import Extra, root_validator

from ..plugin.metaclass import PluginMetaclassMixin, UndefVersion
from ..util import cache, is_public_name
from ..util.models import field_atomic_types, traverse_typehint
from ..util.typing import (
    get_annotations,
    get_type_hints,
    is_classvar,
    is_instance_of,
    is_subclass_of,
    is_subtype,
)
from .base import BaseModelPlus
from .encoder import DynEncoderModelMetaclass
from .inspect import (
    FieldInspector,
    LiftedRODict,
    WrappedLiftedDict,
    lift_dict,
    make_field_inspector,
)
from .jsonschema import finalize_schema_extra, schema_of
from .partial import PartialFactory, is_mergeable_type
from .types import to_semver_str


def add_missing_field_descriptions(schema, model):
    """Add missing field descriptions from own Fields info, if possible."""
    for fname, fjsdef in schema.get("properties", {}).items():
        if not fjsdef.get("description"):
            try:
                if desc := model.Fields[fname].description:
                    fjsdef["description"] = desc
            except KeyError:
                pass  # no field info for that field


KEY_SCHEMA_PG = "$metador_plugin"
"""Key in JSON schema to put metador plugin name an version."""

KEY_SCHEMA_CONSTFLDS = "$metador_constants"
"""Key in JSON schema to put metador 'constant fields'."""


class SchemaBase(BaseModelPlus):
    __constants__: ClassVar[Dict[str, Any]]
    """Constant model fields, usually added with a decorator, ignored on input."""

    __overrides__: ClassVar[Set[str]]
    """Field names explicitly overriding inherited field type.

    Those not listed (by @overrides decorator) must, if they are overridden,
    be strict subtypes of the inherited type."""

    __types_checked__: ClassVar[bool]
    """Helper flag used by check_overrides to avoid re-checking."""

    class Config:
        @staticmethod
        def schema_extra(schema: Dict[str, Any], model: Type[BaseModelPlus]) -> None:
            model = UndefVersion._unwrap(model) or model

            # custom extra key to connect back to metador schema:
            if pgi := model.__dict__.get("Plugin"):
                schema[KEY_SCHEMA_PG] = pgi.ref().copy(exclude={"group"}).json_dict()

            # enrich schema with descriptions retrieved from e.g. docstrings
            if model is not MetadataSchema:
                add_missing_field_descriptions(schema, model)

            # special handling for "constant fields"
            if model.__constants__:
                schema[KEY_SCHEMA_CONSTFLDS] = {}
                for cname, cval in model.__constants__.items():
                    # list them (so they are not rejected even with additionalProperties=False)
                    schema["properties"][cname] = True
                    # store the constant alongside the schema
                    schema[KEY_SCHEMA_CONSTFLDS][cname] = cval

            # do magic
            finalize_schema_extra(schema, model, base_model=MetadataSchema)

    @classmethod
    def schema(cls, *args, **kwargs):
        """Return customized JSONSchema for this model."""
        return schema_of(UndefVersion._unwrap(cls) or cls, *args, **kwargs)

    @root_validator(pre=True)
    def override_consts(cls, values):
        """Override/add defined schema constants.

        They must be present on dump, but are ignored on load.
        """
        values.update(cls.__constants__)
        return values


ALLOWED_SCHEMA_CONFIG_FIELDS = {"title", "extra", "allow_mutation"}
"""Allowed pydantic Config fields to be overridden in schema models."""


class SchemaMagic(DynEncoderModelMetaclass):
    """Metaclass for doing some magic."""

    def __new__(cls, name, bases, dct):
        # enforce single inheritance
        if len(bases) > 1:
            raise TypeError("A schema can only have one parent schema!")
        baseschema = bases[0]

        # only allow inheriting from other schemas:
        # NOTE: can't normally happen (this metaclass won't be triggered)
        # if not issubclass(baseschema, SchemaBase):
        #     raise TypeError(f"Base class {baseschema} is not a MetadataSchema!")

        # prevent user from defining special schema fields by hand
        for atr in SchemaBase.__annotations__.keys():
            if atr in dct:
                raise TypeError(f"{name}: Invalid attribute '{atr}'")

        # prevent most changes to pydantic config
        if conf := dct.get("Config"):
            for conffield in conf.__dict__:
                if (
                    is_public_name(conffield)
                    and conffield not in ALLOWED_SCHEMA_CONFIG_FIELDS
                ):
                    raise TypeError(f"{name}: {conffield} must not be set or changed!")

        # generate pydantic model of schema (further checks are easier that way)
        # can't do these checks in __init__, because in __init__ the bases could be mangled
        ret = super().__new__(cls, name, bases, dct)

        # prevent user defining fields that are constants in a parent
        if base_consts := set(getattr(baseschema, "__constants__", {}).keys()):
            new_defs = set(get_annotations(ret).keys())
            if illegal := new_defs.intersection(base_consts):
                msg = (
                    f"{name}: Cannot define {illegal}, defined as const field already!"
                )
                raise TypeError(msg)

        # prevent parent-compat breaking change of extra handling / new fields:
        parent_forbids_extras = baseschema.__config__.extra is Extra.forbid
        if parent_forbids_extras:
            # if parent forbids, child does not -> problem (child can parse, parent can't)
            extra = ret.__config__.extra
            if extra is not Extra.forbid:
                msg = (
                    f"{name}: cannot {extra.value} extra fields if parent forbids them!"
                )
                raise TypeError(msg)

            # parent forbids extras, child has new fields -> same problem
            if new_flds := set(ret.__fields__.keys()) - set(
                baseschema.__fields__.keys()
            ):
                msg = f"{name}: Cannot define new fields {new_flds} if parent forbids extra fields!"
                raise TypeError(msg)

        # everything looks ok
        return ret

    def __init__(self, name, bases, dct):
        self.__types_checked__ = False  # marker used by check_types (for performance)

        # prevent implicit inheritance of class-specific internal/meta stuff:
        # should be taken care of by plugin metaclass
        assert self.Plugin is None or self.Plugin != bases[0].Plugin

        # also prevent inheriting override marker
        self.__overrides__ = set()

        # and manually prevent inheriting annotations (for Python < 3.10)
        if "__annotations__" not in self.__dict__:
            self.__annotations__ = {}

        # "constant fields" are inherited, but copied - not shared
        self.__constants__ = {}
        for b in bases:
            self.__constants__.update(getattr(b, "__constants__", {}))

    @property  # type: ignore
    @cache
    def _typehints(self):
        """Return typehints of this class."""
        return get_type_hints(self)

    @property  # type: ignore
    @cache
    def _base_typehints(self):
        """Return typehints accumulated from base class chain."""
        return ChainMap(
            *(b._typehints for b in self.__bases__ if issubclass(b, SchemaBase))
        )

    # ---- for public use ----

    def __str__(self):
        """Show schema and field documentation."""
        unwrapped = UndefVersion._unwrap(self)
        schema = unwrapped or self
        defstr = f"Schema {super().__str__()}"
        defstr = f"{defstr}\n{'='*len(defstr)}"
        descstr = ""
        if schema.__doc__ is not None and schema.__doc__.strip():
            desc = schema.__doc__
            descstr = f"\nDescription:\n------------\n\t{desc}"
        fieldsstr = f"Fields:\n-------\n\n{str(self.Fields)}"
        return "\n".join([defstr, descstr, fieldsstr])

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
        return PartialSchemas.get_partial(self)


class SchemaMetaclass(PluginMetaclassMixin, SchemaMagic):
    """Combine schema magic with general plugin magic."""


class MetadataSchema(SchemaBase, metaclass=SchemaMetaclass):
    """Extends Pydantic models with custom serializers and functions."""

    Plugin: ClassVar[Optional[Type]]  # user-defined inner class (for schema plugins)


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

    def _get_description(self):
        fld = self.origin._typehints[self.name]
        if any(map(lambda x: fld is x, (None, type(None), bool, int, float, str))):
            return None
        return super()._get_description()

    def __init__(self, model: Type[MetadataSchema], name: str, hint: str):
        super().__init__(model, name, hint)

        # to show plugin name and version in case of registered plugin schemas:
        og = self.origin
        self._origin_name = f"{og.__module__}.{og.__qualname__}"
        if pgi := og.Plugin:
            self._origin_name += f" (plugin: {pgi.name} {to_semver_str(pgi.version)})"

        # access to sub-entities/schemas:
        subschemas = list(field_atomic_types(og.__fields__[name], bound=MetadataSchema))
        self.schemas = lift_dict("Schemas", {s.__name__: s for s in set(subschemas)})

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
# wrapper to "infect" nested schemas with UndefVersion


class UndefVersionFieldInspector(wrapt.ObjectProxy):
    @property
    def schemas(self):
        return WrappedLiftedDict(self.__wrapped__.schemas, UndefVersion._mark_class)

    def __repr__(self):
        return repr(self.__wrapped__)


# ----


class PartialSchemas(PartialFactory):
    """Partial model for MetadataSchema model.

    Needed for harvesters to work (which can provide validated but partial metadata).
    """

    base_model = SchemaBase

    # override to ignore "constant fields" for partials
    @classmethod
    def _get_field_vals(cls, obj):
        return (
            (k, v)
            for k, v in super()._get_field_vals(obj)
            if k not in obj.__constants__
        )

    # override to add some fixes to partials
    @classmethod
    def _create_partial(cls, mcls, *, typehints=...):
        th = getattr(mcls, "_typehints", None)
        ret, nested = super()._create_partial(mcls, typehints=th)
        # attach constant field list for field filtering
        setattr(ret, "__constants__", getattr(mcls, "__constants__", set()))
        # copy custom parser to partial
        # (otherwise partial can't parse correctly with parser mixin)
        if parser := getattr(mcls, "Parser", None):
            setattr(ret, "Parser", parser)
        return (ret, nested)


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
        if not is_public_name(field):
            continue  # private field
        if not is_mergeable_type(hint):
            msg = f"{schema}:\n\ttype of '{field}' contains a forbidden pattern!"
            raise TypeError(msg)

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


def infer_parent(plugin: Type[MetadataSchema]) -> Optional[Type[MetadataSchema]]:
    """Return closest base schema that is a plugin, or None.

    This allows to skip over intermediate schemas and bases that are not plugins.
    """
    return next(
        filter(
            lambda c: issubclass(c, MetadataSchema) and c.__dict__.get("Plugin"),
            plugin.__mro__[1:],
        ),
        None,
    )


def is_pub_instance_field(schema, name, hint):
    """Return whether field `name` in `schema` is a non-constant, public schema instance field."""
    return (
        is_public_name(name)
        and not is_classvar(hint)
        and name not in schema.__constants__
    )


def detect_field_overrides(schema: Type[MetadataSchema]):
    anns = get_annotations(schema)
    base_hints = cast(Any, schema._base_typehints)
    new_hints = {n for n, h in anns.items() if is_pub_instance_field(schema, n, h)}
    return set(base_hints.keys()).intersection(new_hints)


def check_overrides(schema: Type[MetadataSchema]):
    """Check that fields are overridden to subtypes or explicitly declared as overridden."""
    hints = cast(Any, schema._typehints)
    base_hints = cast(Any, schema._base_typehints)

    actual_overrides = detect_field_overrides(schema)
    undecl_override = actual_overrides - schema.__overrides__
    if unreal_override := schema.__overrides__ - set(base_hints.keys()):
        msg = f"{schema.__name__}: No parent field to override: {unreal_override}"
        raise ValueError(msg)

    if miss_override := schema.__overrides__ - actual_overrides:
        msg = f"{schema.__name__}: Missing claimed field overrides: {miss_override}"
        raise ValueError(msg)

    # all undeclared overrides must be strict subtypes of the inherited type:
    for fname in undecl_override:
        hint, parent_hint = hints[fname], base_hints[fname]
        if not is_subtype(hint, parent_hint):
            parent = infer_parent(schema)
            parent_name = (
                parent.Fields[fname]._origin_name
                if parent
                else schema.__base__.__name__
            )
            msg = f"""The type assigned to field '{fname}'
in schema {repr(schema)}:

  {hint}

does not look like a valid subtype of the inherited type:

  {parent_hint}

from schema {parent_name}.

If you are ABSOLUTELY sure that this is a false alarm,
use the @overrides decorator to silence this error
and live forever with the burden of responsibility.
"""
            raise TypeError(msg)


# def detect_field_overrides(schema: Type[MetadataSchema]) -> Set[str]:
#     return {n
#         for n in updated_fields(schema)
#         if is_public_name(n) and not is_classvar(schema._typehints[n]) and n not in schema.__constants__
#     }
