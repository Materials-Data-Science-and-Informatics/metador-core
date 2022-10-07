"""Core Metadata schemas for Metador that are essential for the container API."""

from __future__ import annotations

import json
from collections import ChainMap
from functools import partial
from typing import Any, ClassVar, Dict, Optional, Set, Type, cast

import wrapt
from overrides import overrides
from pydantic import BaseModel, Extra, root_validator
from pydantic_yaml import YamlModelMixin
from pydantic_yaml.mixin import YamlModelMixinConfig, YamlStyle

from ..plugin.metaclass import PluginMetaclassMixin, UndefVersion
from .encoder import DynEncoderModelMetaclass
from .inspect import FieldInspector, LiftedRODict, make_field_inspector
from .jsonschema import (
    KEY_SCHEMA_DEFS,
    KEY_SCHEMA_HASH,
    fixup_jsonschema,
    jsonschema_id,
    schema_of,
)
from .parser import ParserMixin
from .partial import DeepPartialModel
from .utils import (
    cache,
    field_model_types,
    get_annotations,
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
        # keep extra fields by default
        extra = Extra.allow
        # make PrivateAttr wrappers not always needed
        underscore_attrs_are_private = True
        # serialize enums properly
        use_enum_values = True
        # when alias is set, still allow using field name
        # (we use aliases for invalid attribute names in Python)
        allow_population_by_field_name = True
        # users should jump through hoops to add invalid stuff
        validate_assignment = True
        # defaults should also be validated
        validate_all = True
        # for JSON compat
        allow_inf_nan = False
        # pydantic anystr config: non-empty, non-whitespace
        # (but we prefer NonEmptyStr anyway for inheritance)
        anystr_strip_whitespace = True
        min_anystr_length = 1

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

    def __str__(self) -> str:
        return self.json(indent=2)


def add_missing_field_descriptions(schema, model):
    """Add missing field descriptions from own Fields info, if possible."""
    for fname, fjsdef in schema.get("properties", {}).items():
        if not fjsdef.get("description"):
            try:
                if desc := model.Fields[fname].description:
                    fjsdef["description"] = desc
            except KeyError:
                pass  # no field info for that field


def split_model_inheritance(schema, model):
    """Decompose a model into an allOf combination with a parent model.

    This is ugly because pydantic does in-place wrangling and caching,
    and we need to hack around it.
    """
    # NOTE: important - we assume to get the standard for of a $ref + $defs
    # so that the $defs contain the actual definition of the base_schema
    # and everything it needs.
    # simply calling schema() is different for recursive and non-recursive schemas,
    # schema_of is consistent in its output.
    base_schema = schema_of(model.__base__)

    # compute filtered properties / required section
    schema_new = dict(schema)
    ps = schema_new.pop("properties", None)
    rq = schema_new.pop("required", None)

    lst_fields = detect_field_overrides(model)
    lst_fields.update(
        set(model.__fields__.keys()) - set(model.__base__.__fields__.keys())
    )
    ps_new = {k: v for k, v in ps.items() if k in lst_fields}
    rq_new = None if not rq else [k for k in rq if k in ps_new]
    schema_this = {k: v for k, v in [("properties", ps_new), ("required", rq_new)] if v}

    # construct new schema as combination of base schema and remainder schema
    schema_new.update(
        {
            # "rdfs:subClassOf": f"/{base_id}",
            "allOf": [{"$ref": base_schema["$ref"]}, schema_this],
        }
    )

    # we need to add the definitions to/from the base schema as well
    if KEY_SCHEMA_DEFS not in schema_new:
        schema_new[KEY_SCHEMA_DEFS] = {}
    schema_new[KEY_SCHEMA_DEFS].update(base_schema.get(KEY_SCHEMA_DEFS, {}))

    schema.clear()
    schema.update(schema_new)


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
            # print("schema_extra", repr(model))
            model = UndefVersion._unwrap(model) or model

            # a schema should have a specified standard
            schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"

            # custom extra key to connect back to metador schema:
            if pgi := getattr(model, "Plugin", None):
                schema[KEY_SCHEMA_PG] = pgi.ref().copy(exclude={"group"}).json_dict()

            if model is not MetadataSchema:
                add_missing_field_descriptions(schema, model)

            if model.__constants__:
                schema[KEY_SCHEMA_CONSTFLDS] = {}
                for cname, cval in model.__constants__.items():
                    # list them (so they are not rejected even with additionalProperties=False)
                    schema["properties"][cname] = True
                    # store the constant alongside the schema
                    schema[KEY_SCHEMA_CONSTFLDS][cname] = cval

            if (
                model.__base__ is not MetadataSchema
            ):  # tricky part: de-duplicate from parent class
                split_model_inheritance(schema, model)

            # do this last, because it needs everything else to compute:
            schema[KEY_SCHEMA_HASH] = f"{jsonschema_id(schema)}"
            fixup_jsonschema(schema)

    @classmethod
    def schema(cls, *args, **kwargs):
        # print("schema", repr(cls))
        ret = dict(super().schema(*args, **kwargs))
        # from pprint import pprint
        # pprint(ret)
        fixup_jsonschema(ret)
        return ret

    @root_validator(pre=True)
    def override_consts(cls, values):
        """Override/add defined schema constants.

        They must be present on dump, but are ignored on load.
        """
        values.update(cls.__constants__)
        return values


ALLOWED_SCHEMA_CONFIG_FIELDS = {"title", "schema_extra", "extra", "allow_mutation"}
"""Allowed pydantic Config fields to be overridden in Schemas."""


class SchemaMagic(DynEncoderModelMetaclass):
    """Metaclass for doing some magic."""

    def __new__(cls, name, bases, dct):
        # enforce single inheritance
        if len(bases) > 1:
            raise TypeError("A schema can only have one parent schema!")
        baseschema = bases[0]

        # only allow inheriting from other schemas:
        if not issubclass(baseschema, SchemaBase):
            raise TypeError(f"Base class {baseschema} is not a MetadataSchema!")

        # prevent user from defining special schema fields by hand
        for atr in SchemaBase.__annotations__.keys():
            if atr in dct:
                raise TypeError(f"{name}: Invalid attribute '{atr}'")

        # prevent most changes to pydantic config
        if conf := dct.get("Config"):
            for conffield in conf.__dict__:
                if (
                    conffield[0] != "_"
                    and conffield not in ALLOWED_SCHEMA_CONFIG_FIELDS
                ):
                    raise TypeError(f"{name}: {conffield} must not be changed!")

        # generate pydantic model of schema (further checks are easier that way)
        # can't do these checks in __init__, because in __init__ the bases could be mangled
        ret = super().__new__(cls, name, bases, dct)

        # prevent parent-compat breaking change of extra handling / new fields:
        parent_forbids_extras = baseschema.__config__.extra is Extra.forbid
        if parent_forbids_extras:
            # if parent forbids, child does not -> problem (child can parse, parent can't)
            extra = ret.__config__.extra
            if extra is not Extra.forbid:
                raise TypeError(
                    f"{name}: cannot {extra.value} extra fields if parent forbids them!"
                )

            # parent forbids extras, child has new fields -> same problem
            new_fields = set(ret.__fields__.keys()) - set(baseschema.__fields__.keys())
            if new_fields:
                msg = f"{name}: Cannot define new fields {new_fields} if parent forbids extra fields!"
                raise TypeError(msg)

        # everything looks ok
        return ret

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
    miss_override = schema.__overrides__ - actual_overrides
    undecl_override = actual_overrides - schema.__overrides__
    if miss_override:
        raise TypeError(f"{schema}: Missing claimed field overrides: {miss_override}")

    # all undeclared overrides must be strict subtypes of the inherited type:
    for fname in undecl_override:
        hint, parent_hint = hints[fname], base_hints[fname]
        if not issubtype(hint, parent_hint):
            msg = f"""The type assigned to field '{fname}'
in schema {repr(schema)}:

  {hint}

does not look like a valid subtype of the inherited type:

  {parent_hint}

from schema {cast(Any, infer_parent(schema)).Fields[fname]._origin_name}.

If you are ABSOLUTELY sure that this is a false alarm,
use the @overrides decorator to silence this error
and live forever with the burden of responsibility.
"""
            raise TypeError(msg)
