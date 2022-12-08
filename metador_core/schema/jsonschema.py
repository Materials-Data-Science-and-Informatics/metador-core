"""Hacks to improve pydantic JSON Schema generation."""

import json
from functools import partial
from typing import Any, Dict, Iterable, List, Type, Union

from pydantic import BaseModel
from pydantic import schema_of as pyd_schema_of
from pydantic.schema import schema as pyd_schemas
from typing_extensions import TypeAlias

from ..util.hashsums import hashsum
from ..util.models import updated_fields

KEY_SCHEMA_DEFS = "$defs"
"""JSON schema key to store subschema definitions."""

KEY_SCHEMA_HASH = "$jsonschema_hash"
"""Custom key to store schema hashsum."""

# ----

JSON_PRIMITIVE_TYPES = (type(None), bool, int, float, str)

# shallow type definitions
JSONPrimitive: TypeAlias = Union[None, bool, int, float, str]
JSONObject: TypeAlias = Dict[str, Any]
JSONArray: TypeAlias = List[Any]
JSONType: TypeAlias = Union[JSONPrimitive, JSONArray, JSONObject]

JSONSCHEMA_STRIP = {
    # these are meta-level keys that should not affect the hash:
    "title",
    "description",
    "examples",
    "$comment",
    "readOnly",
    "writeOnly",
    "deprecated",
    "$id",
    # subschemas have their own hashes, so if referenced schemas
    # change, then the $refs change automatically.
    "definitions",
    KEY_SCHEMA_DEFS,
    # we can't hash the hash, it cannot be part of the clean schema.
    KEY_SCHEMA_HASH,
}
"""Fields to be removed for JSON Schema hashsum computation."""


def clean_jsonschema(obj: JSONType, *, _is_properties: bool = False):
    if isinstance(obj, JSON_PRIMITIVE_TYPES):
        return obj
    if isinstance(obj, list):
        return list(map(clean_jsonschema, obj))
    if isinstance(obj, dict):
        return {
            k: clean_jsonschema(v, _is_properties=k == "properties")
            for k, v in obj.items()
            # must ensure not to touch keys in a properties sub-object!
            if _is_properties or k not in JSONSCHEMA_STRIP
        }

    raise ValueError(f"Object {obj} not of a JSON type: {type(obj)}")


def normalized_json(obj: JSONType) -> bytes:
    return json.dumps(
        obj,
        ensure_ascii=True,
        allow_nan=False,
        indent=None,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def jsonschema_id(schema: JSONType):
    """Compute robust semantic schema identifier.

    A schema identifier is based on the schema plugin name + version
    and its JSON Schema representation, which includes all parent and nested schemas.
    """
    return hashsum(normalized_json(clean_jsonschema(schema)), "sha256")


# ----


def lift_nested_defs(schema: JSONObject):
    """Flatten nested $defs ($defs -> key -> $defs) in-place."""
    if mydefs := schema.get(KEY_SCHEMA_DEFS):
        inner = []
        for schema in mydefs.values():
            lift_nested_defs(schema)
            if nested := schema.pop(KEY_SCHEMA_DEFS, None):
                inner.append(nested)
        for nested in inner:
            mydefs.update(nested)


KEY_PYD_DEFS = "definitions"
"""key name where pydantic stores subschema definitions."""

REF_PREFIX = f"#/{KEY_PYD_DEFS}/"
"""default $refs prefix of pydantic."""


def merge_nested_defs(schema: JSONObject):
    """Merge definitions in-place."""
    if defs := schema.pop(KEY_PYD_DEFS, None):
        my_defs = schema.get(KEY_SCHEMA_DEFS)
        if not my_defs:
            schema[KEY_SCHEMA_DEFS] = {}
            my_defs = schema[KEY_SCHEMA_DEFS]
        # update, by preserve existing
        defs.update(my_defs)
        my_defs.update(defs)


# ----


def collect_defmap(defs: JSONObject):
    """Compute dict mapping current name in $defs to new name based on metador_hash."""
    defmap = {}
    for name, subschema in defs.items():
        if KEY_SCHEMA_HASH in subschema:
            defmap[name] = subschema[KEY_SCHEMA_HASH].strip("/")
        else:
            # print("no hashsum: ", name)
            defmap[name] = name

    return defmap


def map_ref(defmap, refstr: str):
    """Update the `$ref` string based on defmap.

    Will replace `#/definitions/orig`
    with `#/$defs/mapped`.
    """
    if refstr.startswith(REF_PREFIX):
        # print("remap", refstr)
        plen = len(REF_PREFIX)
        if new_name := defmap.get(refstr[plen:]):
            return f"#/{KEY_SCHEMA_DEFS}/{new_name}"
    return refstr


def update_refs(defmap, obj):
    """Recursively update `$ref` in `obj` based on defmap."""
    if isinstance(obj, (type(None), bool, int, float, str)):
        return obj
    elif isinstance(obj, list):
        return list(map(partial(update_refs, defmap), obj))
    elif isinstance(obj, dict):
        return {
            k: (update_refs(defmap, v) if k != "$ref" else map_ref(defmap, v))
            for k, v in obj.items()
        }
    raise ValueError(f"Object {obj} not of a JSON type: {type(obj)}")


def remap_refs(schema):
    """Remap the $refs to use metador_hash-based keys.

    Input must be a completed schema with a global `$defs` section
    that all nested entities use for local references.
    """
    defs = schema.pop(KEY_SCHEMA_DEFS, None)
    if not defs:  # nothing to do
        return schema

    # get name map, old -> new
    defmap = collect_defmap(defs)
    # update refs
    defs.update(update_refs(defmap, defs))
    schema.update(update_refs(defmap, schema))
    # rename defs
    schema[KEY_SCHEMA_DEFS] = {defmap[k]: v for k, v in defs.items()}


# ----


def fixup_jsonschema(schema):
    merge_nested_defs(schema)  # move `definitions` into `$defs`
    lift_nested_defs(schema)  # move nested `$defs` to top level `$defs`
    remap_refs(schema)  # "rename" defs from model name to metador hashsum


def schema_of(model: Type[BaseModel], *args, **kwargs):
    """Return JSON Schema for a model.

    Improved version of `pydantic.schema_of`, returns result
    in $defs normal form, with $ref pointing to the model.
    """
    schema = pyd_schema_of(model, *args, **kwargs)
    schema.pop("title", None)
    fixup_jsonschema(schema)
    return schema


def schemas(models: Iterable[Type[BaseModel]], *args, **kwargs):
    """Return JSON Schema for multiple models.

    Improved version of `pydantic.schema.schema`,
    returns result in $defs normal form.
    """
    schema = pyd_schemas(tuple(models), *args, **kwargs)
    fixup_jsonschema(schema)
    return schema


# ----


def split_model_inheritance(schema: JSONObject, model: Type[BaseModel]):
    """Decompose a model into an allOf combination with a parent model.

    This is ugly because pydantic does in-place wrangling and caching,
    and we need to hack around it.
    """
    # NOTE: important - we assume to get the $defs standard form
    # print("want schema of", model.__base__.__name__)
    base_schema = model.__base__.schema()  # type: ignore

    # compute filtered properties / required section
    schema_new = dict(schema)
    ps = schema_new.pop("properties", None)
    rq = schema_new.pop("required", None)

    lst_fields = updated_fields(model)
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


def finalize_schema_extra(
    schema: JSONObject,
    model: Type[BaseModel],
    *,
    base_model: Type[BaseModel] = None,
) -> None:
    """Perform custom JSON Schema postprocessing.

    To be called as last action in custom schema_extra method in the used base model.

    Arguments:
        base_model: The custom base model that this function is called for.
    """
    base_model = base_model or BaseModel
    assert issubclass(model, base_model)

    # a schema should have a specified standard
    schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"

    if model.__base__ is not base_model:
        # tricky part: de-duplicate fields from parent class
        split_model_inheritance(schema, model)

    # do this last, because it needs everything else to compute the correct hashsum:
    schema[KEY_SCHEMA_HASH] = f"{jsonschema_id(schema)}"
    fixup_jsonschema(schema)
