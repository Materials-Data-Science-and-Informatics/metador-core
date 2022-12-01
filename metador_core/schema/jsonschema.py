import json
from functools import partial

from pydantic import schema_of as pyd_schema_of
from pydantic.schema import schema as pyd_schemas

from ..plugin.metaclass import UndefVersion
from ..util.hashsums import hashsum

KEY_SCHEMA_DEFS = "$defs"
"""JSON schema key to store subschema definitions."""

KEY_SCHEMA_HASH = "$metador_hash"
"""Custom key to store schema hashsum."""

# ----

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
"""Fields to strip for JSON Schema hashsum."""


def clean_jsonschema(obj, *, is_properties: bool = False):
    if isinstance(obj, (type(None), bool, int, float, str)):
        return obj
    if isinstance(obj, list):
        return list(map(clean_jsonschema, obj))
    if isinstance(obj, dict):
        return {
            k: clean_jsonschema(v, is_properties=k == "properties")
            for k, v in obj.items()
            # must ensure not to touch keys in a properties sub-object!
            if is_properties or k not in JSONSCHEMA_STRIP
        }

    raise ValueError(f"Object {obj} not of a JSON type: {type(obj)}")


def jsonschema_id(schema):
    """Compute robust schema identifier.

    A schema identifier is based on the schema plugin name + version
    and its JSON Schema representation, which includes all parent and nested schemas.
    """
    rep = json.dumps(clean_jsonschema(schema))
    ret = hashsum(rep.encode("utf-8"), "sha256")[:16]
    return ret


# ----


def lift_nested_defs(schema):
    """Flatten nested $defs ($defs -> key -> $defs)."""
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


def merge_nested_defs(schema):
    """Merge definitions."""
    if defs := schema.pop(KEY_PYD_DEFS, None):
        my_defs = schema.get(KEY_SCHEMA_DEFS)
        if not my_defs:
            schema[KEY_SCHEMA_DEFS] = {}
            my_defs = schema[KEY_SCHEMA_DEFS]
        # update, by preserve existing
        defs.update(my_defs)
        my_defs.update(defs)


# ----


def collect_defmap(defs):
    """Compute dict mapping current name in $defs to new name based on metador_hash."""
    defmap = {}
    for name, subschema in defs.items():
        if KEY_SCHEMA_HASH in subschema:
            defmap[name] = subschema[KEY_SCHEMA_HASH].strip("/")
        else:
            print("no hashsum: ", name)
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


def eliminate_true(obj, in_properties: bool = False):
    """Replace `true` with `{}`."""
    if isinstance(obj, (type(None), bool, int, float, str)):
        return obj
    elif isinstance(obj, list):
        return list(map(eliminate_true, obj))
    elif isinstance(obj, dict):
        return {
            k: {}
            if v == True and in_properties
            else eliminate_true(v, k == "properties")
            for k, v in obj.items()
        }
    raise ValueError(f"Object {obj} not of a JSON type: {type(obj)}")


# ----


def fixup_jsonschema(schema):
    schema.update(eliminate_true(schema))
    merge_nested_defs(schema)  # move `definitions` into `$defs`
    lift_nested_defs(schema)  # move nested `$defs` to top level `$defs`
    remap_refs(schema)  # "rename" defs from model name to metador hashsum


def schema_of(model):
    """Improved version of `pydantic.schema_of`."""
    schema = pyd_schema_of(UndefVersion._unwrap(model) or model)
    fixup_jsonschema(schema)
    return schema


def schemas(models, *args, **kwargs):
    """Improved version of `pydantic.schema.schema`."""
    models = tuple(map(lambda m: UndefVersion._unwrap(m) or m, models))
    schema = pyd_schemas(models, *args, **kwargs)
    fixup_jsonschema(schema)
    return schema
