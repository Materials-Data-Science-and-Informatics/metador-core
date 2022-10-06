import json
from functools import partial

from pydantic import schema_of as pyd_schema_of
from pydantic.schema import schema as pyd_schemas

from ..hashutils import hashsum
from ..plugin.metaclass import UndefVersion

JSCHEMA_DEFINITIONS = "definitions"
REF_PREFIX = f"#/{JSCHEMA_DEFINITIONS}/"

JSCHEMA_DEFS = "$defs"
# JSCHEMA_NDEFS_KEY = "$metador_nested_defs"

JSCHEMA_HASH_KEY = "$metador_hash"
JSCHEMA_PG_KEY = "$metador_plugin"
JSCHEMA_CONSTFIELDS_KEY = "$metador_constants"


JSONSCHEMA_STRIP = {
    "title",
    "description",
    "definitions",
    "examples",
    "$comment",
    "readOnly",
    "writeOnly",
    "deprecated",
    "$id",
    JSCHEMA_DEFS,
    JSCHEMA_HASH_KEY,
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


def renamed_defs(func, *args, **kwargs):
    """Wrap a jsonschema pydantic function to put `definitions` into `$defs`."""
    ref_template = kwargs.pop("ref_template", None) or "#/$defs/{model}"
    ret = dict(func(*args, **kwargs, ref_template=ref_template))
    if defs := ret.pop("definitions", None):
        ret["$defs"] = defs
    return ret


def lift_nested_defs(schema):
    """Flatten nested $defs ($defs -> key -> $defs)."""
    if mydefs := schema.get(JSCHEMA_DEFS):
        inner = []
        for schema in mydefs.values():
            lift_nested_defs(schema)
            if nested := schema.pop(JSCHEMA_DEFS, None):
                inner.append(nested)
        for nested in inner:
            mydefs.update(nested)


def merge_nested_defs(schema):
    """Merge definitions."""
    if defs := schema.pop(JSCHEMA_DEFINITIONS, None):
        my_defs = schema.get(JSCHEMA_DEFS)
        if not my_defs:
            schema[JSCHEMA_DEFS] = {}
            my_defs = schema[JSCHEMA_DEFS]
        # update, by preserve existing
        defs.update(my_defs)
        my_defs.update(defs)


def collect_defmap(defs):
    """Compute dict mapping current name in $defs to new name based on metador_hash."""
    defmap = {}
    for name, subschema in defs.items():
        if JSCHEMA_HASH_KEY in subschema:
            defmap[name] = subschema[JSCHEMA_HASH_KEY].strip("/")
        else:
            print("no hashsum: ", name)
            defmap[name] = name

    return defmap


def map_ref(defmap, refstr):
    # print("remap", refstr)
    if refstr.startswith(REF_PREFIX):
        plen = len(REF_PREFIX)
        if new_name := defmap.get(refstr[plen:]):
            return f"#/{JSCHEMA_DEFS}/{new_name}"
    return refstr


def update_refs(defmap, obj):
    """Substitute $ref based on defmap."""
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

    Input must be a completed schema with a global $defs section
    that all entities use for local references.
    """
    defs = schema.get(JSCHEMA_DEFS)
    if not defs:  # nothing to do
        return schema

    defmap = collect_defmap(defs)
    # print(schema, defmap)
    # update refs
    schema.update(update_refs(defmap, schema))
    # rename defs
    schema[JSCHEMA_DEFS] = {defmap[k]: v for k, v in defs.items()}


# ----


def fixup_jsonschema(schema):
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
