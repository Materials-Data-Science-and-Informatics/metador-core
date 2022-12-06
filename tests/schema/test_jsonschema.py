import json

import pytest

from metador_core.schema.jsonschema import clean_jsonschema, normalized_json

# TODO: test custom jsonschema export


@pytest.mark.parametrize("x", [None, True, 10, 1.0, "hello"])
def test_clean_jsonschema_primitives(x):
    assert clean_jsonschema(x) == x


dirt = {
    "title": "some title",
    "description": "blah",
    "examples": ["a", "b"],
    "readOnly": True,
    "writeOnly": False,
    "deprecated": False,
    "$id": "some_identifier",
    "definitions": {"uiae": {}},
    "$defs": {"dtrn": 42},
    "$metador_schema_hash": "uiae",
}
"""Noise to be added to json schemas."""


schema = {
    "required": ["x"],
    "properties": {
        "x": {
            "type": "string",
            "format": "email",
        },
        "y": {
            "type": "int",
            "exclusiveMinimum": 10,
        },
    },
}

schema_dirty = {
    **dirt,  # type: ignore
    "required": ["x"],
    "properties": {
        "x": {
            **dirt,  # type: ignore
            "type": "string",
            "format": "email",
        },
        "y": {
            **dirt,  # type: ignore
            "type": "int",
            "exclusiveMinimum": 10,
        },
    },
}


def test_clean_jsonschema_nested():
    assert clean_jsonschema(schema_dirty) == schema


def test_normalized_json_str():
    input = {"y": None, "x": [True, 1.234e-10, "z", 567]}
    expected = b'{"x":[true,1.234e-10,"z",567],"y":null}'
    result = normalized_json(input)
    assert result == expected
    assert json.loads(result) == input


# def test_lift_nested_defs():
