from __future__ import annotations

import json
from typing import List, Optional, Union

import pytest
from pydantic import BaseModel

from metador_core.schema.jsonschema import (
    KEY_SCHEMA_HASH,
    clean_jsonschema,
    finalize_schema_extra,
    jsonschema_id,
    normalized_json,
    schema_of,
)


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
    KEY_SCHEMA_HASH: "uiae",
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


def test_jsonschema_id_sanity_check():
    assert jsonschema_id(schema) == jsonschema_id(schema_dirty)


# ----


class MyBaseModel(BaseModel):
    """Minimal base model with custom JSON Schema hacks."""

    class Config:
        @staticmethod
        def schema_extra(schema, model):
            print("call extra", model.__name__)
            finalize_schema_extra(schema, model, base_model=MyBaseModel)
            print("done extra", model.__name__)

    @classmethod
    def schema(cls, *args, **kwargs):
        print("call schema", cls.__name__)

        if cls.__base__ is MyBaseModel:
            ...  # no split inheritance
            # just move definitions -> $defs and rename models
        else:
            ...  # split inheritance, normalize
        ret = schema_of(cls, *args, **kwargs)

        print("done schema", cls.__name__)
        return ret


class Model1(MyBaseModel):
    val1: Optional[Model2]
    foo: Model4


class Model2(Model1):
    val2: Optional[List[Union[str, Model1]]]
    qux: bool


class Model3(MyBaseModel):
    bar: str
    baz: Union[int, str]


class Model4(Model3):
    baz: int
    quux: float


for m in [Model1, Model2, Model3, Model4]:
    m.update_forward_refs()

# Model1.schema()


@pytest.mark.skip(reason="FIXME")
def test_lift_nested_defs():
    s, h = [], []
    for m in [Model1, Model2, Model3, Model4]:
        s.append(dict(m.schema()))
        h.append(s[-1]["$ref"].split("/")[-1])
        # ensure we get same schema each time
        assert m.schema() == s[-1]
        # ensure the referenced schema is included
        assert h[-1] in s[-1]["$defs"]
        # check schema_json agrees with schema
        assert json.loads(m.schema_json()) == s[-1]

    # expected number of schemas in the dumps (one per (parent) model)
    assert list(map(lambda x: len(x["$defs"]), s)) == [4, 4, 1, 2]

    # same result (they cross-refer each other)
    assert s[0]["$defs"] == s[1]["$defs"]

    print(Model1.schema_json(indent=2))
