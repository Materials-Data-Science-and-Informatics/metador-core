import json
from dataclasses import dataclass
from typing import Optional

import pytest
from pydantic import BaseModel, ValidationError

from metador_core.schema.encoder import (
    DynEncoderModelMetaclass,
    add_json_encoder,
    json_encoder,
)
from metador_core.schema.parser import BaseParser, ParserMixin

# ----
# Test custom parser


class SomeModel(BaseModel):
    x: str


class TrivialParsed(ParserMixin, SomeModel):
    """Parser does nothing, only accepts an instance."""

    Parser = BaseParser


class StringParsed(ParserMixin, SomeModel):
    """Parser packs a string into the model."""

    class Parser(BaseParser):
        schema_info = dict(title="Foobar")
        strict = True

        @classmethod
        def parse(cls, target, v):
            return target(x=v)


class SubModel(StringParsed):
    """Subclass of class with custom parser, should not inherit it."""


class InvalidParsed(ParserMixin, SomeModel):
    """Incorrect parser implementation (violates strict flag)."""

    class Parser(BaseParser):
        strict = True

        @classmethod
        def parse(cls, target, v):
            return v  # <- invalid, not returning target instance


class OuterModel(BaseModel):
    y: StringParsed
    s: Optional[SubModel]
    z: Optional[TrivialParsed]
    i: Optional[InvalidParsed]


def test_custom_parser():
    with pytest.raises(TypeError):

        class Invalid(ParserMixin, SomeModel):
            class Parser:
                ...  # <- not subclass of BaseParser

        class FaultyModel(BaseModel):
            x: Invalid

        list(FaultyModel.__get_validators__())

    with pytest.raises(RuntimeError):  # for i
        obj = OuterModel(y="hello", i={"x": "everyone"})

    with pytest.raises(ValidationError):  # for z
        obj = OuterModel(y="hello", z="world")

    with pytest.raises(ValidationError):  # for s
        obj = OuterModel(y="hello", s="everyone")

    # test parsing
    obj = OuterModel(y="hello", s=SubModel(x="test"), z=TrivialParsed(x="world"))
    assert isinstance(obj.y, StringParsed)
    assert isinstance(obj.z, TrivialParsed)

    # make sure the description is attached to jsonschema
    assert obj.schema()["properties"]["y"]["title"] == "Foobar"


# ----
# Test custom JSON encoder


def test_invalid_custom_encoder():
    with pytest.raises(TypeError):
        # not allowed on pydantic models
        @json_encoder(lambda _: "foo")
        class MyModel(BaseModel):
            ...

    with pytest.raises(TypeError):
        # not allowed on dataclasses
        @json_encoder(lambda _: "foo")
        @dataclass
        class MyModel2:
            ...

    @json_encoder(lambda _: "foo")
    class MyModel3:
        ...

    with pytest.raises(ValueError):
        # not allowed twice
        add_json_encoder(MyModel3, lambda _: "bar")


def test_custom_encoder():
    class CustomClass:
        ...

    @json_encoder(lambda _: "foo")
    class JsonableClass(CustomClass):
        ...

    class MyBaseModel(BaseModel, metaclass=DynEncoderModelMetaclass):
        class Config:
            arbitrary_types_allowed = True

    class DummyModel(MyBaseModel):
        x: JsonableClass

    # check that serialization is as expected (custom function)
    j = DummyModel(x=JsonableClass()).json()
    d = json.loads(j)
    assert d.get("x") == "foo"

    class NonJsonableClass:
        ...

    class DummyModel2(MyBaseModel):
        x: NonJsonableClass

    o = DummyModel2(x=NonJsonableClass())
    o.dict()  # should work
    with pytest.raises(TypeError):  # not JSON serializable!
        o.json()


# ----
# Test combination (non-pydantic class with added custom parser and json encoder)


def test_custom_parser_encoder():
    class CombinedBaseModel(BaseModel, metaclass=DynEncoderModelMetaclass):
        ...

    class CustomClass:
        ...

    @json_encoder(lambda _: "foo")
    class ParsableClass(ParserMixin, CustomClass):
        class Parser(BaseParser):
            strict = True

            @classmethod
            def parse(cls, target, val):
                if val == "foo":
                    return target()
                raise ValueError(f"Invalid input: {val}")

    class DummyModel2(CombinedBaseModel):
        x: ParsableClass

    with pytest.raises(ValidationError):
        j = DummyModel2(x="bar").json()

    # serialize, deserialize
    j = DummyModel2(x="foo").json()
    d = json.loads(j)
    o = DummyModel2.parse_obj(d)
    assert isinstance(o.x, ParsableClass)
    assert isinstance(o.x, CustomClass)

    # make serialization invalid -> should fail
    d["x"] = "bar"
    with pytest.raises(ValidationError):
        o = DummyModel2.parse_obj(d)
