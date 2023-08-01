import enum
from typing import Literal, Optional

import pytest
from pydantic import BaseModel, ValidationError

from metador_core.plugin.util import register_in_group
from metador_core.schema import MetadataSchema
from metador_core.schema.decorators import add_const_fields, make_mandatory, override


def test_schema_base_check():
    """Should refuse to work on non-metador models."""

    class NonSchema(BaseModel):
        foo: Optional[int]

    with pytest.raises(TypeError):
        make_mandatory("foo")(NonSchema)

    with pytest.raises(TypeError):
        add_const_fields({"bla": None})(NonSchema)

    with pytest.raises(TypeError):
        override("foo")(NonSchema)


@pytest.fixture
def dummy_schema():
    class DummySchema(MetadataSchema):
        foo: Optional[int]

    return DummySchema


def test_field_private_check(dummy_schema):
    """Should prevent messing with private fields."""
    with pytest.raises(ValueError):
        make_mandatory("foo", "_bar")(dummy_schema)

    with pytest.raises(ValueError):
        add_const_fields({"bla": int, "_bar": int})(dummy_schema)

    with pytest.raises(ValueError):
        override("_bar")(dummy_schema)


def test_add_const_fields(dummy_schema):
    assert dummy_schema(foo=5).dict().get("foo") == 5

    # new constant field works
    add_const_fields({"bar": "world"})(dummy_schema)
    assert dummy_schema(foo=5).dict().get("bar") == "world"

    with pytest.raises(ValueError):
        # override = False, foo exists
        add_const_fields({"foo": "blub"})(dummy_schema)

    # override into a constant field works
    add_const_fields({"foo": "hello"}, override=True)(dummy_schema)
    assert dummy_schema(foo=5).dict().get("foo") == "hello"

    # preserved in subclass
    class Child(dummy_schema):
        ...

    assert Child(foo=5).dict().get("foo") == "hello"

    # behaves like parent type in foo
    class ChildWorks(dummy_schema):
        qux: str

    assert ChildWorks(qux="x", foo=5).dict().get("foo") == "hello"
    assert ChildWorks(qux="y").dict().get("bar") == "world"

    # const can be overridden with other const
    add_const_fields({"foo": "bye"}, override=True)(ChildWorks)
    assert ChildWorks(qux="x", foo=5).dict().get("foo") == "bye"

    # cannot override constant with normal field again
    with pytest.raises(TypeError):

        class ChildFail(dummy_schema):
            foo: int


def test_add_const_fields_enum():
    SomeEnum = enum.Enum("SomeEnum", dict(a="A", b="B"))  # type: ignore

    class A(MetadataSchema):
        x: SomeEnum

    # works without override (restriction)
    @add_const_fields({"x": SomeEnum.a})
    class B(A):
        ...

    # invalid without override
    with pytest.raises(TypeError):
        # not enum value
        @add_const_fields({"x": "invalid"})
        class C(A):
            ...

    with pytest.raises(ValueError):
        # already defined as constant field
        @add_const_fields({"x": SomeEnum.b})
        class D(B):
            ...


def test_add_const_fields_literal():
    class A(MetadataSchema):
        x: Literal["foo", 42]

    # works without override (restriction)
    @add_const_fields({"x": "foo"})
    class B(A):
        ...

    # invalid without override
    with pytest.raises(TypeError):
        # not type value
        @add_const_fields({"x": "invalid"})
        class C(A):
            ...

    with pytest.raises(ValueError):
        # already defined as constant field
        @add_const_fields({"x": 42})
        class D(B):
            ...


def test_make_mandatory(dummy_schema):
    with pytest.raises(ValueError):
        # no such field
        @make_mandatory("bar")
        class DummyChild1(dummy_schema):
            ...

    with pytest.raises(ValueError):
        # collision: decorator + definition
        @make_mandatory("foo")
        class DummyChild2(dummy_schema):
            foo: int

    @make_mandatory("foo")
    class DummyChild(dummy_schema):
        ...

    # field is already defined by decorator
    with pytest.raises(ValueError):
        make_mandatory("foo")(DummyChild)

    # parent still ok with optional field
    assert dummy_schema().foo is None

    # child now requires it
    with pytest.raises(ValidationError):
        DummyChild()  # missing foo
    assert DummyChild(foo=5).foo == 5  # ok


def test_override(dummy_schema, plugingroups_test):
    schemas = plugingroups_test.get("schema")

    assert dummy_schema(foo=123).foo == 123

    class DummyChild(dummy_schema):
        class Plugin:
            name = "test.dummy"
            version = (0, 1, 0)

        foo: str

    assert DummyChild(foo="hello").foo == "hello"

    with pytest.raises(TypeError):
        # str not subtype of optional[int]
        register_in_group(schemas, DummyChild, violently=True)

    # with override it works
    override("foo")(DummyChild)
    register_in_group(schemas, DummyChild, violently=True)

    with pytest.raises(ValueError):
        # missing override hint for foo
        @register_in_group(schemas, violently=True)
        @override("foo")
        class DummyChild2(dummy_schema):
            class Plugin:
                name = "test.dummy2"
                version = (0, 1, 0)

    with pytest.raises(ValueError):
        # override non-existing -> error
        @register_in_group(schemas, violently=True)
        @override("bar")
        class DummyChild3(dummy_schema):
            class Plugin:
                name = "test.dummy3"
                version = (0, 1, 0)
