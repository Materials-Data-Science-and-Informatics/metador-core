from typing import List, Optional, Union

import pytest
from pydantic import Extra

from metador_core.plugin.metaclass import UndefVersion
from metador_core.plugin.util import register_in_group
from metador_core.schema.core import MetadataSchema, detect_field_overrides
from metador_core.schema.decorators import add_const_fields, override
from metador_core.schema.parser import BaseParser
from metador_core.util.models import updated_fields


def test_schema_metaclass_exceptions():
    with pytest.raises(TypeError):

        class Foo(MetadataSchema):
            ...

        class Bar(MetadataSchema):
            ...

        # only one parent!
        class Qux(Foo, Bar):
            ...

    with pytest.raises(TypeError):
        # forbidden attributes
        class Foo(MetadataSchema):
            __constants__ = {}

    with pytest.raises(TypeError):
        # prevent many config changes
        class Foo(MetadataSchema):
            class Config:
                bla = 5

    with pytest.raises(TypeError):

        @add_const_fields(dict(cnst="my const"))
        class Foo(MetadataSchema):
            ...

        # prevent using names for fields that are parent consts
        class Bar(Foo):
            cnst: str

    with pytest.raises(TypeError):

        class Foo(MetadataSchema):
            class Config:
                extra = Extra.forbid

        # parent extras forbidden, but in child not
        class Bar(Foo):
            class Config:
                extra = Extra.ignore

    with pytest.raises(TypeError):

        class Foo(MetadataSchema):
            class Config:
                extra = Extra.forbid

        # parent extras forbidden, child has new fields
        class Bar(Foo):
            new_field: int


class DummyModel(MetadataSchema):
    Parser = BaseParser

    val: float


class DummyParent(MetadataSchema):
    ovrd: int


@add_const_fields(dict(cnst="my const"))  # adds a const field
@override("ovrd")  # adds an override marker
class Foo(DummyParent):
    class Plugin:  # plugin inner class
        name = "dummy.plugin"
        version = (0, 1, 0)

    ovrd: Optional[str]  # type: ignore
    _priv: str = "hello"

    x: int
    """A number."""

    y: Optional[DummyModel]
    """A schema."""


class Bar(Foo):
    """Test schema."""

    z: bool
    """Some flag."""


def test_schema_metaclass_init():
    # check parent
    assert Foo.Plugin is not None
    assert set(Foo.__constants__.keys()) == {"cnst"}
    assert Foo.__overrides__ == {"ovrd"}

    # check child
    # ----
    # special markers
    assert Bar.Plugin is None

    assert Bar.__overrides__ is not Foo.__overrides__
    assert not Bar.__overrides__

    assert Bar.__constants__ is not Foo.__constants__
    assert Bar.__constants__ == Foo.__constants__

    # (cached) typehints
    assert "z" not in Bar._base_typehints
    assert "x" in Bar._base_typehints
    assert dict(Bar._base_typehints.items()) == dict(Foo._typehints.items())

    assert "z" in Bar._typehints
    assert "x" in Bar._typehints
    assert Bar._typehints["x"] == Foo._typehints["x"]

    # public
    # ----
    # class is pretty-printed
    assert str(Bar).find("Description:") > 0

    # can access partial
    assert DummyModel.Partial.Parser is DummyModel.Parser
    obj = Bar(x=5, z=True)
    prt = Bar.Partial(z=True).merge_with(Bar.Partial(x=5))
    assert prt.from_partial() == obj

    # can access fields
    assert set(iter(Bar.Fields)) == {
        "ovrd",
        "x",
        "y",
        "z",
    }  # no private or const fields
    assert Bar.Fields.x.origin is Foo
    assert Bar.Fields.x.name == "x"
    assert Bar.Fields.x.description is None
    assert Bar.Fields.y.description == "A schema."
    assert Bar.Fields.z.origin is Bar
    assert list(iter(Bar.Fields.x.schemas)) == []
    assert list(iter(Bar.Fields.y.schemas)) == ["DummyModel"]
    assert Bar.Fields.y.schemas.DummyModel is DummyModel


def test_forbidden_types_rejected(schemas_test):
    """Test field hints being checked for forbidden patterns."""

    class InvalidModel(MetadataSchema):
        class Plugin:
            name = "dummy.test"
            version = (0, 1, 0)

        x: Union[int, List[int]]  # <- a forbidden pattern

    with pytest.raises(TypeError):
        register_in_group(schemas_test, InvalidModel, violently=True)

    class InvalidModel2(MetadataSchema):
        Plugin = InvalidModel.Plugin

        x: List[Optional[int]]  # <- forbidden pattern

    with pytest.raises(TypeError):
        register_in_group(schemas_test, InvalidModel2, violently=True)


def test_wrapped_fields_undef_version(schemas_test):
    """Test that UndefVersion infects introspected field schemas."""
    register_in_group(schemas_test, Foo, violently=True)
    GotFoo = schemas_test["dummy.plugin"]

    assert not UndefVersion._is_marked(Foo)
    assert UndefVersion._is_marked(GotFoo)
    assert repr(GotFoo.Fields.y) == repr(Foo.Fields.y)

    GotDummy = GotFoo.Fields.y.schemas.DummyModel
    assert UndefVersion._is_marked(GotDummy)
    assert UndefVersion._unwrap(GotDummy) is DummyModel

    with pytest.raises(TypeError):

        class InvalidSchema(GotDummy):  # <- no well-defined version
            ...

    with pytest.raises(TypeError):

        @register_in_group(schemas_test, violently=True)
        class InvalidSchema2(Bar):
            class Plugin:
                name = "dummy.invalid"
                version = (0, 1, 2)

            fld: GotDummy  # <- no well-defined version


def test_detect_overridden(schemas_test):
    """Cross-check detect_field_overrides with updated_fields."""
    for name in schemas_test.keys():
        schema = schemas_test[name]
        assert detect_field_overrides(schema).issubset(updated_fields(schema))
