import pytest
from pydantic import ValidationError

from metador_core.schema import MetadataSchema
from metador_core.schema.ld import LDIdRef, LDOrRef, LDSchema, ld, ld_decorator


def test_ld():
    @ld(context="https://hello.com", type="Dummy")
    class DummySchema(MetadataSchema):
        foo: str

    obj = DummySchema(foo="test")
    assert obj.dict() == {
        "@context": "https://hello.com",
        "@type": "Dummy",
        "foo": "test",
    }

    @ld(type="Dummy2")
    class DummySchema2(DummySchema):
        bar: int

    obj2 = DummySchema2(foo="test", bar=123)
    assert obj2.dict() == {
        "@context": "https://hello.com",
        "@type": "Dummy2",
        "foo": "test",
        "bar": 123,
    }

    @ld(context="https://world.com", blub="bla")
    class DummySchema3(DummySchema2):
        ...

    obj3 = DummySchema3(foo="test", bar=123)
    assert obj3.dict() == {
        "@blub": "bla",
        "@context": "https://world.com",
        "@type": "Dummy2",
        "foo": "test",
        "bar": 123,
    }


def test_ld_no_override():
    @ld(context="https://hello.com", type="Dummy")
    class DummySchema(MetadataSchema):
        foo: str

    # overriding -> raises exception

    with pytest.raises(ValueError):

        @ld(type="Dummy2", override=False)
        class DummySchema2(DummySchema):
            bar: int

    with pytest.raises(ValueError):

        @ld(context="https://override.com", override=False)
        class DummySchema2(DummySchema):  # noqa: F811
            bar: int

    # not overriding -> fine
    @ld(blub="value", override=False)
    class DummySchema2(DummySchema):  # noqa: F811
        bar: int


def test_custom_ld():
    myld = ld_decorator(context="https://hello.com", type="Dummy")

    # works even without arguments
    @myld
    class DummySchema(MetadataSchema):
        foo: str

    obj = DummySchema(foo="test")
    assert obj.dict() == {
        "@context": "https://hello.com",
        "@type": "Dummy",
        "foo": "test",
    }

    @myld(type="Dummy2")
    class DummySchema2(DummySchema):
        bar: int

    obj2 = DummySchema2(foo="test", bar=123)
    assert obj2.dict() == {
        "@context": "https://hello.com",
        "@type": "Dummy2",
        "foo": "test",
        "bar": 123,
    }

    @myld(context="https://world.com", blub="bla")
    class DummySchema3(DummySchema2):
        ...

    obj3 = DummySchema3(foo="test", bar=123)
    assert obj3.dict() == {
        "@blub": "bla",
        "@context": "https://world.com",
        "@type": "Dummy",  # <- presets ALWAYS override
        "foo": "test",
        "bar": 123,
    }


# ----


def test_ldschema():
    class DummySchema(LDSchema):
        foo: str

    obj = DummySchema(foo="test")
    assert obj.dict().get("id_") is None
    assert obj.dict().get("@id") is None

    assert not obj.is_ld_ref
    with pytest.raises(ValueError):
        assert obj.ref()

    obj.id_ = "fancy_id"
    assert obj.dict().get("id_") is None
    assert obj.dict()["@id"] == "fancy_id"
    o_ref = obj.ref()

    assert o_ref.is_ld_ref
    assert o_ref.ref() is o_ref
    assert isinstance(o_ref, LDIdRef)
    assert isinstance(o_ref, LDSchema)
    assert o_ref.dict().get("id_") is None
    assert o_ref.dict()["@id"] == "fancy_id"

    # an ID ref has an id_ field
    with pytest.raises(ValidationError):
        LDIdRef(id_="")
    with pytest.raises(ValidationError):
        LDIdRef()


def test_ld_or_ref():
    class DummySchema(LDSchema):
        foo: str

    class OuterSchema(LDSchema):
        bar: LDOrRef[DummySchema]  # either LDIdRef or a DummySchema

    # all of these work
    OuterSchema(bar=LDIdRef(id_="my_id"))
    OuterSchema(bar=DummySchema(foo="blub"))
    OuterSchema(bar=DummySchema(foo="blub", id_="my_id"))

    # these do not
    with pytest.raises(ValidationError):
        OuterSchema()
    with pytest.raises(ValidationError):
        OuterSchema(bar=5)
