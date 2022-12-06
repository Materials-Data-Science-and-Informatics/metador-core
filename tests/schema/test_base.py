from enum import Enum
from typing import Optional

import pytest
from pydantic import Field, ValidationError
from typing_extensions import Annotated

from metador_core.schema.base import BaseModelPlus


class DummyEnum(str, Enum):
    a = "a"
    b = "b"


def test_base_model():
    with pytest.raises(ValidationError):
        # defaults are validated
        class WrongDummy(BaseModelPlus):
            foo: int = "invalid"

        WrongDummy()  # default is used -> invalid

    # now a valid model:
    class DummyModel(BaseModelPlus):
        _priv: str

        foo: Annotated[Optional[int], Field(alias="@foo")]
        bar: Optional[float]
        val: DummyEnum

    # inf/nan are forbidden
    with pytest.raises(ValidationError):
        DummyModel(bar=float("nan"))
    with pytest.raises(ValidationError):
        DummyModel(bar=float("inf"))

    # allows using non-alias name (foo) for assignment
    obj = DummyModel(foo=5, val="a", xtra="hi")

    # setters are validated
    with pytest.raises(ValidationError):
        obj.bar = "wrong"
    obj.bar = 2.41
    obj.bar = None

    # private can be set (but is ignored)
    obj._priv = "something"

    # extra is preserved on load
    assert obj.xtra == "hi"

    # yaml dump/load works
    assert DummyModel.parse_raw(obj.yaml()) == obj
    # bytes serialization (utf-8 json with newline)
    assert DummyModel.parse_raw(bytes(obj)) == obj
    # string is json with newlines
    assert DummyModel.parse_raw(str(obj)) == obj
    assert len(str(obj).split()) > 1

    # normal serializations
    assert DummyModel.parse_raw(obj.json()) == obj
    assert DummyModel.parse_obj(obj.dict()) == obj
    assert DummyModel.parse_obj(obj.json_dict()) == obj

    # test the default behavior
    js = obj.dict()

    # private fields are removed on dump
    assert "_priv" not in js

    # extra fields are preserved on dump
    assert "xtra" in js

    # enums become plain types
    assert isinstance(js["val"], str)

    # serialization to alias name
    assert "@foo" in js
    assert "foo" not in js

    # no None values
    assert "bar" not in js
