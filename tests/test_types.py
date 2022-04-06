import pytest
from pydantic import BaseModel, ValidationError

import ardiem_container.types as t


def parse_as(model, val):
    class DummyModel(BaseModel):
        __root__: model

    return DummyModel(__root__=val).__root__


def test_str_types():
    # nonempty_str
    parse_as(t.nonempty_str, "a")
    with pytest.raises(ValidationError):
        parse_as(t.nonempty_str, "")

    # mimetype_str
    parse_as(t.mimetype_str, "application/json")
    parse_as(t.mimetype_str, "application/JSON;q=0.9;v=abc")
    with pytest.raises(ValidationError):
        parse_as(t.mimetype_str, "invalid/mime/type")
    with pytest.raises(ValidationError):
        parse_as(t.mimetype_str, "invalid mime")
    with pytest.raises(ValidationError):
        parse_as(t.mimetype_str, "invalidMime")

    # hashsum_str
    parse_as(t.hashsum_str, "sha256:aebf")
    parse_as(t.hashsum_str, "md5:aebf")
    with pytest.raises(ValidationError):
        parse_as(t.hashsum_str, "invalid:aebf")
    with pytest.raises(ValidationError):
        parse_as(t.hashsum_str, "md5:invalid")
    with pytest.raises(ValidationError):
        parse_as(t.hashsum_str, "md5")
    with pytest.raises(ValidationError):
        parse_as(t.hashsum_str, "md5:")
    with pytest.raises(ValidationError):
        parse_as(t.hashsum_str, "aebf")
    with pytest.raises(ValidationError):
        parse_as(t.hashsum_str, ":aebf")

    # PintUnit
    parse_as(t.PintUnit, "meter / (second * kg) ** 2")
    parse_as(t.PintUnit, "dimensionless")
    parse_as(t.PintUnit, t.PintUnit.Parsed("second"))
    parse_as(t.PintUnit, "1")
    with pytest.raises(ValidationError):
        parse_as(t.PintUnit, "invalid")
    with pytest.raises(ValidationError):
        parse_as(t.PintUnit, "2")
    with pytest.raises(ValidationError):
        parse_as(t.PintUnit, "")
    with pytest.raises(ValidationError):
        parse_as(t.PintUnit, 123)

    class SomeModel(BaseModel):
        u: t.PintUnit

    SomeModel(u="meters * second").schema_json().lower().find("pint") >= 0  # type: ignore
