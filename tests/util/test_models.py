from __future__ import annotations

import json
from typing import List, Optional, Set, Union

import pytest
from frozendict import frozendict
from pydantic import BaseModel

from metador_core.schema.encoder import add_json_encoder
from metador_core.util.models import (
    atomic_types,
    field_atomic_types,
    field_origins,
    field_parent_type,
    new_fields,
    updated_fields,
)

add_json_encoder(frozendict, lambda x: json.dumps(dict(x)))


class MyBaseModel(BaseModel):
    foo: Optional[List[Union[int, Set[str]]]]

    class Config:
        json_encoders = {frozendict: lambda x: dict(x.items())}

    def dict(self, *args, **kwargs):
        return frozendict(super().dict(*args, **kwargs))


class ModelA(MyBaseModel):
    bar: Union[int, str]
    baz: bool


class ModelB(ModelA):
    foo: List[int]  # type: ignore


class ModelC(ModelB):
    bar: str


def test_field_origins():
    assert list(field_origins(ModelC, "foo")) == [ModelB, MyBaseModel]
    assert list(field_origins(ModelC, "bar")) == [ModelC, ModelA]
    assert list(field_origins(ModelC, "baz")) == [ModelA]
    assert list(field_origins(ModelC, "qux")) == []  # no such field -> no origins


def test_updated_fields():
    assert set(updated_fields(MyBaseModel)) == {"foo"}
    assert set(updated_fields(ModelA)) == {"bar", "baz"}
    assert set(updated_fields(ModelB)) == {"foo"}
    assert set(updated_fields(ModelC)) == {"bar"}


def test_new_fields():
    assert set(new_fields(MyBaseModel)) == {"foo"}
    assert set(new_fields(ModelA)) == {"bar", "baz"}
    assert set(new_fields(ModelB)) == set()
    assert set(new_fields(ModelC)) == set()


def test_field_atomic_types():
    # no upper bound
    assert list(field_atomic_types(MyBaseModel.__fields__["foo"])) == [int, str]
    # with upper bound
    assert list(field_atomic_types(MyBaseModel.__fields__["foo"], bound=int)) == [int]


def test_collect_model_types():
    # no upper bound
    ret = atomic_types(ModelB)
    assert "qux" not in ret
    assert ret["foo"] == {int}
    assert ret["bar"] == {int, str}
    assert ret["baz"] == {bool}

    # add a upper bound on collected types
    ret = atomic_types(ModelC, bound=int)
    assert ret["foo"] == {int}
    assert ret["bar"] == set()
    assert ret["baz"] == {bool}


def test_field_parent():
    assert field_parent_type(ModelC, "foo") == List[int]
    assert field_parent_type(ModelB, "foo") == Optional[List[Union[int, Set[str]]]]
    assert field_parent_type(ModelC, "bar") == Union[int, str]
    assert field_parent_type(ModelC, "baz") == bool

    with pytest.raises(ValueError):
        field_parent_type(ModelC, "qux")  # no hint, not defined
    with pytest.raises(ValueError):
        print(field_parent_type(ModelC, "quux"))  # no hint, but defined
