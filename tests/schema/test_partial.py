from __future__ import annotations

from typing import List, Optional, Set

import pytest
from pydantic import BaseModel, Field, ValidationError
from typing_extensions import Annotated

from metador_core.schema.partial import PartialFactory, PartialModel

from ..util.test_models import MyBaseModel

# from hypothesis import given, strategies as st


class ModelG(MyBaseModel):
    xyz: Optional[int]


class ModelD(MyBaseModel):
    class Config:
        frozen = True

    val: str
    val2: Optional[int]


class ModelE(ModelG):
    class Config:
        frozen = True

    prv: Annotated[ModelD, Field(description="simple model + annotation")]
    rec: Optional[ModelE]  # direct recursion
    col: Optional[Set[ModelD]]  # set
    fwd: List[ModelF] = []  # forward reference (+ indirect recursion)


class ModelF(ModelE):
    ...


class MyPartialFactory(PartialFactory):
    base_model = MyBaseModel


def test_partial_inheritance():
    # only for models sharing the selected base:
    class SomeModel(BaseModel):
        ...

    with pytest.raises(TypeError):
        MyPartialFactory.get_partial(SomeModel)

    classes = [ModelD, ModelE, ModelF, ModelG]
    for cls1 in classes:
        p1 = MyPartialFactory.get_partial(cls1)
        # partial is subclass of partial mixin and the base model:
        assert issubclass(p1, MyPartialFactory.base_model)
        assert issubclass(p1, MyPartialFactory.partial_mixin)
        #  partial MUST NOT be a subclass of original:
        assert not issubclass(p1, cls1)
        for cls2 in classes:
            p2 = MyPartialFactory.get_partial(cls2)
            if cls1 is cls2:
                # idempotent partial generation
                assert p1 is p2

            # but partials reflect inheritance of non-partials:
            orig_subclass = issubclass(cls1, cls2)
            partial_subclass = issubclass(p1, p2)
            assert orig_subclass == partial_subclass


# TODO: test partials properly


def test_partial_behavior():
    PartF = MyPartialFactory.get_partial(ModelF)

    # empty object is always possible
    empty_obj = PartF()

    # example object
    hlp = ModelF(prv=ModelD(val="c"))
    obj = ModelF(prv=ModelD(val="a"), fwd=[hlp.copy()], col=set([hlp.prv.copy()]))
    objE = ModelE(**obj.dict())

    # to_partial
    p_obj = PartF.to_partial(obj)  # no validation case
    assert PartF.to_partial(p_obj) is not p_obj  # should be fresh
    assert PartF.to_partial(objE) == p_obj
    assert PartF.to_partial(objE.dict()) == p_obj

    # test behavior with with invalid fields
    objInv = obj.copy(update=dict(prv=5))
    with pytest.raises(ValidationError):
        PartF.to_partial(objInv.dict())  # failed validation
    # with flag to drop invalid, it should work
    pObjInv = PartF.to_partial(objInv.dict(), ignore_invalid=True)
    # check that the result agrees with dropping the field (setting to "None")
    assert pObjInv.dict() == p_obj.copy(update={"prv": None}).dict()

    # cast
    assert PartF.cast(p_obj) is p_obj  # nothing to do -> returns original
    # should convert
    assert PartF.cast(obj) == p_obj
    p_objE = PartF.cast(objE)
    assert p_objE == p_obj
    assert p_objE is not p_obj

    # to partial -> from partial == original
    assert PartF.from_partial(p_obj) == obj

    # empty object is neutral element
    assert empty_obj.merge_with(p_obj, allow_overwrite=True) == p_obj
    assert p_obj.merge_with(empty_obj, allow_overwrite=True) == p_obj
    assert PartialModel.merge_with(empty_obj, empty_obj) == empty_obj
    assert PartialModel.merge(empty_obj, p_obj, empty_obj) == p_obj

    # without overwrite flag: cannot overwrite "a" with "a"
    with pytest.raises(ValueError):
        assert p_obj.merge_with(p_obj) == p_obj

    # merge 0 -> empty partial
    assert PartF.merge() == empty_obj

    # merge 1 -> partial with same content as given, implicit cast
    assert isinstance(PartF.merge(obj), PartF)
    assert PartF.merge(obj) == obj

    # merge 2 -> non-trivial merge
    hlp2 = ModelF(prv=ModelD(val="d"))
    obj2 = ModelF(
        prv=ModelD(val="b", val2=123),
        fwd=[hlp.copy(), hlp2.copy()],
        col=set([hlp.prv.copy(), hlp2.prv.copy()]),
    )
    obj3 = obj2.copy(update={"prv": obj.prv})
    merged = PartF.merge(obj2, obj3, allow_overwrite=True).from_partial()
    assert merged.col == obj2.col

    assert merged.prv.val == obj.prv.val  # overwritten value
    assert merged.prv.val2 == 123  # preserved by rec. merge
    # the list should be self-concatenated with deep merge
    assert merged.fwd == obj2.fwd + obj3.fwd
