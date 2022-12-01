from __future__ import annotations

from typing import List, Optional, Set

import pytest
from pydantic import BaseModel

from metador_core.schema.partial import DeepPartialModel, PartialModel

from ..util.test_models import MyBaseModel


class ModelG(MyBaseModel):
    xyz: Optional[int]


class ModelD(MyBaseModel):
    class Config:
        frozen = True

    val: str


class ModelE(ModelG):
    class Config:
        frozen = True

    prv: ModelD  # simple model
    rec: Optional[ModelE]  # direct recursion
    col: Optional[Set[ModelD]]  # set
    fwd: List[ModelF] = []  # forward reference (+ indirect recursion)


class ModelF(ModelE):
    ...


@pytest.mark.parametrize("p_cls", [PartialModel, DeepPartialModel])
def test_partial_inheritance(p_cls):
    class MyPartial(p_cls, MyBaseModel):
        ...

    # only for models sharing the selected base:
    class SomeModel(BaseModel):
        ...

    with pytest.raises(TypeError):
        MyPartial._get_partial(SomeModel)

    classes = [ModelD, ModelE, ModelF, ModelG]
    for cls1 in classes:
        p1 = MyPartial._get_partial(cls1)
        # partial is subclass of partial mixin and the base model:
        assert issubclass(p1, MyBaseModel)
        assert issubclass(p1, MyPartial)
        #  partial MUST NOT be a subclass of original:
        assert not issubclass(p1, cls1)
        for cls2 in classes:
            p2 = MyPartial._get_partial(cls2)
            if cls1 is cls2:
                # idempotent partial generation
                assert p1 is p2

            # but partials reflect inheritance of non-partials:
            orig_subclass = issubclass(cls1, cls2)
            partial_subclass = issubclass(p1, p2)
            assert orig_subclass == partial_subclass


# TODO: test partials properly


@pytest.mark.parametrize("p_cls", [PartialModel, DeepPartialModel])
def test_partial_behavior(p_cls):
    class MyPartial(p_cls, MyBaseModel):
        ...

    PartF = MyPartial._get_partial(ModelF)

    # empty object is always possible
    empty_obj = PartF()

    # example object
    hlp = ModelF(prv=ModelD(val="c"))
    # hlp2 = ModelF(prv=ModelD(val="d"))
    obj = ModelF(prv=ModelD(val="a"), fwd=[hlp.copy()], col=set([hlp.prv.copy()]))
    # obj2 = ModelF(
    #     prv=ModelD(val="b"),
    #     fwd=[hlp.copy(), hlp2.copy()],
    #     col=set([hlp.prv.copy(), hlp2.prv.copy()]),
    # )

    # convert it to a partial
    p_obj = PartF.to_partial(obj)
    # to partial -> from partial == original
    assert PartF.from_partial(p_obj) == obj
    # empty object is neutral element
    assert empty_obj.merge_with(p_obj, allow_overwrite=True) == p_obj
    assert p_obj.merge_with(empty_obj, allow_overwrite=True) == p_obj

    # without overwrite flag: cannot overwrite "a" with "a"
    with pytest.raises(ValueError):
        assert p_obj.merge_with(p_obj) == p_obj

    # self-merge
    # p_obj2 = p_obj.copy()
    # p_obj2.fwd = list(p_obj2.fwd) + list(p_obj2.fwd)
    # print(p_obj2.json(indent=2))
    # ret = p_obj.merge_with(p_obj, allow_overwrite=True)
    # print(ret.json(indent=2))
    # assert ret == p_obj2
