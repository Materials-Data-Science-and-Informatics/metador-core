import pytest

from metador_core.schema.inspect import (
    UnsupportedOperation,
    WrappedLiftedDict,
    lift_dict,
)


def test_lifted_ro_dict():
    """Test LiftedRODict functionality."""
    dct = dict(x=0, y=1, z=2)
    LDct = lift_dict("LDct", dct)

    # check it's read-only (__setattr__)
    with pytest.raises(UnsupportedOperation):
        LDct.a = 5
    with pytest.raises(TypeError):
        LDct["a"] = 5

    # read (__getattr__, __getitem__)
    assert LDct.x == 0
    assert LDct["x"] == LDct.x
    with pytest.raises(AttributeError):
        LDct.a

    # __contains__
    assert "a" not in LDct
    assert "x" in LDct

    # __bool__
    assert not bool(lift_dict("LDctEmpty", {}))
    assert bool(LDct)

    # __dir__
    assert dir(LDct) == ["x", "y", "z"]

    LDct2 = lift_dict("LDct2", dct, keys=["z", "y", "x"])

    # __iter__
    assert list(iter(LDct)) == ["x", "y", "z"]
    assert list(iter(LDct2)) == ["z", "y", "x"]

    # __repr__
    r1 = repr(LDct)
    assert 0 <= r1.find("x") < r1.find("y") < r1.find("z")
    r2 = repr(LDct2)
    assert 0 <= r2.find("z") < r2.find("y") < r2.find("x")


def test_wrapped():
    """Test WrappedLiftedDict (with a value mapping function)."""
    LDct = lift_dict("LDct", dict(hello="world"))
    WDct = WrappedLiftedDict(LDct, lambda _: "surprise")
    assert WDct.hello == "surprise"
    assert WDct["hello"] == "surprise"
    assert repr(LDct) == repr(WDct)
