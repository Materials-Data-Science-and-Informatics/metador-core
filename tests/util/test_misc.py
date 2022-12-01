import pytest

from metador_core.util import drop
from metador_core.util.pytest import parameters


@pytest.mark.parametrize("n", [0, 1, 2, 3, 4])
def test_drop(n):
    lst = list(range(0, 3))
    assert list(drop(n, lst)) == lst[n:]


def test_parameters():
    assert parameters(1) == [(1,)]
    assert parameters([1, 2, 3]) == [(1,), (2,), (3,)]

    exp = [
        ("x", "y", "a"),
        ("x", "y", "b"),
        ("x", "y", "c", "d"),
        ("x", "y", "c", "e"),
        ("z", "f"),
    ]
    assert parameters({"x": {"y": ["a", "b", {"c": ["d", "e"]}]}, "z": "f"}) == exp
