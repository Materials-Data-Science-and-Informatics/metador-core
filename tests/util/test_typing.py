from __future__ import annotations

import sys
from typing import List, Literal, Optional, Set, Tuple, Union

import pytest
from pydantic import parse_obj_as
from typing_extensions import Annotated, ClassVar

from metador_core.util import typing as t

# test (enhanced) wrappers for normal checks


@pytest.mark.skipif(sys.version_info < (3, 9), reason="test needs Python >= 3.9")
def test_compat_get_origin():
    assert t.get_origin(list[int]) is t.List
    assert t.get_origin(set[int]) is t.Set
    assert t.get_origin(dict[int, str]) is t.Dict
    assert t.get_origin(type[int]) is t.Type


def test_hint_predicates():
    assert t.is_list(t.List[int])
    assert not t.is_list(t.Set[int])

    assert t.is_set(t.Set[int])
    assert not t.is_set(t.List[int])

    assert t.is_classvar(ClassVar[int])
    assert not t.is_classvar(int)

    assert t.is_nonetype(type(None))
    assert not t.is_nonetype(None)

    assert t.is_union(t.Union[int, str])
    assert t.is_union(Optional[int])  # Optional is a union
    assert t.is_union(t.Union[None, int])  # same as Optional
    assert not t.is_union(t.Union[int, int])

    assert t.is_optional(Optional[int])
    assert t.is_optional(Optional[t.Union[int, str]])
    assert t.is_optional(t.Union[None, int, str])  # union with None is Optional
    assert not t.is_optional(t.Union[int, str])

    assert t.is_annotated(Annotated[Optional[bool], "foo"])
    assert not t.is_annotated(Optional[Annotated[bool, "foo"]])
    assert not t.is_annotated(int)


class DummyClass:
    foo: Optional[str]


class DummyClass2(DummyClass):
    bar: int


def test_get_type_hints():
    # evaluated type hints
    th = t.get_type_hints(DummyClass2)
    assert th.get("foo") == Optional[str]
    assert th.get("bar") == int


def test_get_annotations():
    # unevaluated type hint
    ann = t.get_annotations(DummyClass2)
    assert "foo" not in ann
    assert ann.get("bar") == "int"

    # including inherited hints
    ann2 = t.get_annotations(DummyClass2, all=True)
    assert ann2.get("foo") == "Optional[str]"
    assert ann2.get("bar") == "int"


# test more specific stuff


def test_make_typehint():
    assert t.make_typehint(Literal[1], 5 == Literal[5])
    assert t.make_typehint(t.List[int], bool) == t.List[bool]
    assert t.make_typehint(t.Union[int, str], float, bool) == t.Union[float, bool]
    assert t.make_typehint(Optional[int], str) == t.Union[type(None), str]
    assert (
        t.make_typehint(Annotated[t.List[int], "hello"], bool)
        == Annotated[bool, "hello"]
    )


def test_make_literal():
    assert t.make_literal(None) == type(None)  # noqa: E721
    assert t.make_literal(True) == Literal[True]
    assert t.make_literal(1) == Literal[1]
    assert t.make_literal("foo") == Literal["foo"]

    # lists and tuples are mapped to tuples
    assert t.make_literal(["a", 1]) == t.Tuple[Literal["a"], Literal[1]]
    assert t.make_literal(("a", 1)) == t.Tuple[Literal["a"], Literal[1]]

    # test typeddict literal with pydantic
    TD = t.make_literal({"a": "bar"})
    assert parse_obj_as(TD, {"a": "bar"})
    with pytest.raises(ValueError):
        assert parse_obj_as(TD, {"a": "baz"})

    # sets and floats not supported
    with pytest.raises(ValueError):
        t.make_literal({1, 2, 3})
    with pytest.raises(ValueError):
        t.make_literal(3.12)


def test_traverse_typehint():
    u = Set[str]
    v, w = Tuple[int, bool], Annotated[u, "a"]
    x = Union[v, w]  # won't be visited, will be merged with y by type constructor
    y = Optional[x]
    z = List[y]
    hint = Annotated[z, "b"]

    expected_pre = [hint, z, y, v, int, bool, w, u, str, "a", type(None), "b"]
    assert list(t.traverse_typehint(hint)) == expected_pre

    expected_post = [int, bool, v, str, u, "a", w, type(None), y, z, "b", hint]
    assert list(t.traverse_typehint(hint, post_order=True)) == expected_post


def test_map_typehint():
    # map over structure of type hint, should preserve annotations
    orig = Tuple[int, List[Annotated[Union[int, Set[str]], "x"]]]
    exp = Tuple[bool, List[Annotated[Union[bool, Set[str]], "x"]]]

    def int_to_bool(x):
        return x if x is not int else bool

    assert t.map_typehint(orig, int_to_bool) == exp


def test_unoptional():
    # non-Optionals are just returned
    assert t.unoptional(int) == int
    assert t.unoptional(object) == object
    assert t.unoptional(type(None)) == type(None)  # noqa: E721

    # optional is unwrapped, nested type is untouched
    assert t.unoptional(Optional[int]) == int
    assert t.unoptional(Optional[t.List[str]]) == t.List[str]
    assert t.unoptional(Optional[t.Union[str, int]]) == t.Union[int, str]
    assert t.unoptional(Optional[Annotated[bool, "foo"]]) == Annotated[bool, "foo"]

    # annotation is preserved and transparent
    assert t.unoptional(Annotated[Optional[bool], "foo"]) == Annotated[bool, "foo"]


def test_is_subtype():
    # following is broken, should be fixed in typing_utils > 0.1.0
    # https://github.com/bojiang/typing_utils/pull/12
    # assert t.is_subtype(Literal["a"], Literal["a", "b"])

    assert t.is_subtype(int, t.Union[int, str])
    assert t.is_subtype(t.List[str], t.List[t.Union[t.Set[int], str]])
    assert t.is_subtype(Optional[int], t.Union[None, int, str])
    assert t.is_subtype(DummyClass2, DummyClass)
    assert t.is_subtype(List[int], Optional[List[Union[int, Set[str]]]])

    assert not t.is_subtype(DummyClass, DummyClass2)
    assert not t.is_subtype(Optional[int], t.Union[int, str])

    # Annotated types must agree on the type and annotation status, annotation is ignored
    # (at least currently this is the semantics)
    assert t.is_subtype(Annotated[int, "x"], Annotated[t.Union[int, str], "y"])
    assert not t.is_subtype(int, Annotated[t.Union[int, str], "y"])

    # Literal (custom hack)
    assert t.is_subtype(Literal[1], Literal[1, 2])
    assert t.is_subtype(Literal["b", "a"], Literal["a", "b"])
    assert t.is_subtype(Literal["a"], Literal["a", "b"])
    assert t.is_subtype(Literal["b"], Literal["a", "b"])
    assert not t.is_subtype(Literal["c"], Literal["a", "b"])
    assert not t.is_subtype(Literal["b", "a"], Literal["a"])


def test_predicates():
    d1, d2 = DummyClass(), DummyClass2()
    lst = [DummyClass, DummyClass2, d1, d2]

    class DummyClass3:
        ...

    lst2 = [DummyClass, DummyClass2, DummyClass3]

    # test predicates in a filter
    assert list(filter(t.is_subclass_of(DummyClass2), lst)) == [DummyClass2]
    assert list(filter(t.is_instance_of(DummyClass2), lst)) == [d2]
    assert list(filter(t.is_subtype_of(DummyClass), lst2)) == [DummyClass, DummyClass2]
