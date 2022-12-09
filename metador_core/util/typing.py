from collections import ChainMap
from typing import Any, Callable, Dict, Iterable, List, Mapping, Set, Tuple, Type, Union

import typing_extensions as te
import typing_utils
from typing_extensions import Annotated, ClassVar, Literal, TypedDict

get_args = te.get_args  # re-export get_args

TypeHint: Any
"""For documentation purposes - to mark type hint arguments."""


def get_type_hints(cls) -> Mapping[str, Any]:
    """Return type hints of this class."""
    return te.get_type_hints(cls, include_extras=True)


def get_annotations(cls, *, all: bool = False) -> Mapping[str, Any]:
    """Return (non-inherited) annotations (unparsed) of given class."""
    if not all:
        return cls.__dict__.get("__annotations__", {})
    return ChainMap(*(c.__dict__.get("__annotations__", {}) for c in cls.__mro__))


to38hint: Dict[Type, Any] = {
    list: List,
    set: Set,
    dict: Dict,
    type: Type,
}
"""Type hint map for consistent behavior between python versions."""


def get_origin(hint):
    # Fixed get_origin to return 3.8-compatible type hints.
    # Need this for pydantic dynamic model generation in 3.8.
    o = te.get_origin(hint)
    return to38hint.get(o, o)


# ----


def is_list(hint):
    return get_origin(hint) is List


def is_set(hint):
    return get_origin(hint) is Set


def is_union(hint):
    return get_origin(hint) is Union


def is_classvar(hint):
    return get_origin(hint) is ClassVar


def is_annotated(hint):
    return get_origin(hint) is Annotated


NoneType = type(None)


def is_nonetype(hint):
    return hint is NoneType


def is_optional(hint):
    # internally, Optional is just sugar for a Union including NoneType.
    return is_union(hint) and any(map(is_nonetype, get_args(hint)))


# ----


def make_typehint(h, *args):
    if is_optional(h):
        args_ = list(args)
        args_.append(type(None))
    elif is_annotated(h):
        args_ = (args[0],)
    else:
        args_ = args
    return h.copy_with(tuple(args_))


UNION_PROXY = Union[int, str]

LIT = Literal[1]
TUP = Tuple[Any]


def make_literal(val):
    """Given a JSON object, return type that parses exactly that object.

    Note that dicts can have extra fields that will be ignored
    and that coercion between bool and int might happen.

    Sets and floats are not supported.
    """
    if val is None:
        return type(None)
    elif isinstance(val, (bool, int, str)):
        return make_typehint(LIT, val)
    elif isinstance(val, (tuple, list)):
        args = tuple(map(make_literal, val))
        return make_typehint(TUP, *args)
    elif isinstance(val, dict):
        d = {k: make_literal(v) for k, v in val.items()}
        # NOTE: the TypedDict must be from typing_extensions for 3.8!
        return TypedDict("AnonConstDict", d)  # type: ignore
    raise ValueError(f"Unsupported value: {val}")


def make_tree_traversal(succ_func: Callable[[Any], Iterable]):
    """Return generator to traverse nodes of a tree-shaped object.

    Args:
        child_func: Function to be called on each node returning Iterable of children
        post_order: If True, will emit the parent node after children instead of before
    """

    def traverse(obj, *, post_order: bool = False):
        if not post_order:
            yield obj
        for t in succ_func(obj):
            yield from traverse(t, post_order=post_order)
        if post_order:
            yield obj

    return traverse


traverse_typehint = make_tree_traversal(get_args)
"""Perform depth-first pre-order traversal of a type annotation.

Args:
    th: type hint object to be traversed
"""


def make_tree_mapper(node_constructor, succ_func):
    def map_func(obj, leaf_map_func):
        if children := succ_func(obj):
            mcs = (map_func(c, leaf_map_func) for c in children)
            return node_constructor(obj, *mcs)
        else:
            return leaf_map_func(obj)

    return map_func


map_typehint = make_tree_mapper(make_typehint, get_args)


def unoptional(th):
    """Return type hint that is not optional (if it was optional)."""
    if is_annotated(th):
        # remove inner optional, preserve annotation
        return make_typehint(th, unoptional(get_args(th)[0]))

    if not is_union(th):
        # all optionals are actually unions -> nothing to do
        return th

    # filter out NoneType from the Union arguments
    args = tuple(filter(lambda h: not is_nonetype(h), get_args(th)))
    if len(args) == 1:
        # not a union anymore -> remove type
        return args[0]
    # remove union without NoneType (i.e. not optional)
    return make_typehint(UNION_PROXY, *args)


# ----


def is_subtype(sub, base):
    # add hack to ignore pydantic Annotated FieldInfo
    # NOTE: this is only superficial, actually issubtype must be fixed
    # or it won't work with nested Annotated types
    ann_sub, ann_base = is_annotated(sub), is_annotated(base)
    if ann_sub != ann_base:
        return False  # not equal on annotated wrapping status

    if not ann_sub:
        # proceed as usual
        return typing_utils.issubtype(sub, base)
    else:
        sub_args, base_args = get_args(sub), get_args(base)
        # NOTE: FieldInfo of pydantic is not comparable :( so we ignore it
        # same_ann = list(sub_args)[1:] == list(base_args)[1:]
        return is_subtype(sub_args[0], base_args[0])  # and same_ann


def is_subtype_of(t: Any) -> Callable[[Any], bool]:
    """Return a predicate to check issubtype for a given type."""
    return lambda obj: is_subtype(obj, t)


def is_instance_of(t: Any) -> Callable[[Any], bool]:
    """Return a predicate to check isinstance for a given type."""
    return lambda obj: isinstance(obj, t)


def is_subclass_of(t: Any) -> Callable[[Any], bool]:
    """Return a predicate to check issubclass for a given type."""
    return lambda obj: isinstance(obj, type) and issubclass(obj, t)
