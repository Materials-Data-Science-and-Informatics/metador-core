from collections import ChainMap
from typing import (  # type: ignore
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    Set,
    Tuple,
    Type,
    Union,
    _GenericAlias,
)

import typing_extensions
import typing_utils
from pydantic import BaseModel
from pydantic.fields import ModelField
from typing_extensions import Annotated, ClassVar, Literal, TypedDict, get_args


def drop(n: int, it: Iterable):
    """Drop fixed number of elements from iterator."""
    return (x for i, x in enumerate(it) if i >= n)


def is_instance_of(t: Any) -> Callable[[Any], bool]:
    """Return a predicate to check isinstance for a given type."""
    return lambda obj: isinstance(obj, t)


def is_subclass_of(t: Any) -> Callable[[Any], bool]:
    """Return a predicate to check issubclass for a given type."""
    return lambda obj: issubclass(obj, t)


def is_public_name(n: str):
    return n[0] != "_"


to38hint: Dict[Type, Any] = {
    list: List,
    set: Set,
    dict: Dict,
    type: Type,
}


def get_origin(hint):
    # Fixed get_origin to return 3.8-compatible type hints.
    # Need this for pydantic dynamic model generation in 3.8.
    o = typing_extensions.get_origin(hint)
    return to38hint.get(o, o)


def is_list(hint):
    return get_origin(hint) is List


def is_set(hint):
    return get_origin(hint) is Set


def is_union(hint):
    return get_origin(hint) is Union


def is_classvar(hint):
    return get_origin(hint) is ClassVar


NoneType = type(None)


def is_nonetype(hint):
    return hint is NoneType


def is_optional(hint):
    # internally, Optional is just sugar for a Union including NoneType.
    return is_union(hint) and any(map(is_nonetype, get_args(hint)))


def get_type_hints(cls) -> Mapping[str, Any]:
    """Return type hints of this class (if desired, including inherited)."""
    return typing_extensions.get_type_hints(cls, include_extras=True)


def get_annotations(cls, *, all: bool = False) -> Mapping[str, Any]:
    """Return (non-inherited) annotations (unparsed) of given class."""
    if not all:
        return cls.__dict__.get("__annotations__", {})
    return ChainMap(*(c.__dict__.get("__annotations__", {}) for c in cls.__mro__))


def issubtype(sub, base):
    return typing_utils.issubtype(sub, base)


def make_typehint(t_cons_proxy, *t_args):
    """Construct a type hint based on a proxy type hint object and desired args."""
    t_cons = get_origin(t_cons_proxy)
    if t_cons is Annotated:
        return typing_extensions._AnnotatedAlias(t_args[0], tuple(t_args[1:]))
    return _GenericAlias(t_cons, tuple(t_args))


def make_literal(val):
    """Given a JSON object, return type that parses exactly that object."""
    if isinstance(val, dict):
        d = {k: make_literal(v) for k, v in val.items()}
        return TypedDict("AnonConstDict", d)  # type: ignore
    if isinstance(val, tuple) or isinstance(val, list):
        args = tuple(map(make_literal, val))
        return _GenericAlias(Tuple, args)
    if val is None:
        return type(None)
    # using Literal directly makes Literal[True] -> Literal[1] -> don't want that
    return _GenericAlias(Literal, val)


# ----


class ParserMixin:
    """Base mixin class to simplify creation of custom pydantic field types."""

    class ParserConfig:
        _loaded = True

    @classmethod
    def _parser_config(cls, base=None):
        base = base or cls
        try:
            conf = base.ParserConfig
        except AttributeError:
            # this class has no own parser config
            # -> look up in base class and push down
            conf = cls._parser_config(base)
            setattr(base, "ParserConfig", conf)

        # config is initialized -> return it
        if getattr(conf, "_loaded", False):
            return conf

        # find next upper parser config
        nxt = list(filter(lambda c: issubclass(c, ParserMixin), base.mro()))[1]
        if nxt is None:
            raise RuntimeError("Did not find base ParserConfig")

        base_conf = base._parser_config(nxt)
        for key, value in base_conf.__dict__.items():
            if key[0] == "_":
                continue
            if not hasattr(conf, key):
                setattr(conf, key, value)

        setattr(conf, "_loaded", True)
        return conf

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def __modify_schema__(cls, schema):
        schema.update(**cls._parser_config().schema)

    @classmethod
    def validate(cls, v):
        """Validate passed value and return as declared `Parsed` type."""
        try:
            return cls.parse(v)
        except ValueError as e:
            msg = f"Could not parse {cls.__name__} value {v}: {str(e)}"
            raise ValueError(msg)


# ----


def make_tree_traversal(
    succ_func: Callable[[Any], Iterable], *, post_order: bool = False
):
    """Return generator to traverse nodes of a tree-shaped object.

    Args:
        child_func: Function to be called on each node returning Iterable of children
        post_order: If True, will emit the parent node after children instead of before
    """

    def traverse(obj):
        if not post_order:
            yield obj
        for t in succ_func(obj):
            yield from traverse(t)
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


def field_model_types(mf: ModelField, *, bound=object) -> Iterable[Type]:
    return filter(
        is_subclass_of(bound), filter(is_instance_of(type), traverse_typehint(mf.type_))
    )


def collect_model_types(m: BaseModel, *, bound=object) -> Dict[str, Set[Type]]:
    """Return dict from field name to model classes referenced in the field definition.

    Args:
        bound: If provided, will be used to filter results to
          contain only subclasses of the bound.
    """
    return {k: set(field_model_types(v, bound=bound)) for k, v in m.__fields__.items()}


def field_origins(m: Type[BaseModel], name: str) -> Iterator[Type[BaseModel]]:
    """Return sequence of bases where the field type hint was defined / overridden."""
    return (
        b for b in m.__mro__ if issubclass(b, BaseModel) and name in get_annotations(b)
    )


def parent_field_type(m: Type[BaseModel], name: str) -> Iterable[Type[BaseModel]]:
    b = next(filter(lambda x: x is not m, field_origins(m, name)), None)
    if not b:
        raise ValueError(f"No base class of {m} defines a field called '{name}'!")
    # hints = b.__typehints__ or get_type_hints(b)
    # if not b.__typehints__:
    #     b.__typehints__ = hints
    hints = get_type_hints(b)
    if name not in hints:
        raise TypeError(f"No type annotation for '{name}' in base {b} of {m}!")
    return hints[name]


def unoptional(th):
    """Return type hint that is not optional (if it was optional)."""
    if not is_union(th):
        return th
    args = tuple(filter(lambda h: not is_nonetype(h), get_args(th)))
    if len(args) == 1:
        return args[0]
    return make_typehint(th, *args)
