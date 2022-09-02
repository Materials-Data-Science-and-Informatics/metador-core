from collections import ChainMap
from dataclasses import dataclass
from io import UnsupportedOperation
from typing import (  # type: ignore
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    Set,
    Tuple,
    Type,
    Union,
    _GenericAlias,
)

import typing_extensions
from pydantic import BaseModel
from pydantic.fields import ModelField
from typing_extensions import Annotated, Literal, TypedDict, get_args


def is_instance_of(t: Any) -> Callable[[Any], bool]:
    """Return a predicate to check isinstance for a given type."""
    return lambda obj: isinstance(obj, t)


def is_subclass_of(t: Any) -> Callable[[Any], bool]:
    """Return a predicate to check issubclass for a given type."""
    return lambda obj: issubclass(obj, t)


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


NoneType = type(None)


def is_nonetype(hint):
    return hint is NoneType


def is_optional(hint):
    # internally, Optional is just sugar for a Union including NoneType.
    return is_union(hint) and any(map(is_nonetype, get_args(hint)))


def get_annotations(cls):
    """Return (non-inherited) annotations (unparsed) of given class."""
    return cls.__dict__.get("__annotations__", {})


def get_type_hints(cls, *, include_inherited: bool = False) -> Mapping[str, Any]:
    """Return type hints of this class (if desired, including inherited)."""
    if not include_inherited:
        return typing_extensions.get_type_hints(cls, include_extras=True)
    return ChainMap(
        *(
            typing_extensions.get_type_hints(c, include_extras=True)
            for c in cls.__mro__
            if "__annotations__" in c.__dict__
        )
    )


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


def field_types(mf: ModelField, *, bound=object) -> Iterable[Type]:
    return filter(
        is_subclass_of(bound), filter(is_instance_of(type), traverse_typehint(mf.type_))
    )


def collect_model_types(m: BaseModel, *, bound=object) -> Dict[str, Set[Type]]:
    """Return set of classes referenced in the definition of a pydantic model.

    Args:
        bound: If provided, will be used to filter results to
          contain only subclasses of the bound.
    """
    return {k: set(field_types(v, bound=bound)) for k, v in m.__fields__.items()}


# ----


class LiftedDict(type):
    """Metaclass for classes providing dict keys as attributes.

    Mostly for aesthetic reasons and to be used for things
    where the dict is actually a fixed lookup table.
    """

    _dict: Dict[str, Any]
    _repr: str = ""

    def __repr__(self):
        if self._repr:
            return self._repr
        return repr(list(self._dict.keys()))

    def __dir__(self):
        return list(self._dict.keys())

    def __getattr__(self, key):
        if s := self._dict.get(key):
            return s
        raise AttributeError(key)


class LiftedRODict(LiftedDict):
    """Like LiftedDict, but prohibits setting the attributes."""

    def __setattr__(self, key, value):
        raise UnsupportedOperation


@dataclass
class FieldInspector:
    origin: Type

    name: str
    type: Type
    schemas: Type

    @property
    def description(self) -> str:
        from simple_parsing.docstring import get_attribute_docstring

        docs = get_attribute_docstring(self.origin, self.name)
        return docs.docstring_below or docs.comment_above or docs.comment_inline

    def __init__(self, model, name, description, hint, schemas):
        self.origin = model
        self.name = name
        self.type = hint

        class Schemas(metaclass=LiftedRODict):
            _dict = {s.__name__: s for s in schemas}

        self.schemas = Schemas


def attach_field_inspector(model: BaseModel, *, bound=BaseModel):
    """Attach inner class to a model for sub-model lookup.

    This enables users to access subschemas without extra imports,
    improving decoupling of plugins and packages.

    Also can be used for introspection about fields.
    """
    field_schemas = collect_model_types(model, bound=bound)
    field_hint = {k: v for k, v in get_type_hints(model).items() if k in field_schemas}

    class FieldInspectors(metaclass=LiftedRODict):
        _dict = {
            n: FieldInspector(model, n, "", field_hint[n], field_schemas[n])
            for n in field_schemas
        }

    setattr(model, "Fields", FieldInspectors)
