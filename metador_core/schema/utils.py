from io import UnsupportedOperation
from typing import Any, Callable, Dict, Iterable, Set

from pydantic import BaseModel
from pydantic.fields import ModelField
from typing_extensions import get_args


def is_instance_of(t: Any) -> Callable[[Any], bool]:
    """Return a predicate to check isinstance for a given type."""
    return lambda obj: isinstance(obj, t)


def is_subclass_of(t: Any) -> Callable[[Any], bool]:
    """Return a predicate to check issubclass for a given type."""
    return lambda obj: issubclass(obj, t)


def tree_traversal(children: Callable[[Any], Iterable], *, post_order: bool = False):
    """Return generator to traverse nodes of a tree-shaped object.

    Args:
        child_func: Function to be called on each node returning Iterable of children
        post_order: If True, will emit the parent node after children instead of before
    """

    def traverse(obj):
        if not post_order:
            yield obj
        for t in children(obj):
            yield from traverse(t)
        if post_order:
            yield obj

    return traverse


traverse_typehint = tree_traversal(get_args)
"""Perform depth-first pre-order traversal of a type annotation.

Args:
    th: type hint object to be traversed
"""


def field_types(mf: ModelField, *, bound=object) -> Iterable:
    return filter(
        is_subclass_of(bound), filter(is_instance_of(type), traverse_typehint(mf.type_))
    )


def collect_model_types(m: BaseModel, *, bound=object) -> Set:
    """Return set of classes referenced in the definition of a pydantic model.

    Args:
        bound: If provided, will be used to filter results to
          contain only subclasses of the bound.
    """
    return set.union(
        set(), *map(lambda mf: set(field_types(mf, bound=bound)), m.__fields__.values())
    )


class LiftedDict(type):
    """Metaclass for classes providing dict keys as attributes.

    Mostly for aesthetic reasons and to be used for things
    where the dict is actually a fixed lookup table.
    """

    _schemas: Dict[str, Any]
    _repr: str = ""

    def __repr__(self):
        if self._repr:
            return self._repr
        return repr(self)

    def __dir__(self):
        return dir(super()) + list(self._schemas.keys())

    def __getattr__(self, key):
        if s := self._schemas.get(key):
            return s
        raise AttributeError(key)


class LiftedRODict(LiftedDict):
    """Like LiftedDict, but prohibits setting the attributes."""

    def __setattr__(self, key, value):
        raise UnsupportedOperation
