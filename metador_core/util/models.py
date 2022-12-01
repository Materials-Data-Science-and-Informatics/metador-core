from typing import Dict, Iterator, Set, Type

from pydantic import BaseModel
from pydantic.fields import ModelField

from .typing import get_annotations, get_type_hints, is_subclass_of, traverse_typehint


def field_origins(m: Type[BaseModel], name: str) -> Iterator[Type[BaseModel]]:
    """Return sequence of bases where the field type hint was defined / overridden."""
    return (
        b for b in m.__mro__ if issubclass(b, BaseModel) and name in get_annotations(b)
    )


def field_atomic_types(mf: ModelField, *, bound=object) -> Iterator[Type]:
    """Return sequence of nested atomic types in the hint of given field."""
    return filter(is_subclass_of(bound), traverse_typehint(mf.type_))


def atomic_types(m: BaseModel, *, bound=object) -> Dict[str, Set[Type]]:
    """Return dict from field name to model classes referenced in the field definition.

    Args:
        bound: If provided, will be used to filter results to
          contain only subclasses of the bound.
    """
    return {k: set(field_atomic_types(v, bound=bound)) for k, v in m.__fields__.items()}


def field_parent_type(m: Type[BaseModel], name: str) -> Type[BaseModel]:
    """Return type of field assigned in the next parent that provides a type hint."""
    b = next(filter(lambda x: x is not m, field_origins(m, name)), None)
    if not b:
        raise ValueError(f"No base class of {m} defines a field called '{name}'!")
    return get_type_hints(b).get(name)
