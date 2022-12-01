"""Helpers to create pydantic models that parse into and (de-)serialize to JSON-LD."""
from __future__ import annotations

from collections import ChainMap
from typing import Any, Dict, Mapping, Optional, TypeVar, Union

from pydantic import Extra, Field
from typing_extensions import Annotated, TypeAlias

from .core import MetadataSchema
from .decorators import add_const_fields
from .types import NonEmptyStr


def with_key_prefix(prefix: str, dct: Mapping[str, Any]) -> Dict[str, Any]:
    """Return new dict with all keys prefixed by `prefix`."""
    return {f"{prefix}{k}": v for k, v in dct.items() if v is not None}


def ld_decorator(**presets):
    """Return LD schema decorator with pre-set fields, e.g. `@context`.

    The returned decorator will attach the passed fields to a schema.

    All additional fields passed to the decorator will also be added,
    if not present, or override the default that is passed to this function.

    Note that the pre-set fields will ALWAYS override existing fields,
    regardless of the state of the override flag.

    Example usage:
    Pass your `@context` as `context` to this decorator factory.
    Use the returned decorator with `type` in order to
    set both the `@context` and `@type` for a schema.

    By default, will silently override any inherited
    constant fields that already exist in the schema.
    """

    def decorator(schema=None, *, override: bool = True, **kwargs):
        fields = with_key_prefix("@", ChainMap(kwargs, presets))
        dec = add_const_fields(fields, override=override)
        return dec if schema is None else dec(schema)

    return decorator


ld = ld_decorator()
"""Decorator to add constant JSON-LD fields equal for all instances of a schema."""


class LDSchema(MetadataSchema):
    """Semantically enriched schema for JSON-LD."""

    id_: Annotated[Optional[NonEmptyStr], Field(alias="@id")]

    def ref(self) -> LDIdRef:
        """Return LDIdRef, i.e. a pure @id reference for object.

        Throws an exception if no @id is found.
        """
        if self.id_ is None:
            raise ValueError("Object has no @id attribute!")
        return LDIdRef(id_=self.id_)

    @property
    def is_ld_ref(self):
        return False


class LDIdRef(LDSchema):
    """Object with just an @id reference (more info is given elsewhere)."""

    class Config:
        extra = Extra.forbid

    id_: Annotated[NonEmptyStr, Field(alias="@id")]

    def ref(self) -> LDIdRef:
        return self

    @property
    def is_ld_ref(self):
        return True


T = TypeVar("T", bound=LDSchema)
LDOrRef: TypeAlias = Union[LDIdRef, T]
"""LDOrRef[T] is either an object of LD Schema T, or a reference to an object.

An LD reference is just an object with an @id.
"""
