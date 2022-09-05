"""Helpers to create pydantic models that parse into and (de-)serialize to JSON-LD."""

from typing import Any, Dict, Optional, TypeVar, Union

from pydantic import Extra, Field
from typing_extensions import Annotated, TypeAlias

from .core import MetadataSchema
from .decorators import const
from .types import NonEmptyStr


def ld(**kwargs) -> Dict[str, Any]:
    """Return dict where all passed argument names are prefixed with @."""
    return {f"@{k}": v for k, v in kwargs.items() if v}


def ld_type_decorator(context=None):
    """Return LD schema decorator for given context.

    By default will silently override any inherited `@context` and/or `@type`.
    """

    def decorator(name: str, *, override: bool = True):
        return const(ld(context=context, type=name), override=override)

    return decorator


ld_type = ld_type_decorator()
"""Decorator to set @type without providing a context."""


class LDIdRef(MetadataSchema):
    """Object with just an @id reference (more info is given elsewhere)."""

    class Config:
        extra = Extra.forbid

    id_: Annotated[NonEmptyStr, Field(alias="@id")]

    @property
    def is_ld_ref(self):
        return True


class LDSchema(MetadataSchema):
    """Semantically enriched schema for JSON-LD."""

    id_: Annotated[Optional[NonEmptyStr], Field(alias="@id")]

    def ref(self) -> LDIdRef:
        """Return LDIdRef, i.e. a pure @id reference for object.

        Throws an exception if no @id is found.
        """
        assert self.id_ is not None, "Object has no @id attribute!"
        return LDIdRef(id_=self.id_)

    @property
    def is_ld_ref(self):
        return False


T = TypeVar("T", bound=LDSchema)
LDOrRef: TypeAlias = Union[LDIdRef, T]
"""LDOrRef[T] is either an object of LD Schema T, or a reference to an object.

An LD reference is just an object with an @id.
"""
