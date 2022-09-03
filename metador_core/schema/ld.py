"""Helpers to create pydantic models that parse into and (de-)serialize to JSON-LD."""

from typing import Any, Dict, Optional

from pydantic import Extra, Field
from typing_extensions import Annotated

from .core import MetadataSchema
from .decorators import const
from .types import NonEmptyStr


def ld(**kwargs) -> Dict[str, Any]:
    """Return dict where all passed argument names are prefixed with @."""
    return {f"@{k}": v for k, v in kwargs.items() if v}


def ld_type_decorator(context=None):
    """Return LD schema decorator for given context."""

    def decorator(name: str, *, override: bool = False):
        return const(ld(type=name, context=context), override=override)

    return decorator


ld_type = ld_type_decorator()
"""Decorator to set @type without providing a context."""


class LDIdRef(MetadataSchema):
    """Object with just an @id reference (more info is given elsewhere)."""

    class Config:
        extra = Extra.forbid

    id_: Annotated[NonEmptyStr, Field(alias="@id")]


class LDSchema(MetadataSchema):
    """Semantically enriched schema for JSON-LD."""

    id_: Annotated[Optional[NonEmptyStr], Field(alias="@id")]

    def ref(self) -> LDIdRef:
        """Return LDIdRef, i.e. a pure @id reference for object.

        Throws an exception if no @id is found.
        """
        assert self.id_ is not None, "Object has no @id attribute!"
        return LDIdRef(id_=self.id_)
