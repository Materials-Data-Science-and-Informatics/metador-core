"""Helpers to create pydantic models that parse into and (de-)serialize to JSON-LD."""

from typing import Any, Dict, Optional

from pydantic import BaseModel, Extra, Field
from pydantic.fields import FieldInfo, ModelField
from typing_extensions import Annotated

from .core import MetadataSchema
from .utils import make_literal


def add_annotations(consts: Dict[str, Any], **kwargs):
    """Decorate pydantic models to add constant annotation fields.

    Must be passed a dict of field names mapped to the default values (only default JSON types).

    Annotated fields are optional during parsing and are added to a parsed instance.
    If is present during parsing, they must have exactly the passed annotated value.

    Annotation fields are included in serialization, unless exclude_defaults is set.

    This can be used e.g. to make JSON data models semantic by attaching JSON-LD annotations.
    """
    allow_override = kwargs.pop("allow_override", True)
    if kwargs:
        raise ValueError(f"Unknown keyword arguments: {kwargs}")

    def add_fields(mcls):
        if not issubclass(mcls, BaseModel):
            raise ValueError("This is a decorator for pydantic models!")
        if not allow_override:
            if shadowed := set.intersection(
                set(consts.keys()), set(mcls.__fields__.keys())
            ):
                msg = f"Attached const fields {shadowed} override defined fields in {mcls}!"
                raise ValueError(msg)

        for name, value in consts.items():
            val = value.default if isinstance(value, FieldInfo) else value
            ctype = Optional[make_literal(val)]  # type: ignore
            field = ModelField.infer(
                name=name,
                value=value,
                annotation=ctype,
                class_validators=None,
                config=mcls.__config__,
            )
            mcls.__fields__[name] = field
            mcls.__annotations__[name] = field.type_

        return mcls

    return add_fields


def ld_type(name, *, context: str = ""):
    ret = {"@type": name}
    if context:
        ret["@context"] = context
    return ret


class LDIdRef(MetadataSchema):
    """Object with just an @id reference (more info is given elsewhere)."""

    class Config:
        extra = Extra.forbid

    id_: Annotated[str, Field(alias="@id", min_length=1)]


class LDSchema(MetadataSchema):
    """Semantically enriched schema for JSON-LD."""

    id_: Annotated[Optional[str], Field(alias="@id", min_length=1)]

    def ref(self) -> LDIdRef:
        """Return LDIdRef, i.e. a pure @id reference for object.

        Throws an exception if no @id is found.
        """
        assert self.id_ is not None, "Object has no @id attribute!"
        return LDIdRef(id_=self.id_)
