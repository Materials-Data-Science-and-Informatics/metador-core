"""Definition of Metador schema interface and core schemas."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Optional

import isodate
from pydantic import BaseModel, Extra, Field
from pydantic.fields import FieldInfo, ModelField
from pydantic_yaml import YamlModelMixin
from typing_extensions import Annotated

from .types import PintQuantity, PintUnit, make_literal

# PGSchema is supposed to be imported by other code from here!
# this ensures that typing works but no circular imports happen
if TYPE_CHECKING:
    from .plugingroup import PGSchema
else:
    PGSchema = Any

# ----


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


# ----


def _mod_def_dump_args(kwargs):
    """Set `by_alias=True` in given kwargs dict, if not set explicitly."""
    if "by_alias" not in kwargs:
        kwargs["by_alias"] = True
    if "exclude_none" not in kwargs:
        kwargs["exclude_none"] = True
    return kwargs


class MetadataSchema(YamlModelMixin, BaseModel):
    """Extended Pydantic base model with custom serializers and functions.

    Use (subclasses of) this baseclass to create new Metador metadata schemas and plugins.
    """

    # Plugin: SchemaPlugin

    class Config:
        underscore_attrs_are_private = True  # avoid using PrivateAttr all the time
        use_enum_values = True  # to serialize enums properly
        allow_population_by_field_name = (
            True  # when alias is set, still allow using field name
        )
        validate_assignment = True
        # https://pydantic-docs.helpmanual.io/usage/exporting_models/#json_encoders
        json_encoders = {
            PintUnit.Parsed: lambda x: str(x),
            PintQuantity.Parsed: lambda x: str(x),
            isodate.Duration: lambda x: isodate.duration_isoformat(x),
        }

    @classmethod
    def partial_from(cls, model):
        """Return partial model based on a different model, skipping validation."""
        return cls.construct(_fields_set=model.__fields_set__, **model.__dict__)

    def update(self, new_atrs):
        """Update fields from dict (validates resulting dict, then modifies model)."""
        merged = self.dict()
        merged.update(new_atrs)
        self.validate(merged)  # also runs root_validators!
        for k, v in new_atrs.items():
            setattr(
                self, k, v
            )  # parses/validates on assigment due to validate_assignment

    def dict(self, *args, **kwargs):
        return super().dict(*args, **_mod_def_dump_args(kwargs))

    def json(self, *args, **kwargs):
        return super().json(*args, **_mod_def_dump_args(kwargs))

    def yaml(self, *args, **kwargs):
        return super().yaml(*args, **_mod_def_dump_args(kwargs))

    def __bytes__(self) -> bytes:
        """Serialize to JSON and return UTF-8 encoded bytes to be written in a file."""
        # add a newline, as otherwise behaviour with text editors will be confusing
        # (e.g. vim automatically adds a trailing newline that it hides)
        # https://stackoverflow.com/questions/729692/why-should-text-files-end-with-a-newline
        return (self.json() + "\n").encode(encoding="utf-8")


# ----


class LDIdRef(MetadataSchema):
    """Object with just an @id reference (more info is given elsewhere)."""

    class Config:
        extra = Extra.forbid

    id_: Annotated[str, Field(alias="@id", min_length=1)]


class LDSchema(MetadataSchema):
    """Semantically enriched schema for JSON-LD."""

    id_: Annotated[Optional[str], Field(alias="@id", min_length=1)]

    def ref(self) -> LDIdRef:
        """Return LDIdRef, i.e. a pure @id reference for object."""
        return LDIdRef(id_=self.id_)
