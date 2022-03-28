"""Metadata models for Ardiem datasets."""

from tokenize import TokenError
from typing import List, Optional, Union

from pint import Unit, get_application_registry
from pint.errors import UndefinedUnitError
from pydantic import BaseModel, Field
from typing_extensions import Annotated, Literal

nonempty_str = Annotated[str, Field(min_length=1)]

# rough regex checking a string looks like a mime-type
mimetype_regex = r"^\S+/\S+(;\S+)*$"
mimetype_str = Annotated[str, Field(regex=mimetype_regex)]


class PintUnit(str):
    """Pydantic validator for serialized physical units that can be parsed by pint."""

    # https://pydantic-docs.helpmanual.io/usage/types/#custom-data-types

    # TODO: do we want to allow empty string as "dimensionless"?

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def __modify_schema__(cls, field_schema):
        field_schema.update(
            title="Physical unit string compatible with the Python pint library.",
            type="string",
            examples=["meter * candela", "kilogram / second ** 2"],
        )

    @classmethod
    def validate(cls, v) -> Unit:
        if not isinstance(v, str):
            raise TypeError("Expected unit as string.")
        try:
            return get_application_registry().parse_expression(v).units
        except (TokenError, UndefinedUnitError):
            raise ValueError(f"Could not parse unit: {v}")


class File(BaseModel):
    type: Literal["file"]
    filename: nonempty_str
    mimetype: Optional[mimetype_str]
    title: Optional[nonempty_str]


class ColumnHead(BaseModel):
    title: nonempty_str
    unit: PintUnit


class Table(BaseModel):
    type: Literal["table"]
    title: nonempty_str
    columns: List[ColumnHead]


class Node(BaseModel):
    __root__: Union[File, Table] = Field(..., discriminator="type")
