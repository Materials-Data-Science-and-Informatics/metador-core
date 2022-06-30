"""Useful types to use in pydantic models."""

import re

from pint import Unit
from pint.errors import UndefinedUnitError
from pydantic import Field, NonNegativeInt
from typing import Tuple
from typing_extensions import Annotated

from ..hashutils import _hash_alg

nonempty_str = Annotated[str, Field(min_length=1)]

# rough regex checking a string looks like a mime-type
mimetype_str = Annotated[str, Field(regex=r"^[^ /;]+/[^ /;]+(;[^ /;]+)*$")]

# a hashsum string is to be prepended by the used algorithm
_hashalg_regex = f"(?:{'|'.join(_hash_alg.keys())})"
hashsum_str = Annotated[str, Field(regex=r"^" + _hashalg_regex + r":[0-9a-fA-F]+$")]

SemVerTuple = Tuple[NonNegativeInt, NonNegativeInt, NonNegativeInt]


class PintUnit:
    """Pydantic validator for serialized physical units that can be parsed by pint."""

    # https://pydantic-docs.helpmanual.io/usage/types/#custom-data-types

    Parsed = Unit
    """Type stored in the model after validation by this class."""

    UNIT_REGEX = r"^[\w ()*/]+$"
    """Units must be expressed out of:
    words, spaces, multiplication, division, exponentiation."""

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
        if isinstance(v, Unit):
            return v
        if not isinstance(v, str):
            raise TypeError("Expected unit as Unit or str.")
        if re.match(cls.UNIT_REGEX, v) is None:
            raise ValueError(f"Invalid unit: {v}")
        try:
            return Unit(v)
        except (ValueError, UndefinedUnitError):
            raise ValueError(f"Could not parse unit: {v}")
