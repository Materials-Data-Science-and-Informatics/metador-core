"""Useful types and validators for use in pydantic models."""

import re
from typing import _GenericAlias  # type: ignore
from typing import Any, Dict, Tuple, Type

import isodate
from pint import Quantity, Unit
from pydantic import Field, NonNegativeInt
from typing_extensions import Annotated, Literal, TypedDict

from ..hashutils import _hash_alg

nonempty_str = Annotated[str, Field(min_length=1)]

# rough regex checking a string looks like a mime-type
mimetype_str = Annotated[str, Field(regex=r"^[^ /;]+/[^ /;]+(;[^ /;]+)*$")]

# a hashsum string is to be prepended by the used algorithm
_hashalg_regex = f"(?:{'|'.join(_hash_alg.keys())})"
hashsum_str = Annotated[str, Field(regex=r"^" + _hashalg_regex + r":[0-9a-fA-F]+$")]

SemVerTuple = Tuple[NonNegativeInt, NonNegativeInt, NonNegativeInt]
"""Type to be used for SemVer triples."""


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


class SimpleValidator:
    """Simple validator base class for custom data types.

    See: https://pydantic-docs.helpmanual.io/usage/types/#custom-data-types
    """

    Parsed: Type[Any] = str
    """Type stored in the model after validation by this class.

    Override this in subclasses.
    """

    _field_infos: Dict[str, Any] = {}
    """Infos added to JSON Schema when the value is serialized again.

    Override this in subclasses to add helpful information.
    """

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def __modify_schema__(cls, field_schema):
        field_schema.update(**cls._field_infos)

    @classmethod
    def parse(cls, v):
        """Parse passed string value and return as declared `Parsed` type.

        Default implementation will parse a string by calling `Parsed(v)`,
        override this for a custom parsing function.
        """
        if not isinstance(v, str):
            raise TypeError(f"Expected {cls.Parsed.__name__} or str, got {type(v)}.")
        return cls.Parsed(v)

    @classmethod
    def validate(cls, v):
        """Validate passed value and return as declared `Parsed` type."""
        if isinstance(v, cls.Parsed):
            return v
        try:
            return cls.parse(v)
        except Exception as e:
            msg = f"Could not parse {cls.__name__} value {v}: {str(e)}"
            raise ValueError(msg)


# ----
# Some useful pydantic validators.


class Duration(SimpleValidator):
    """Parser from str -> isodate.Duration."""

    Parsed = isodate.Duration
    _field_infos = {
        "title": "string in ISO 8601 duration format",
        "type": "string",
        "examples": ["PT3H4M1S"],
    }

    @classmethod
    def parse(cls, v):
        if not isinstance(v, str):
            raise TypeError(f"Expected {cls.Parsed.__name__} or str, got {type(v)}.")
        return isodate.parse_duration(v)


def pint_validator(pcls, **field_infos):
    class PintValidator(SimpleValidator):
        Parsed = pcls
        _field_infos = field_infos
        REGEX = r"^[0-9\w ()*/]+$"

        @classmethod
        def parse(cls, v):
            if not isinstance(v, str):
                raise TypeError(
                    f"Expected {cls.Parsed.__name__} or str, got {type(v)}."
                )
            if re.match(cls.REGEX, v) is None:
                raise ValueError(f"Invalid {cls.Parsed.__name__}: {v}")
            return super().parse(v)

    PintValidator.__name__ = f"Pint{pcls.__name__}"
    return PintValidator


PintUnit = pint_validator(
    Unit,
    title="Physical unit compatible with the Python pint library.",
    type="string",
    examples=["meter * candela", "kilogram / second ** 2"],
)
"""Parser from str -> pint.Unit."""

PintQuantity = pint_validator(
    Quantity,
    title="Physical quantity compatible with the Python pint library.",
    type="string",
    examples=["5 meter * candela", "7.12 kilogram / second ** 2"],
)
"""Parser from str -> pint.Quantity."""
