"""Useful types and validators for use in pydantic models."""

import datetime
import re
from typing import Tuple

import isodate
from phantom.re import FullMatch
from pint import Quantity, UndefinedUnitError, Unit
from pydantic import NonNegativeInt

from ..hashutils import _hash_alg
from .utils import ParserMixin

SemVerTuple = Tuple[NonNegativeInt, NonNegativeInt, NonNegativeInt]
"""Type to be used for SemVer triples."""


class NonEmptyStr(FullMatch, pattern=".+"):
    """Non-empty string."""

    # use that instead of NonEmpty[str] so we can subclass more flexibly


class MimeType(NonEmptyStr, pattern=r"[^ /;]+/[^ /;]+(;[^ /;]+)*"):
    """String that looks like a mime-type."""


class HashsumStr(NonEmptyStr, pattern="[0-9a-fA-F]+"):
    """String that looks like a hashsum."""


_hashalg_regex = f"(?:{'|'.join(_hash_alg.keys())})"


class QualHashsumStr(HashsumStr, pattern=_hashalg_regex + r":[0-9a-fA-F]+"):
    """Hashsum string, prepended by the used algorithm."""


class Duration(ParserMixin):
    """Parser from str -> isodate.Duration."""

    class ParserConfig:
        schema = {
            "title": "string in ISO 8601 duration format",
            "type": "string",
            "examples": ["PT3H4M1S"],
        }

    def __new__(cls, v):
        return cls.parse(v)

    @classmethod
    def parse(cls, v):
        if isinstance(v, datetime.timedelta):
            return v
        if isinstance(v, str):
            return isodate.parse_duration(v)
        raise TypeError(f"Expected timedelta or str, got {type(v)}.")


class Number(ParserMixin):
    """Whole or floating point number."""

    class ParserConfig:
        schema = {
            "title": "An integer or floating point number.",
            "type": "number",
        }

    def __new__(cls, v):
        return cls.parse(v)

    @classmethod
    def parse(cls, v):
        if isinstance(v, int) or isinstance(v, float):
            return v
        if isinstance(v, str):
            if re.match("^[0-9]+$", v.strip()):
                return int(v)
            else:
                return float(v)
        raise TypeError("Expected int, float or str")


# Physical units


class StringParser(ParserMixin):
    """Mixin that should handle a plain string as user input."""

    class ParserConfig:
        def identity(x):
            return x

        parser = identity
        returns: str

    @classmethod
    def parse(cls, v):
        conf = cls._parser_config()
        rcls = conf.returns
        if isinstance(v, rcls):
            return v

        if not isinstance(v, str):
            msg = f"Expected {rcls} or str, got {type(v)}."
            raise TypeError(msg)

        return conf.parser(v)


class PintParser(StringParser):
    """Shared mixin for `PintUnit` and `PintQuantity`."""

    @classmethod
    def parse(cls, v):
        if not v:
            msg = f"Got empty string, expected {cls.ParserConfig.parser.__name__}."
            raise ValueError(msg)
        try:
            return super().parse(v)
        except UndefinedUnitError as e:
            raise ValueError(str(e))


class PintUnit(PintParser, Unit):
    """pydantic-compatible pint.Unit."""

    class ParserConfig:
        parser = Unit
        returns = Unit

        schema = {
            "title": "Physical unit compatible with the Python pint library.",
            "type": "string",
            "examples": ["meter * candela", "kilogram / second ** 2"],
        }


class PintQuantity(PintParser, Quantity):
    """pydantic-compatible pint.Quantity."""

    class ParserConfig:
        parser = Quantity
        returns = Quantity

        schema = {
            "title": "Physical quantity compatible with the Python pint library.",
            "type": "string",
            "examples": ["5 meter * candela", "7.12 kilogram / second ** 2"],
        }
