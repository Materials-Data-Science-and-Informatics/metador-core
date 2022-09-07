"""Useful types and validators for use in pydantic models."""

import re
from typing import Tuple

import isodate
import wrapt
from phantom.re import FullMatch
from pint import Quantity, UndefinedUnitError, Unit
from pydantic import NonNegativeInt

from ..hashutils import _hash_alg
from .encoder import json_encoder
from .parser import BaseParser, ParserMixin

SemVerTuple = Tuple[NonNegativeInt, NonNegativeInt, NonNegativeInt]
"""Type to be used for SemVer triples."""

# ----


# we use that instead of NonEmpty[str] so we can subclass more flexibly:
class NonEmptyStr(FullMatch, pattern=".+"):
    """Non-empty string."""


class MimeTypeStr(NonEmptyStr, pattern=r"[^ /;]+/[^ /;]+(;[^ /;]+)*"):
    """String that looks like a mime-type."""


class HashsumStr(NonEmptyStr, pattern="[0-9a-fA-F]+"):
    """String that looks like a hashsum."""


_hashalg_regex = f"(?:{'|'.join(_hash_alg.keys())})"


class QualHashsumStr(HashsumStr, pattern=_hashalg_regex + r":[0-9a-fA-F]+"):
    """Hashsum string, prepended by the used algorithm."""


# ----


class Number(ParserMixin, wrapt.ObjectProxy):
    """Whole or floating point number.

    Use this instead of Union[float, int] to get an int if there is no decimal.

    NEVER use Union[int, float], because you will lose the decimal part of any float!
    """

    class Parser(BaseParser):
        strict = False  # will return int or float, not a Number

        schema_info = dict(
            title="An integer or floating point number.",
            type="number",
        )

        @classmethod
        def parse(cls, _, v):
            if isinstance(v, int) or isinstance(v, float):
                return v
            if isinstance(v, str):
                if re.match("^[0-9]+$", v.strip()):
                    return int(v)
                else:
                    return float(v)
            raise TypeError("Expected int, float or str")

    def __repr__(self):
        return repr(self.__wrapped__)


@json_encoder(isodate.duration_isoformat)
class Duration(ParserMixin, isodate.Duration):
    """ISO 8601 Duration."""

    class Parser(BaseParser):
        schema_info = dict(
            title="string in ISO 8601 duration format",
            type="string",
            examples=["PT3H4M1S"],
        )

        @classmethod
        def parse(cls, tcls, v):
            if not isinstance(v, (str, tcls)):
                raise TypeError(f"Expected str or Duration, got {type(v)}.")
            # we have to force it into a Duration object,
            # otherwise we get possibly a timedelta back, which we do not want
            # because we want to serialize to the ISO format in both cases
            dur = isodate.parse_duration(v) if isinstance(v, str) else v
            return tcls(seconds=dur.total_seconds())


# Physical units


class StringParser(BaseParser):
    """Parser from string into some target class."""

    @classmethod
    def parse(cls, tcls, v):
        if isinstance(v, tcls):
            return v

        if not isinstance(v, str):
            msg = f"Expected str or {tcls.__name__}, got {type(v)}."
            raise TypeError(msg)

        ret = tcls(v)
        return ret


class PintParser(StringParser):
    """Shared base for `PintUnit` and `PintQuantity`, taking care of exceptions."""

    @classmethod
    def parse(cls, tcls, v):
        if not v:
            msg = f"Got empty string, expected {tcls.__name__}."
            raise ValueError(msg)
        try:
            return super().parse(tcls, v)
        except UndefinedUnitError as e:
            raise ValueError(str(e))


@json_encoder(str)
class PintUnit(ParserMixin, Unit):
    """pydantic-compatible pint.Unit."""

    class Parser(PintParser):
        schema_info = dict(
            title="Physical unit compatible with the Python pint library.",
            type="string",
            examples=["meter * candela", "kilogram / second ** 2"],
        )


@json_encoder(str)
class PintQuantity(ParserMixin, Quantity):
    """pydantic-compatible pint.Quantity."""

    def __new__(cls, *args, **kwargs):
        # hack to make parsing work, for some reason it does not work without this
        # (it does not correctly identify the unit for some reason)
        if kwargs.get("passthrough"):
            return super().__new__(cls, *args)

        ret = Quantity(*args)  # ensure that the quantity is correctly parsed
        # return instance of the subclass:
        return cls(ret.m, ret.u, passthrough=True)

    class Parser(PintParser):
        schema_info = dict(
            title="Physical quantity compatible with the Python pint library.",
            type="string",
            examples=["5 meter * candela", "7.12 kilogram / second ** 2"],
        )
