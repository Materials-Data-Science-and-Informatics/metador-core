"""Useful types and validators for use in pydantic models."""
from __future__ import annotations

import isodate
from phantom.re import FullMatch
from pint import Quantity, UndefinedUnitError, Unit
from pydantic import StrictBool, StrictBytes, StrictFloat, StrictInt, StrictStr
from typing_extensions import TypeAlias

from ..plugin.types import SemVerTuple, from_semver_str, to_semver_str
from ..util.hashsums import _hash_alg
from .encoder import json_encoder
from .parser import BaseParser, ParserMixin

# ----

# we want people to use the strict types, they should be default.
# coercing / normalization should be done explicitly!
# before, we needed to use fixes from https://github.com/pydantic/pydantic/issues/2329
# with pydantic 1.10.2 we can just pass through to the strict types

Bool: TypeAlias = StrictBool
Int: TypeAlias = StrictInt
Float: TypeAlias = StrictFloat
Str: TypeAlias = StrictStr
Bytes: TypeAlias = StrictBytes

# ----

# we prefer this over pydantic anystr config settings (non-local) and
# we use that instead of NonEmpty[str] because we can also react to whitespace
# and we can subclass it:


class NonEmptyStr(FullMatch, pattern=r"\s*\S[\S\s]*"):
    """Non-empty string (contains non-whitespace characters)."""


class MimeTypeStr(NonEmptyStr, pattern=r"[^ /;]+/[^ /;]+(;[^ /;]+)*"):
    """String that looks like a mime-type."""


class HashsumStr(NonEmptyStr, pattern="[0-9a-fA-F]+"):
    """String that looks like a hashsum."""


_hashalg_regex = f"(?:{'|'.join(_hash_alg.keys())})"


class QualHashsumStr(HashsumStr, pattern=_hashalg_regex + r":[0-9a-fA-F]+"):
    """Hashsum string, prepended by the used algorithm."""


# ----


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


__all__ = [
    "SemVerTuple",
    "to_semver_str",
    "from_semver_str",
    "Bool",
    "Int",
    "Float",
    "Bytes",
    "Str",
    "NonEmptyStr",
    "MimeTypeStr",
    "HashsumStr",
    "QualHashsumStr",
    "Duration",
    "StringParser",
    "PintParser",
    "PintUnit",
    "PintQuantity",
]
