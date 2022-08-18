"""Useful types and validators for use in pydantic models."""

import datetime
import re
from typing import _GenericAlias  # type: ignore
from typing import Tuple

import isodate
from pint import Quantity, UndefinedUnitError, Unit
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


class ParserMixin:
    """Base mixin class to simplify creation of custom pydantic field types."""

    class ParserConfig:
        _loaded = True

    @classmethod
    def _parser_config(cls, base=None):
        base = base or cls
        try:
            conf = base.ParserConfig
        except AttributeError:
            # this class has no own parser config
            # -> look up in base class and push down
            conf = cls._parser_config(base)
            setattr(base, "ParserConfig", conf)

        # config is initialized -> return it
        if getattr(conf, "_loaded", False):
            return conf

        # find next upper parser config
        nxt = list(filter(lambda c: issubclass(c, ParserMixin), base.mro()))[1]
        if nxt is None:
            raise RuntimeError("Did not find base ParserConfig")

        base_conf = base._parser_config(nxt)
        for key, value in base_conf.__dict__.items():
            if key[0] == "_":
                continue
            if not hasattr(conf, key):
                setattr(conf, key, value)

        setattr(conf, "_loaded", True)
        return conf

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def __modify_schema__(cls, schema):
        schema.update(**cls._parser_config().schema)

    @classmethod
    def validate(cls, v):
        """Validate passed value and return as declared `Parsed` type."""
        try:
            return cls.parse(v)
        except ValueError as e:
            msg = f"Could not parse {cls.__name__} value {v}: {str(e)}"
            raise ValueError(msg)


# ----
# Some generally useful pydantic validators.


class Duration(ParserMixin):
    """Parser from str -> isodate.Duration."""

    class ParserConfig:
        schema = {
            "title": "string in ISO 8601 duration format",
            "type": "string",
            "examples": ["PT3H4M1S"],
        }

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


class StringParser(ParserMixin):
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
    @classmethod
    def parse(cls, v):
        if not v:
            raise ValueError("Got empty string, expected number.")
        try:
            super().parse(v)
        except UndefinedUnitError as e:
            raise ValueError(str(e))


class PintUnit(PintParser, Unit):
    """pydantic-suitable pint.Unit."""

    class ParserConfig:
        parser = Unit
        returns = Unit

        schema = {
            "title": "Physical unit compatible with the Python pint library.",
            "type": "string",
            "examples": ["meter * candela", "kilogram / second ** 2"],
        }


class PintQuantity(PintParser, Quantity):
    """pydantic-suitable pint.Quantity."""

    class ParserConfig:
        parser = Quantity
        returns = Quantity

        schema = {
            "title": "Physical quantity compatible with the Python pint library.",
            "type": "string",
            "examples": ["5 meter * candela", "7.12 kilogram / second ** 2"],
        }
