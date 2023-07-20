"""Common metadata models based on RO-Crate and Schema.org."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from pydantic import parse_obj_as

from ...plugins import schemas
from .. import MetadataSchema
from ..decorators import make_mandatory
from ..parser import BaseParser
from ..types import PintQuantity, PintUnit
from .rocrate import Person
from .schemaorg import Number, QuantitativeValue, Text

# ----


class SIValueParser(BaseParser):
    @classmethod
    def parse(cls, tcls, v) -> SIValue:
        if isinstance(v, str):
            q = parse_obj_as(PintQuantity, v)
            return tcls(value=q.m, unitText=str(q.u))

        # special case: its a quantitative value already,
        # just check unit and repack as SIValue
        if isinstance(v, tcls.__base__):
            v.unitText = str(parse_obj_as(PintUnit, v.unitText or ""))
            return tcls.construct(v.dict())

        # important: parse back serialized data!
        if isinstance(v, dict):
            return tcls.validate(v)

        msg = f"Cannot parse {v} ({type(v).__name__}) into a {tcls.__name__}!"
        raise TypeError(msg)


class SIValue(QuantitativeValue):
    """QuantitativeValue that holds a numerical value given in SI units.

    Uses the `pint` library to parse and normalize the unit.
    """

    value: Number
    Parser = SIValueParser


class NumValue(QuantitativeValue):
    """Quantitative value that can have a unit out of a fixed list."""

    value: Number

    class Parser(BaseParser):
        schema_info: Dict[str, Any] = {}

        allowed_units: List[str] = []
        infer_unit: Optional[str] = None
        require_unit: bool = False

        @classmethod
        def parse(cls, tcls, v):
            if isinstance(v, (int, float)):
                if cls.require_unit:
                    raise ValueError(f"Value '{v}' must have a unit!")
                return tcls.construct(value=v, unitText=cls.infer_unit)

            if isinstance(v, str):
                arr = v.strip().split(maxsplit=1)

            # important to parse back serialized data!
            if isinstance(v, dict):
                v = tcls.__base__.validate(v)  # -> QuantitativeValue

            if isinstance(v, tcls.__base__):  # unpack QuantitativeValue
                def_unit = cls.infer_unit or ""
                arr = (v.value, v.unitText or v.unitCode or def_unit)

            # check that value and unit are valid:

            if len(arr) == 1:  # no unit given?
                if cls.require_unit:
                    raise ValueError(f"Value '{v}' must have a unit!")
                val = parse_obj_as(Number, arr[0])
                # return with inferred unit
                return tcls.construct(value=val, unitText=cls.infer_unit)

            val = parse_obj_as(Tuple[Number, str], arr)
            if cls.allowed_units and not val[1] in cls.allowed_units:
                msg = (
                    f"Invalid unit '{val[1]}', unit must be one of {cls.allowed_units}"
                )
                raise ValueError(msg)
            return tcls.construct(value=val[0], unitText=val[1])


class Pixels(NumValue):
    """Numeric value representing pixels."""

    class Parser(NumValue.Parser):
        allowed_units = ["px"]
        infer_unit = "px"


# ----

FileMeta: Any = schemas.get("core.file", (0, 1, 0))
DirMeta: Any = schemas.get("core.dir", (0, 1, 0))


@make_mandatory("name", "abstract", "dateCreated")
class BibMeta(DirMeta):
    """Minimal bibliographic metadata required for a container."""

    class Plugin:
        name = "core.bib"
        version = (0, 1, 0)

    author: List[Person]
    """List of authors (creators of the actual content)."""

    creator: Person
    """Person who created the container."""


class ImageFileMeta(FileMeta):
    """A rasterized image file with known dimensions.

    Also serves as marker schema for the imagefile widget.
    """

    class Plugin:
        name = "core.imagefile"
        version = (0, 1, 0)

    width: Pixels
    """Width of the image in pixels."""

    height: Pixels
    """Height of the image in pixels."""


# TODO: see https://github.com/Materials-Data-Science-and-Informatics/metador-core/issues/8


class ColumnHeader(MetadataSchema):
    """Table column metadata."""

    name: Text
    """Column title."""

    unit: PintUnit
    """Physical unit for this column."""


class TableMeta(MetadataSchema):
    """Table metadata."""

    class Plugin:
        name = "core.table"
        version = (0, 1, 0)

    name: Text
    """Table title."""

    columns: List[ColumnHeader]
    """List of column descriptions."""
