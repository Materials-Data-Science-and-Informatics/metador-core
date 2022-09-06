"""Common metadata models based on RO-Crate and Schema.org."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from pydantic import parse_obj_as

from ...plugins import schemas
from .. import MetadataSchema
from ..decorators import make_mandatory
from ..types import ParserMixin, PintQuantity, PintUnit
from .rocrate import Person
from .schemaorg import Number, QuantitativeValue, Text

# ----


class SIValue(ParserMixin, QuantitativeValue):
    """Numerical value in SI physical units."""

    value: Number

    @classmethod
    def parse(cls, v):
        if isinstance(v, str):
            q = PintQuantity.validate(v)
            return cls(value=q.m, unitText=str(q.u))

        # important to parse back serialized data!
        if isinstance(v, dict):
            v = parse_obj_as(cls, v)
        if isinstance(v, QuantitativeValue):
            v.unitText = str(PintUnit.validate(v.unitText or ""))
            return v


class NumValue(ParserMixin, QuantitativeValue):
    """Quantitative value that can have a unit out of a fixed list."""

    value: Number

    class ParserConfig:
        schema: Dict[str, Any] = {}
        allowed_units: List[str] = []
        infer_unit: Optional[str] = None
        require_unit: bool = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__dict__.update(self.parse(self).__dict__)

    @classmethod
    def parse(cls, v):
        conf = cls._parser_config()

        if isinstance(v, int) or isinstance(v, float):
            if conf.require_unit:
                raise ValueError(f"Value '{v}' must have a unit!")
            return cls.construct(value=v, unitText=conf.infer_unit)

        if isinstance(v, str):
            arr = v.strip().split(maxsplit=1)

        # important to parse back serialized data!
        if isinstance(v, dict):
            v = parse_obj_as(QuantitativeValue, v)
        if isinstance(v, QuantitativeValue):
            def_unit = conf.infer_unit or ""
            arr = (v.value, v.unitText or v.unitCode or def_unit)

        if len(arr) == 1:
            if conf.require_unit:
                raise ValueError(f"Value '{v}' must have a unit!")
            val = parse_obj_as(Number, arr[0])
            return cls.construct(value=val, unitText=conf.infer_unit)

        val = parse_obj_as(Tuple[Number, str], arr)
        if conf.allowed_units and not val[1] in conf.allowed_units:
            msg = f"Invalid unit '{val[1]}', unit must be one of {conf.allowed_units}"
            raise ValueError(msg)
        return cls.construct(value=val[0], unitText=val[1])


class Pixels(NumValue):
    """Numeric value representing pixels."""

    class ParserConfig:
        allowed_units = ["px"]
        infer_unit = "px"


# ----

FileMeta: Any = schemas["core.file"]
DirMeta: Any = schemas["core.dir"]


@make_mandatory("name", "abstract", "dateCreated")
class BibMeta(DirMeta):
    """Minimal bibliographic metadata required for a container."""

    class Plugin:
        name = "core.bib"
        version = (0, 1, 0)
        parent_schema = DirMeta.Plugin.ref(version=(0, 1, 0))

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
        parent_schema = FileMeta.Plugin.ref(version=(0, 1, 0))

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
