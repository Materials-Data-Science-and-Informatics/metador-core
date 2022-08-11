"""Common metadata models based on RO-Crate and Schema.org."""

from __future__ import annotations

from typing import List, Optional, Tuple, Type, Union

from pydantic import parse_obj_as

from .. import MetadataSchema
from ..plugingroup import SchemaPlugin
from ..types import PintQuantity, PintUnit, SimpleValidator
from .rocrate import DirMeta, FileMeta, Organization, Person
from .schemaorg import Number, QuantitativeValue, Text

# ----


class NumericQuantity(QuantitativeValue):
    """Quantitative value that is a number."""

    value: Number


class QuantityValidator(SimpleValidator):
    Parsed = NumericQuantity


def quantity(
    *,
    allowed_units: List[str] = [],
    infer_unit: Optional[str] = None,
    require_unit: bool = False,
) -> Type[QuantityValidator]:
    """Return validator class to parse a quantity into a `QuantitativeValue`.

    The parser will handle a value given as number, string or [number, string].

    To be used as type annotation in models.

    Args:
        allowed_units: List of unit strings that should be accepted
        infer_unit: If the unit is missing, the infer_unit will be set as unit.
        require_unit: If set, will fail validation if no unit is provided.
    """
    examples = [42, 3.14]
    if allowed_units:
        some_unit = allowed_units[0]
        examples.append(f'"5 {some_unit}"')  # type: ignore
        examples.append(f'[5,"{some_unit}"]')  # type: ignore

    class GenQuantityValidator(QuantityValidator):
        _field_infos = {
            "title": f"Parser for quantity of a unit in {allowed_units}.",
            "type": ("number", "array", "string"),
            "examples": examples,
        }

        @classmethod
        def parse(cls, v):
            if isinstance(v, int) or isinstance(v, float):
                if require_unit:
                    raise ValueError(f"Value '{v}' must have a unit!")
                return NumericQuantity(value=v, unitText=infer_unit)

            if isinstance(v, str):
                arr = v.strip().split(maxsplit=1)

                # important to parse back serialized data!
            if isinstance(v, dict):
                v = parse_obj_as(QuantitativeValue, v)
            if isinstance(v, QuantitativeValue):
                arr = (v.value, v.unitText or v.unitCode or "")

            if len(arr) == 1:
                if require_unit:
                    raise ValueError(f"Value '{v}' must have a unit!")
                val = parse_obj_as(Union[float, int], arr[0])
                return NumericQuantity(value=val, unitText=infer_unit)

            val = parse_obj_as(Tuple[Union[float, int], str], arr)
            if allowed_units and not val[1] in allowed_units:
                msg = f"Invalid unit '{val[1]}', unit must be one of {allowed_units}"
                raise ValueError(msg)
            return NumericQuantity(value=val[0], unitText=val[1])

    GenQuantityValidator.__name__ = f"Quantity({allowed_units})"

    return GenQuantityValidator  # type: ignore


AnyNumericQuantity = quantity()
"""A numeric value with any unit or without a unit.

Should not be used for quantities that have a corresponding unit.

That means, only use this for dimensionless numeric values.
"""


class SIValue(SimpleValidator):
    """Validator to parse a quantity in SI physical units into a `QuantitativeValue`.

    To be used as type annotation in models.

    For other quantities, use `quantity`.
    """

    Parsed = NumericQuantity
    _field_infos = PintQuantity._field_infos

    @classmethod
    def parse(cls, v):
        q = PintQuantity.validate(v)
        return cls.Parsed(value=q.m, unitText=str(q.u))


# ----


class BibMeta(DirMeta):
    """Minimal bibliographic metadata required for a container."""

    class Plugin(SchemaPlugin):
        name = "core.bib"
        version = (0, 1, 0)
        parent_schema = DirMeta.Plugin.ref(version=(0, 1, 0))

    name: Text
    """Title of dataset."""

    description: Text
    """Description of dataset."""

    author: List[Union[Person, Organization]]
    """List of authors of dataset."""


Pixels = quantity(allowed_units=["px"], infer_unit="px")
"""Numeric value representing pixels."""


class ImageFileMeta(FileMeta):
    """A rasterized image file with known dimensions.

    Also serves as marker schema for the imagefile widget.
    """

    class Plugin(SchemaPlugin):
        name = "core.imagefile"
        version = (0, 1, 0)
        parent_schema = FileMeta.Plugin.ref(version=(0, 1, 0))

    width: Pixels  # type: ignore
    height: Pixels  # type: ignore


class ColumnHeader(MetadataSchema):
    name: Text
    unit: PintUnit


class TableMeta(MetadataSchema):
    """Metadata about a table."""

    class Plugin(SchemaPlugin):
        name = "core.table"
        version = (0, 1, 0)

    name: Text
    columns: List[ColumnHeader]
