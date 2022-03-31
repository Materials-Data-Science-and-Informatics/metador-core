"""Metadata models for Ardiem records."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import List, Optional, Union

import magic
from pint import Unit
from pint.errors import UndefinedUnitError
from pydantic import AnyHttpUrl, BaseModel, Field
from typing_extensions import Annotated, Literal

from .hashutils import HASH_ALG, file_hashsum

nonempty_str = Annotated[str, Field(min_length=1)]

# rough regex checking a string looks like a mime-type
mimetype_str = Annotated[str, Field(regex=r"^\S+/\S+(;\S+)*$")]

# a hashsum string is to be prepended by the used algorithm
hashsum_str = Annotated[str, Field(regex=r"^" + HASH_ALG + r":\w+$")]


class PintUnit:
    """Pydantic validator for serialized physical units that can be parsed by pint."""

    # https://pydantic-docs.helpmanual.io/usage/types/#custom-data-types

    UNIT_REGEX = r"^[\w */]+$"
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


class ExtBaseModel(BaseModel):
    """Extended base model with custom serializers."""

    # https://pydantic-docs.helpmanual.io/usage/exporting_models/#json_encoders

    class Config:
        json_encoders = {Unit: lambda x: str(x)}


class PackerMeta(ExtBaseModel):
    """Metadata of the packer that created some record."""

    id: nonempty_str
    """Unique identifier of the packer (the same as the entry-point name)."""

    version: nonempty_str
    """Version of the packer (can be different from version of the Python package)."""

    docs: Optional[AnyHttpUrl] = None
    """Documentation of the packer (possibly location within more general package docs)."""

    python_package: Optional[nonempty_str] = None
    """Python package name (i.e. what to `pip install`, if published on PyPI)."""

    python_source: Optional[AnyHttpUrl] = None
    """Python package source location (e.g. online-hosted git repository)."""

    uname: List[str] = []
    """Environment of packer during execution (os.uname() without the hostname)."""

    @staticmethod
    def get_uname() -> List[str]:
        """Get minimal information about the system to be put in the uname attribute."""
        un = os.uname()
        # remove nodename (too private)
        return [un.sysname, un.release, un.version, un.machine]


# metadata records stored in node_meta attribute inside record


class FileMeta(ExtBaseModel):
    """Metadata to be provided for each embedded file.

    The type is preferably given as mime-type, otherwise implied py file extension.
    For previews, the shown title is the provided title, otherwise the file name.
    """

    type: Literal["file"]
    filename: nonempty_str
    hashsum: hashsum_str
    mimetype: Optional[mimetype_str] = None
    title: Optional[nonempty_str] = None

    @classmethod
    def from_file(cls, path: Path) -> FileMeta:
        """Return expected metadata for a file.

        Will compute its hashsum and try to detect the MIME type.
        Title will be left empty.
        """
        return FileMeta(
            type="file",
            filename=path.name,
            hashsum=file_hashsum(path, HASH_ALG),
            mimetype=magic.from_file(path, mime=True),
        )


class ColumnHead(ExtBaseModel):
    title: nonempty_str
    unit: PintUnit


class TableMeta(ExtBaseModel):
    type: Literal["table"]
    title: nonempty_str
    columns: List[ColumnHead]


class NodeMeta(ExtBaseModel):
    """Metadata attached to a node (group or HDF5 record)."""

    __root__: Union[FileMeta, TableMeta] = Field(..., discriminator="type")
