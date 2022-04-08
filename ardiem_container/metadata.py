"""Metadata models for Ardiem records."""

from __future__ import annotations

import os
from enum import Enum
from json.decoder import JSONDecodeError
from pathlib import Path
from typing import List, Optional, Tuple, Union

import h5py
import magic
from pydantic import AnyHttpUrl, BaseModel, Field, NonNegativeInt, ValidationError
from pydantic_yaml import YamlModelMixin
from typing_extensions import Literal

from .hashutils import HASH_ALG, file_hashsum
from .ih5.record import IH5Group, IH5Record
from .packer import ArdiemValidationErrors
from .types import PintUnit, hashsum_str, mimetype_str, nonempty_str


class ArdiemBaseModel(YamlModelMixin, BaseModel):
    """Extended base model with custom serializers and functions."""

    # https://pydantic-docs.helpmanual.io/usage/exporting_models/#json_encoders

    class Config:
        use_enum_values = True  # to serialize enums properly
        json_encoders = {PintUnit.Parsed: lambda x: str(x)}  # for SI units

    @classmethod
    def from_file(cls, path: Path):
        """Get YAML- or JSON-serialized metadata from a file as a model instance.

        If the path is not existing or cannot be parsed, will raise ArdiemValidationErrors.
        Otherwise, will return the parsed model instance.
        """
        errs = ArdiemValidationErrors()
        try:
            return cls.parse_file(path)
        except (JSONDecodeError, ValidationError, FileNotFoundError) as e:
            errs.add(str(path), str(e))
            raise errs

    @classmethod
    def from_record(cls, rec: IH5Record, path: str):
        """Get JSON-serialized metadata from a record as a model instance.

        If the path is not existing or cannot be parsed, will raise ArdiemValidationErrors.
        If the path exists, but is a stub value, will return None.
        Otherwise, will return the parsed model instance.
        """
        if path not in rec:
            raise ArdiemValidationErrors({path: ["not found!"]})
        val = None
        try:
            val = rec["/"].at(path)
            if isinstance(val, IH5Group):
                raise ArdiemValidationErrors({path: ["Expected JSON, found a group!"]})
            if isinstance(val, h5py.Empty):
                return None
            else:
                return cls.parse_raw(val, content_type="application/json")
        except (TypeError, JSONDecodeError) as e:
            msg = f"Cannot parse {type(val).__name__} as JSON: {str(e)}"
            raise ArdiemValidationErrors({path: [msg]})
        except ValidationError as e:
            raise ArdiemValidationErrors({path: [str(e)]})

    @classmethod
    def from_path(cls, path: Union[Path, str], record: Optional[IH5Record] = None):
        """Read instance from a file path or from a path relative to a given record.

        JSON and YAML supported for file paths,
        only JSON allowed from record datasets and attributes.

        Wraps `from_record` and `from_file` in a unified function.
        """
        if record:
            return cls.from_record(record, str(path))
        else:
            return cls.from_file(Path(path))

    @classmethod
    def check_path(cls, path: Union[Path, str], record: Optional[IH5Record] = None):
        """Check instance at a file path or a path inside a given record.

        JSON and YAML supported for file paths,
        only JSON allowed from record datasets and attributes.

        Will treat `h5py.Empty` as valid metadata (in order to work for stub records).

        Returns errors if any.
        """
        try:
            cls.from_path(path, record=record)
        except ArdiemValidationErrors as e:
            return e
        return ArdiemValidationErrors()


class PackerMeta(ArdiemBaseModel):
    """Metadata of the packer that created some record."""

    id: nonempty_str
    """Unique identifier of the packer (the same as the entry-point name)."""

    version: Tuple[NonNegativeInt, NonNegativeInt, NonNegativeInt]
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


class NodeMetaTypes(str, Enum):
    file = "file"
    table = "table"


class FileMeta(ArdiemBaseModel):
    """Metadata to be provided for each embedded file.

    The type is preferably given as mime-type, otherwise implied py file extension.
    For previews, the shown title is the provided title, otherwise the file name.
    """

    type: Literal[NodeMetaTypes.file]
    filename: nonempty_str
    hashsum: hashsum_str
    mimetype: Optional[mimetype_str] = None
    title: Optional[nonempty_str] = None

    @classmethod
    def for_file(cls, path: Path) -> FileMeta:
        """Generate and return expected metadata for a file.

        Will compute its hashsum and try to detect the MIME type.
        Title will be left empty.
        """
        return FileMeta(
            type=NodeMetaTypes.file,
            filename=path.name,
            hashsum=file_hashsum(path, HASH_ALG),
            mimetype=magic.from_file(path, mime=True),
        )


class ColumnHead(ArdiemBaseModel):
    title: nonempty_str
    unit: PintUnit


class TableMeta(ArdiemBaseModel):
    type: Literal[NodeMetaTypes.table]
    title: nonempty_str
    columns: List[ColumnHead]


class NodeMeta(ArdiemBaseModel):
    """Metadata attached to a node (group or HDF5 record)."""

    __root__: Union[FileMeta, TableMeta] = Field(..., discriminator="type")
