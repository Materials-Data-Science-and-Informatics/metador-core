"""Metadata models for Ardiem records."""

from __future__ import annotations

import json
import os
from json.decoder import JSONDecodeError
from pathlib import Path
from typing import List, Optional, Tuple, Union

import h5py
import magic
from pydantic import AnyHttpUrl, BaseModel, Field, NonNegativeInt, ValidationError
from typing_extensions import Literal

from .hashutils import HASH_ALG, file_hashsum
from .ih5.record import IH5Record
from .packer import ArdiemValidationErrors
from .types import PintUnit, hashsum_str, mimetype_str, nonempty_str


class ArdiemBaseModel(BaseModel):
    """Extended base model with custom serializers and functions."""

    # https://pydantic-docs.helpmanual.io/usage/exporting_models/#json_encoders

    class Config:
        json_encoders = {PintUnit.Parsed: lambda x: str(x)}

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
            val = rec["/"].value_get(path)
            if isinstance(val, h5py.Empty):
                return None
            else:
                return cls.parse_obj(json.loads(val))
        except ValueError:
            raise ArdiemValidationErrors({path: ["Expected JSON, found a group!"]})
        except (TypeError, JSONDecodeError):
            msg = f"Cannot parse {type(val).__name__} as JSON!"
            raise ArdiemValidationErrors({path: [msg]})
        except (ValidationError, FileNotFoundError) as e:
            raise ArdiemValidationErrors({path: [str(e)]})


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


class FileMeta(ArdiemBaseModel):
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


class ColumnHead(ArdiemBaseModel):
    title: nonempty_str
    unit: PintUnit


class TableMeta(ArdiemBaseModel):
    type: Literal["table"]
    title: nonempty_str
    columns: List[ColumnHead]


class NodeMeta(ArdiemBaseModel):
    """Metadata attached to a node (group or HDF5 record)."""

    __root__: Union[FileMeta, TableMeta] = Field(..., discriminator="type")
