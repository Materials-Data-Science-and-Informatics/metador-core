"""Metadata models for previewable entities in Metador containers."""

from __future__ import annotations

from pathlib import Path
from typing import List, Optional

import magic
from PIL import Image

from ..hashutils import file_hashsum
from .interface import MetadataSchema, schema_ref
from .types import PintUnit, hashsum_str, mimetype_str, nonempty_str


class FileMeta(MetadataSchema):
    """Metadata to be provided for each embedded file.

    The type is preferably given as mime-type, otherwise implied py file extension.
    For previews, the shown title is the provided title, otherwise the file name.
    """
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
            filename=path.name,
            hashsum=file_hashsum(path),
            mimetype=magic.from_file(path, mime=True),
        )


class ImageMeta(FileMeta):
    # dimensions in pixels
    width: int
    height: int

    @classmethod
    def parent_schema(cls):
        return schema_ref("common_file")

    @classmethod
    def for_file(cls, path: Path) -> FileMeta:
        """Generate and return expected metadata for a file.

        Will compute its hashsum and try to detect the MIME type.
        Title will be left empty.
        """
        with Image.open(path) as img:
            width, height = img.size
        return ImageMeta(
            filename=path.name,
            hashsum=file_hashsum(path),
            mimetype=magic.from_file(path, mime=True),
            width=width,
            height=height,
        )


class ColumnHead(MetadataSchema):
    title: nonempty_str
    unit: PintUnit


class TableMeta(FileMeta):

    @classmethod
    def parent_schema(cls):
        return schema_ref("common_file")

    columns: List[ColumnHead]
