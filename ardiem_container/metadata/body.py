"""Metadata models for previewable entities in Ardiem records."""

from __future__ import annotations

from pathlib import Path
from typing import List, Optional

import magic
from PIL import Image
from typing_extensions import Literal

from ..hashutils import HASH_ALG, file_hashsum
from .base import ArdiemBaseModel
from .types import PintUnit, hashsum_str, mimetype_str, nonempty_str


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
    def for_file(cls, path: Path) -> FileMeta:
        """Generate and return expected metadata for a file.

        Will compute its hashsum and try to detect the MIME type.
        Title will be left empty.
        """
        return FileMeta(
            type="file",
            filename=path.name,
            hashsum=file_hashsum(path, HASH_ALG),
            mimetype=magic.from_file(path, mime=True),
        )


class ImageMeta(FileMeta):
    type: Literal["image"]  # type: ignore

    # dimensions in pixels
    width: int
    height: int

    @classmethod
    def for_file(cls, path: Path) -> FileMeta:
        """Generate and return expected metadata for a file.

        Will compute its hashsum and try to detect the MIME type.
        Title will be left empty.
        """
        with Image.open(path) as img:
            width, height = img.size
        return ImageMeta(
            type="image",
            filename=path.name,
            hashsum=file_hashsum(path, HASH_ALG),
            mimetype=magic.from_file(path, mime=True),
            width=width,
            height=height,
        )


class ColumnHead(ArdiemBaseModel):
    title: nonempty_str
    unit: PintUnit


class TableMeta(ArdiemBaseModel):
    type: Literal["table"]

    title: nonempty_str
    columns: List[ColumnHead]
