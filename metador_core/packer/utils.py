"""Various helper functions that packers can use."""

from __future__ import annotations

from json.decoder import JSONDecodeError
from pathlib import Path
from typing import Optional

import h5py
import numpy
from pydantic import ValidationError

from ..container import MetadorContainer, MetadorDataset
from ..harvester import harvest, harvesters
from ..schema import MetadataSchema, schemas
from .types import DirValidationErrors


def _h5_wrap_bytes(bs: bytes):
    """Wrap bytes with numpy.void if non-empty, or return h5py.Empty."""
    # NOTE: need to think what to do in case we provide non-HDF backends
    # should not be provided directly to users! it's an internal detail!
    return numpy.void(bs) if len(bs) else h5py.Empty("b")


def check_metadata_file(path: Path, **kwargs):
    """Check a metadata file, return error object.

    If `required` is set, will add an error if file is missing.

    If `schema` is passed and file exists, will validate the file and log errors.

    Combine both to check that a file does exist and is valid according to a schema.
    """
    required: bool = kwargs.get("required", False)
    schema: MetadataSchema = kwargs.get("schema", None)
    errs = DirValidationErrors()

    exists = path.is_file()
    if required and not exists:
        errs.add(str(path), f"Required metadata file not found: '{path}'")
    if schema is not None and exists:
        try:
            schema.parse_file(path)
        except (JSONDecodeError, ValidationError, FileNotFoundError) as e:
            errs.add(str(path), str(e))

    return errs


FileMeta = schemas["core.file"]


def embed_file(
    container: MetadorContainer,
    container_path: str,
    file_path: Path,
    *,
    metadata: Optional[MetadataSchema] = None,
) -> MetadorDataset:
    """Embed a file, adding minimal generic metadata to it.

    Args:
        container: Container where to embed the file contents
        container_path: Fresh path in container where to place the file
        file_path: Path of an existing file to be embedded
        metadata: If provided, will attach this instead of harvesting defaults.

    Returns:
        Dataset of new embedded file.
    """
    # check container and file
    if container_path in container:
        raise ValueError(f"Path '{container_path}' already exists in container!")
    if not file_path.is_file():
        raise ValueError(f"Path '{file_path}' does not look like an existing file!")

    if not metadata:
        # no metadata given -> harvest minimal information about a file
        hv_file = harvesters["core.file.generic"]
        metadata = harvest(FileMeta, [hv_file(file_path)])

    # check metadata
    if not isinstance(metadata, FileMeta):
        msg = f"Provided metadata is not compatible with '{FileMeta.Plugin.name}'!"
        raise ValueError(msg)
    if not metadata.is_plugin():
        msg = f"Given metadata is a {type(metadata)}, which is not a schema plugin!"
        raise ValueError(msg)

    # embed file and metadata in container:
    data = _h5_wrap_bytes(file_path.read_bytes())
    ret = container.create_dataset(container_path, data=data)
    ret.meta[metadata.Plugin.name] = metadata
    return ret
