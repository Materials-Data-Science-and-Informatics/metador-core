"""Various helper functions that packers can use."""

from __future__ import annotations

import urllib.parse
from json.decoder import JSONDecodeError
from pathlib import Path
from typing import Optional, Union

import h5py
import numpy
from pydantic import ValidationError

from ..container import MetadorContainer, MetadorDataset, MetadorGroup
from ..harvester import harvest
from ..plugins import harvesters, schemas
from ..schema import MetadataSchema
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


FileMeta = schemas.get("core.file", (0, 1, 0))


def embed_file(
    node: Union[MetadorContainer, MetadorGroup],
    node_path: str,
    file_path: Union[Path, str],
    *,
    metadata: Optional[MetadataSchema] = None,
) -> MetadorDataset:
    """Embed a file, adding minimal generic metadata to it.

    Will also ensure that the attached metadata has RO-Crate compatible @id set correctly.

    Args:
        container: Container where to embed the file contents
        container_path: Fresh path in container where to place the file
        file_path: Path of an existing file to be embedded
        metadata: If provided, will attach this instead of harvesting defaults.

    Returns:
        Dataset of new embedded file.
    """
    file_path = Path(file_path)

    # check container and file
    if node_path in node:
        raise ValueError(f"Path '{node}' already exists in given container or group!")
    if not file_path.is_file():
        raise ValueError(f"Path '{file_path}' does not look like an existing file!")

    if not metadata:
        # no metadata given -> harvest minimal information about a file
        hv_file = harvesters["core.file.generic"]
        metadata = harvest(FileMeta, [hv_file(filepath=file_path)])
    else:
        metadata = metadata.copy()  # defensive copying!

    # check metadata
    if not isinstance(metadata, FileMeta):
        msg = f"Provided metadata is not compatible with '{FileMeta.Plugin.name}'!"
        raise ValueError(msg)
    if not schemas.is_plugin(type(metadata)):
        msg = f"Given metadata is a {type(metadata)}, which is not a schema plugin!"
        raise ValueError(msg)

    data = _h5_wrap_bytes(file_path.read_bytes())
    ret = node.create_dataset(node_path, data=data)

    # set file metadata @id to be relative to dataset root just like RO Crate wants
    metadata.id_ = urllib.parse.quote(f".{ret.name}")

    # embed file and metadata in container:
    ret.meta[metadata.Plugin.name] = metadata
    return ret
