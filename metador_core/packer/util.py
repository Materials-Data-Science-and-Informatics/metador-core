"""Various helper functions that packers can use."""

from __future__ import annotations

from json.decoder import JSONDecodeError
from pathlib import Path

# from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union
from typing import Any, Dict, List, Optional

import h5py
import numpy
from pydantic import ValidationError

from ..container import MetadorContainer
from ..schema import MetadataSchema


class DirValidationErrors(ValueError):
    """Structure to collect record or directory validation errors.

    Returned from validation functions and thrown in other contexts.
    """

    errors: Dict[str, List[Any]]
    """Common type used to collect errors.

    Maps path in container or directory to list of errors with that path.

    The list should contain either strings or other such dicts,
    but Python type checkers are unable to understand recursive types.
    """

    def __init__(self, errs: Optional[Dict[str, Any]] = None):
        self.errors = errs or {}

    def __bool__(self):
        return bool(self.errors)

    def __repr__(self):  # pragma: no cover
        return repr(self.errors)

    def update(self, *err_objs: DirValidationErrors):
        """Add errors from other instances to this object."""
        errs = self.errors
        for more_errs in err_objs:
            for k, v in more_errs.errors.items():
                if k not in errs:
                    errs[k] = v
                else:
                    errs[k] += v

    def add(self, k, v):
        if k not in self.errors:
            self.errors[k] = []
        self.errors[k].append(v)


# Convenience functions for packers


def check_file(path: Path, **kwargs):
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


def wrap_bytes_h5(bs: bytes):
    """Wrap bytes with numpy.void if non-empty, or return h5py.Empty."""
    return numpy.void(bs) if len(bs) else h5py.Empty("b")


def embed_file(container: MetadorContainer, path: str, filepath: Path):
    pass
    # TODO: wrap, add minimal FileMeta
