"""Various helper functions that packers can use."""

from __future__ import annotations

# from json.decoder import JSONDecodeError
# from pathlib import Path
# from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union
from typing import Any, Dict, List, Optional

# import h5py
# from pydantic import BaseModel, ValidationError

# from ..ih5.protocols import H5DatasetLike, H5FileLike


class DirValidationError(ValueError):
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

    def append(self, *err_objs: DirValidationError):
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


# Convenience functions for packers (TODO move to packer interface / helpers)
# Schemas should not be coupled to packer-specific features

# @classmethod
# def from_file(cls, path: Path):
#     """Get YAML- or JSON-serialized metadata from a file as a model instance.

#     If the path is not existing or cannot be parsed, will raise MetadorValidationErrors.
#     Otherwise, will return the parsed model instance.
#     """
#     errs = MetadorValidationErrors()
#     try:
#         return cls.parse_file(path)
#     except (JSONDecodeError, ValidationError, FileNotFoundError) as e:
#         errs.add(str(path), str(e))
#         raise errs

# @classmethod
# def from_container(cls, rec: H5FileLike, path: str):
#     """Load JSON-serialized metadata from a container.

#     If the path is not existing or cannot be parsed, will raise MetadorValidationErrors.
#     If the path exists, but is a stub value (h5py.Empty), will return None.
#     Otherwise, will return the parsed model instance.
#     """
#     if path not in rec:
#         raise MetadorValidationErrors({path: ["not found!"]})
#     val = None
#     try:
#         val = rec[path]
#         if not isinstance(val, H5DatasetLike):
#             raise MetadorValidationErrors({path: ["Expected dataset!"]})
#         dat = val[()]
#         if isinstance(dat, h5py.Empty):
#             return None
#         else:
#             assert isinstance(dat, bytes)
#             return cls.parse_raw(dat, content_type="application/json")
#     except (TypeError, JSONDecodeError) as e:
#         msg = f"Cannot parse {type(val).__name__} as JSON: {str(e)}"
#         raise MetadorValidationErrors({path: [msg]})
#     except ValidationError as e:
#         raise MetadorValidationErrors({path: [str(e)]})

# @classmethod
# def from_path(cls, path: Union[Path, str], record: Optional[H5FileLike] = None):
#     """Read instance from a file path or from a path relative to a given record.

#     JSON and YAML supported for file paths,
#     only JSON allowed from record datasets and attributes.

#     Wraps `from_record` and `from_file` in a unified function.
#     """
#     if record:
#         return cls.from_container(record, str(path))
#     else:
#         return cls.from_file(Path(path))

# @classmethod
# def check_path(cls, path: Union[Path, str], record: Optional[H5FileLike] = None):
#     """Check instance at a file path or a path inside a container.

#     JSON and YAML supported for file paths,
#     only JSON allowed from record datasets and attributes.

#     Will treat `h5py.Empty` as valid metadata (in order to work for stub records).

#     Returns errors if any.
#     """
#     try:
#         cls.from_path(path, record=record)
#     except MetadorValidationErrors as e:
#         return e
#     return MetadorValidationErrors()
