from __future__ import annotations

from json.decoder import JSONDecodeError
from pathlib import Path
from typing import Optional, Union

import h5py
from pydantic import BaseModel, ValidationError
from pydantic_yaml import YamlModelMixin

from ..ih5.protocols import H5DatasetLike, H5FileLike
from ..packer.util import MetadorValidationErrors
from .types import PintUnit


def schemas():
    """Access the schema plugin interface."""
    from .pluggable import PluggableSchema  # avoid circular import

    return PluggableSchema


def schema_ref(schema_name: str):
    """Return a FullPluginRef for a schema based on its registered name."""
    return schemas().fullname(schema_name)


class MetadataSchema(YamlModelMixin, BaseModel):
    """Extended Pydantic base model with custom serializers and functions."""

    # https://pydantic-docs.helpmanual.io/usage/exporting_models/#json_encoders

    class Config:
        use_enum_values = True  # to serialize enums properly
        json_encoders = {PintUnit.Parsed: lambda x: str(x)}  # for SI units

    @classmethod
    def parent_schema(cls):
        """Override to declare parent schema plugin as FullPluginRef.

        By declaring a parent schema you agree to the following contract:
        Any data that can be loaded using this schema MUST also be
        loadable by the parent schema (with possible information loss).
        """
        return None

    def __bytes__(self) -> bytes:
        """Serialize to JSON and return UTF-8 encoded bytes to be written in a file."""
        # add a newline, as otherwise behaviour with text editors will be confusing
        # (e.g. vim automatically adds a trailing newline that it hides)
        # https://stackoverflow.com/questions/729692/why-should-text-files-end-with-a-newline
        return (self.json() + "\n").encode(encoding="utf-8")

    # Convenience functions for packers (TODO move to packer interface / helpers)
    # Schemas should not be coupled to packer-specific features

    @classmethod
    def from_file(cls, path: Path):
        """Get YAML- or JSON-serialized metadata from a file as a model instance.

        If the path is not existing or cannot be parsed, will raise MetadorValidationErrors.
        Otherwise, will return the parsed model instance.
        """
        errs = MetadorValidationErrors()
        try:
            return cls.parse_file(path)
        except (JSONDecodeError, ValidationError, FileNotFoundError) as e:
            errs.add(str(path), str(e))
            raise errs

    @classmethod
    def from_container(cls, rec: H5FileLike, path: str):
        """Load JSON-serialized metadata from a container.

        If the path is not existing or cannot be parsed, will raise MetadorValidationErrors.
        If the path exists, but is a stub value (h5py.Empty), will return None.
        Otherwise, will return the parsed model instance.
        """
        if path not in rec:
            raise MetadorValidationErrors({path: ["not found!"]})
        val = None
        try:
            val = rec[path]
            if not isinstance(val, H5DatasetLike):
                raise MetadorValidationErrors({path: ["Expected dataset!"]})
            dat = val[()]
            if isinstance(dat, h5py.Empty):
                return None
            else:
                assert isinstance(dat, bytes)
                return cls.parse_raw(dat, content_type="application/json")
        except (TypeError, JSONDecodeError) as e:
            msg = f"Cannot parse {type(val).__name__} as JSON: {str(e)}"
            raise MetadorValidationErrors({path: [msg]})
        except ValidationError as e:
            raise MetadorValidationErrors({path: [str(e)]})

    @classmethod
    def from_path(cls, path: Union[Path, str], record: Optional[H5FileLike] = None):
        """Read instance from a file path or from a path relative to a given record.

        JSON and YAML supported for file paths,
        only JSON allowed from record datasets and attributes.

        Wraps `from_record` and `from_file` in a unified function.
        """
        if record:
            return cls.from_container(record, str(path))
        else:
            return cls.from_file(Path(path))

    @classmethod
    def check_path(cls, path: Union[Path, str], record: Optional[H5FileLike] = None):
        """Check instance at a file path or a path inside a container.

        JSON and YAML supported for file paths,
        only JSON allowed from record datasets and attributes.

        Will treat `h5py.Empty` as valid metadata (in order to work for stub records).

        Returns errors if any.
        """
        try:
            cls.from_path(path, record=record)
        except MetadorValidationErrors as e:
            return e
        return MetadorValidationErrors()
