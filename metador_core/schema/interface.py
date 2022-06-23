from __future__ import annotations

from json.decoder import JSONDecodeError
from pathlib import Path

from pydantic import BaseModel, ValidationError
from pydantic_yaml import YamlModelMixin

# from ..ih5.record import IH5Record, IH5Dataset
from ..packer.util import MetadorValidationErrors
from .types import PintUnit

# TODO: needs to be decoupled from stubs and IH5 and hidden into metador interface
# also will save us from circular imports


class MetadataSchema(YamlModelMixin, BaseModel):
    """Extended Pydantic base model with custom serializers and functions."""

    # https://pydantic-docs.helpmanual.io/usage/exporting_models/#json_encoders

    class Config:
        use_enum_values = True  # to serialize enums properly
        json_encoders = {PintUnit.Parsed: lambda x: str(x)}  # for SI units

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

    # @classmethod
    # def from_record(cls, rec: IH5Record, path: str):
    #     """Get JSON-serialized metadata from a record as a model instance.

    #     If the path is not existing or cannot be parsed, will raise MetadorValidationErrors.
    #     If the path exists, but is a stub value, will return None.
    #     Otherwise, will return the parsed model instance.
    #     """
    #     if path not in rec:
    #         raise MetadorValidationErrors({path: ["not found!"]})
    #     val = None
    #     try:
    #         val = rec[path]
    #         if not isinstance(val, IH5Dataset):
    #             raise MetadorValidationErrors({path: ["Expected dataset!"]})
    #         dat = val[()]
    #         if isinstance(dat, h5py.Empty):
    #             return None
    #         else:
    #             return cls.parse_raw(dat, content_type="application/json")
    #     except (TypeError, JSONDecodeError) as e:
    #         msg = f"Cannot parse {type(val).__name__} as JSON: {str(e)}"
    #         raise MetadorValidationErrors({path: [msg]})
    #     except ValidationError as e:
    #         raise MetadorValidationErrors({path: [str(e)]})

    # @classmethod
    # def from_path(cls, path: Union[Path, str], record: Optional[IH5Record] = None):
    #     """Read instance from a file path or from a path relative to a given record.

    #     JSON and YAML supported for file paths,
    #     only JSON allowed from record datasets and attributes.

    #     Wraps `from_record` and `from_file` in a unified function.
    #     """
    #     if record:
    #         return cls.from_record(record, str(path))
    #     else:
    #         return cls.from_file(Path(path))

    # @classmethod
    # def check_path(cls, path: Union[Path, str], record: Optional[IH5Record] = None):
    #     """Check instance at a file path or a path inside a given record.

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
