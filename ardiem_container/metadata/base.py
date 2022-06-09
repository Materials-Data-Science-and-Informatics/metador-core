from __future__ import annotations

import inspect
from json.decoder import JSONDecodeError
from pathlib import Path
from typing import Optional, Type, Union, get_args, get_type_hints

import h5py
from pydantic import BaseModel, Field, ValidationError, create_model
from pydantic_yaml import YamlModelMixin

from ..ih5.record import IH5Group, IH5Record
from ..packer import ArdiemValidationErrors
from .types import PintUnit


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


def create_union_model(
    name: str,
    union_type,
    discriminator: Optional[str] = None,
    base: Optional[Type[BaseModel]] = None,
    module: Optional[str] = None,
):
    """Create a wrapper pydantic class for parsing (possibly tagged) unions."""
    if base is None:
        base = BaseModel
    assert issubclass(base, BaseModel)  # must be pydantic model

    if module is None:
        # get module of calling site (to put new class in that namespace)
        frm = inspect.stack()[1]
        mod = inspect.getmodule(frm[0])
        assert mod is not None
        module = mod.__name__

    # use inline intermediate class to add customized methods
    class TaggedUnion(base):  # type: ignore
        @classmethod
        def types(cls):
            """Inspect the types in the tagged union."""
            return get_args(get_type_hints(cls)["__root__"])

        # override parsing of pydantic to return actual object in the __root__
        # this is used in parse_raw, which is used in parse_file
        @classmethod
        def parse_obj(cls, *args, **kwargs):
            """Parse the passed object.

            Returns instance of a type in the tagged union in __root__ on success.
            """
            return super().parse_obj(*args, **kwargs).__root__  # type: ignore

    # use dynamic create_model so we can give returned class a proper name
    return create_model(
        name,
        __module__=module,
        __base__=TaggedUnion,
        __root__=(union_type, Field(..., discriminator=discriminator)),
    )  # type: ignore
