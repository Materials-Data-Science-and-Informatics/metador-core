"""Definition of Metador schema interface and core schemas."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pydantic import BaseModel
from pydantic_yaml import YamlModelMixin

from ..plugins import installed
from .types import PintUnit

# PGSchema is supposed to be imported by other code from here!
# this ensures that typing works but no circular imports happen
if TYPE_CHECKING:
    from .plugingroup import PGSchema

    _SCHEMAS = installed.group("schema", PGSchema)
else:
    PGSchema = Any
    _SCHEMAS = installed.group("schema")


def schema_ref(schema_name: str):
    """Return a PluginRef for a schema based on its registered name."""
    # TODO: does this make sense? this is evaluated in the environment of the user!
    # won't this just always be the installed version, whichever it is?
    return _SCHEMAS.fullname(schema_name)


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
