"""Metador schemas."""

from typing import TYPE_CHECKING

from ..plugin import plugingroups
from .core import (  # noqa: F401
    SCHEMA_GROUP_NAME,
    MetadataSchema,
    SchemaPlugin,
    SchemaPluginRef,
)

if TYPE_CHECKING:
    from .pg import PGSchema

    schemas: PGSchema
    schemas = plugingroups.get(SCHEMA_GROUP_NAME, PGSchema)
else:
    schemas = plugingroups[SCHEMA_GROUP_NAME]
