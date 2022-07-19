"""Defines schema as pluggable entity."""

from typing import List, get_type_hints

from ..plugins.interface import PluginGroup
from .interface import MetadataSchema


class PGSchema(PluginGroup):
    """Interface to access installed schema plugins.

    A valid schema must be subclass of `MetadataSchema` and also subclass of
    the parent schema plugin (if any) that it returns with parent_schema().

    Each instance of a schema MUST be also parsable by the listed parent schema.

    A subschema MUST NOT override existing parent field definitions.

    (NOTE: with some caveats, we technically could weaken this to:
    A subschema SHOULD only extend a parent by new fields.
    It MAY override parent fields with more specific types.)

    All registered schemas can be used anywhere in a Metador container to annotate
    any group or dataset with metadata objects following that schema.

    If you don't want that, do not register the schema as a plugin, but just use the
    schema class as a normal Python dependency. Unregistered schemas still must inherit
    from MetadataSchema, to ensure that required (de-)serializers and methods are
    available.

    Unregistered schemas can be used as "abstract" base schemas that should not be
    instantiated in containers, or for schemas that are not "top-level entities",
    but only describe a part of a meaningful metadata object.
    """

    def check_plugin(self, schema_name: str, ep):
        """Check schema for validity."""
        if not issubclass(ep, MetadataSchema):
            msg = f"{schema_name}: Schema not subclass of MetadataSchema!"
            raise TypeError(msg)

        parent_ref = ep.parent_schema()
        if parent_ref is None:
            return

        parent = self.get(parent_ref.name)
        if not parent:
            msg = f"{schema_name}: Parent schema {parent_ref} not found!"
            raise TypeError(msg)

        inst_parent_ref = self.fullname(parent_ref.name)
        if not inst_parent_ref.supports(parent_ref):
            msg = f"{schema_name}: Installed parent schema version ({inst_parent_ref}) "
            msg += "incompatible with required version ({parent_ref})!"
            raise TypeError(msg)

        if not issubclass(ep, parent):
            msg = f"{schema_name}: {ep} is not subclass of "
            msg += f"claimed parent schema {parent} ({parent_ref})!"
            raise TypeError(msg)

        # make sure that subclasses don't shadow parent attributes
        parent_fields = get_type_hints(parent)
        child_fields = get_type_hints(ep)
        overrides = set(parent_fields.keys()).intersection(set(child_fields.keys()))
        for attr_name in overrides:
            if child_fields[attr_name] != parent_fields[attr_name]:
                msg = f"{schema_name}: '{attr_name}' ({child_fields[attr_name]}) is "
                msg += f"overriding type in parent schema ({parent_fields[attr_name]})!"
                raise TypeError(msg)

    @classmethod
    def parent_path(cls, schema_name: str) -> List[str]:
        """Get sequence of registered parent schema names leading to the given schema.

        This sequence can be a subset of the parent sequences in the actual class
        hierarchy (not every subclass must be registered as a plugin).
        """
        ret = [schema_name]

        curr = cls[schema_name]  # type: ignore
        parent = curr.parent_schema()
        while parent is not None:
            ret.append(parent.name)
            curr = cls[parent.name]  # type: ignore
            parent = curr.parent_schema()

        ret.reverse()
        return ret
