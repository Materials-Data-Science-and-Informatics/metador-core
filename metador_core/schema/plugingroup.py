from __future__ import annotations

from typing import List, Optional, Type

from overrides import overrides

from ..plugins import interface as pg
from . import MetadataSchema

SCHEMA_GROUP_NAME = "schema"


class SchemaPlugin(pg.PluginBase):
    group: str = SCHEMA_GROUP_NAME

    class Fields(pg.PluginBase.Fields):
        parent_schema: Optional[pg.PluginRef]
        """Declares a parent schema plugin.

        By declaring a parent schema you agree to the following contract:
        Any data that can be loaded using this schema MUST also be
        loadable by the parent schema (with possible information loss).
        """


class PGSchema(pg.PluginGroup[MetadataSchema]):
    """Interface to access installed schema plugins.

    A valid schema must be subclass of `MetadataSchema` and also subclass of
    the parent schema plugin (if any) that it returns with parent_schema.

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

    class Plugin(pg.PGPlugin):
        name = SCHEMA_GROUP_NAME
        version = (0, 1, 0)
        required_plugin_groups: List[str] = []
        plugin_subclass = SchemaPlugin

    @overrides
    def check_plugin(self, name: str, plugin: Type[MetadataSchema]):
        pg.check_is_subclass(name, plugin, MetadataSchema)

        parent_ref = plugin.Plugin.parent_schema
        if parent_ref is None:
            return  # no parent method -> nothing to do

        # check whether parent is known, compatible and really is a superclass
        parent = self.get(parent_ref.name)
        if not parent:
            msg = f"{name}: Parent schema {parent_ref} not found!"
            raise TypeError(msg)

        inst_parent_ref = self.fullname(parent_ref.name)
        if not inst_parent_ref.supports(parent_ref):
            msg = f"{name}: Installed parent schema version ({inst_parent_ref}) "
            msg += "incompatible with required version ({parent_ref})!"
            raise TypeError(msg)

        if not issubclass(plugin, parent):
            msg = f"{name}: {plugin} is not subclass of "
            msg += f"claimed parent schema {parent} ({parent_ref})!"
            raise TypeError(msg)

        # make sure that subclasses don't shadow parent attributes
        # this is to strong an assumption, we actually want to allow to specialize them
        # without breaking parsability
        # TODO: this could go into a test suite helper - check that parsing as parent not broken
        # parent_fields = get_type_hints(parent)
        # child_fields = get_type_hints(plugin)
        # overrides = set(parent_fields.keys()).intersection(set(child_fields.keys()))
        # for attr_name in overrides:
        #     if child_fields[attr_name] != parent_fields[attr_name]:
        #         msg = f"{name}: '{attr_name}' ({child_fields[attr_name]}) is "
        #         msg += f"overriding type in parent schema ({parent_fields[attr_name]})!"
        #         raise TypeError(msg)

    def parent_path(self, schema_name: str) -> List[str]:
        """Get sequence of registered parent schema names leading to the given schema.

        This sequence can be a subset of the parent sequences in the actual class
        hierarchy (not every subclass must be registered as a plugin).
        """
        ret = [schema_name]

        curr = self[schema_name]  # type: ignore
        parent = curr.Plugin.parent_schema
        while parent is not None:
            ret.append(parent.name)
            curr = self[parent.name]  # type: ignore
            parent = curr.Plugin.parent_schema

        ret.reverse()
        return ret
