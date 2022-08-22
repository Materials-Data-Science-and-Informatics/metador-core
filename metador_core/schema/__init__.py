"""Definition of Metador schema interface and core schemas."""

from __future__ import annotations

from queue import SimpleQueue
from typing import Any, Dict, List, Literal, Optional, Set, Type

from overrides import overrides

from ..plugins import interface as pg
from .core import MetadataSchema
from .partial import create_partial_model
from .utils import LiftedRODict, collect_model_types

SCHEMA_GROUP_NAME = "schema"


class SchemaPluginRef(pg.PluginRef):
    group: Literal["schema"]


class SchemaPlugin(pg.PluginBase):
    group = SCHEMA_GROUP_NAME
    parent_schema: Optional[SchemaPluginRef]

    class Fields(pg.PluginBase.Fields):
        parent_schema: Optional[SchemaPluginRef]
        """Declares a parent schema plugin.

        By declaring a parent schema you agree to the following contract:
        Any data that can be loaded using this schema MUST also be
        loadable by the parent schema (with possible information loss).
        """


class PGSchema(pg.PluginGroup[MetadataSchema]):
    """Interface to access installed schema plugins.

    All registered schema plugins can be used anywhere in a Metador container to
    annotate any group or dataset with metadata objects following that schema.

    If you don't want that, do not register the schema as a plugin, but just use
    the schema class as a normal Python dependency. Schemas that are not
    registered as plugins still must inherit from MetadataSchema, to ensure that
    all required methods are available and work as expected by the system.

    Unregistered schemas can be used as "abstract" parent schemas that cannot be
    instantiated in containers because they are too general to be useful, or for
    schemas that are not intended to be used on their own in the container, but
    model a meaningful metadata object that can be part of a larger schema.

    Rules for schema versioning:

    All schemas must be subclass of `MetadataSchema` and also subclass of the
    parent schema plugin declared as `parent_schema` (if defined).

    Semantic versioning (MAJOR, MINOR, PATCH) is to be followed.
    For schemas, this roughly translates to:

    If you do not modify the set of parsable instances by your changes,
    you may increment only PATCH.

    If your changes strictly increase parsable instances, that is,
    your new version can parse older metadata of the same MAJOR,
    you may increment only MINOR (resetting PATCH to 0).

    If your changes could make some older metadata invalid,
    you must increment MAJOR (resetting MINOR and PATCH to 0).

    If you add, remove or change the name of a parent schema,
    you must increment MAJOR.

    If you change the version in the `parent_schema` to a version
    that with higher X (MAJOR, MINOR or PATCH), the version
    of your schema must be incremented in X as well.

    Rules for schema extension:

    A child schema that only extends a parent with new fields is safe.
    To schemas that redefine parent fields additional rules apply:

    EACH instance of a schema MUST also be parsable by the parent schema

    This means that a child schema may only override parent fields
    with more specific types, i.e., only RESTRICT the set of possible
    values compared to the parent field (safe examples include
    adding new or narrowing existing bounds and constraints,
    or excluding some values that are allowed by the parent schema).

    As automatically verifying this is not feasible, but the ability
    is very much needed in practical use, the schema developer
    MUST create suitable represantative test cases that check whether
    this property is satisfied.
    """

    class PluginRef(SchemaPluginRef):
        ...

    class Plugin(pg.PGPlugin):
        name = SCHEMA_GROUP_NAME
        version = (0, 1, 0)
        required_plugin_groups: List[str] = []
        plugin_class = MetadataSchema
        plugin_info_class = SchemaPlugin

    @overrides
    def check_plugin(self, name: str, plugin: Type[MetadataSchema]):
        for forbidden in ["Schemas", "Partial"]:
            # check that the auto-generated field names are not used
            if hasattr(plugin, forbidden):
                raise TypeError(f"{name}: Schema has forbidden field '{forbidden}'!")

        parent_ref = plugin.Plugin.parent_schema
        if parent_ref is None:
            return  # no parent method -> nothing to do

        # check whether parent is known, compatible and really is a superclass
        parent = self.get(parent_ref.name)
        if not parent:
            msg = f"{name}: Parent schema '{parent_ref}' not found!"
            raise TypeError(msg)

        inst_parent_ref = self.fullname(parent_ref.name)
        if not inst_parent_ref.supports(parent_ref):
            msg = f"{name}: Installed parent schema version ({inst_parent_ref}) "
            msg += "is incompatible with required version ({parent_ref})!"
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

    @classmethod
    def _attach_subschemas(cls, schema: MetadataSchema, subschemas):
        """Attach inner class to a schema for subschema lookup.

        This enables users to access subschemas without extra imports
        that bypass the plugin system and improves decoupling.
        """
        # custom repr string to make it look like the class was there all along
        rep = f'<class \'{".".join([schema.__module__, schema.__name__, "Schemas"])}\'>'

        class Schemas(metaclass=LiftedRODict):
            _repr = rep
            _schemas = {t.__name__: t for t in subschemas}

        setattr(schema, "Schemas", Schemas)

    def post_load(self):
        subschemas: Dict[Any, Set[Any]] = {}

        # collect registered schemas
        for schema in self.values():
            subschemas[schema] = collect_model_types(schema, bound=MetadataSchema)
            # to those we attach the subschemas helper inner class
            self._attach_subschemas(schema, subschemas[schema])

        # collect possibly unregistered schemas (non-plugins)
        missed = set()
        for deps in subschemas.values():
            for dep in deps:
                if dep not in subschemas:
                    missed.add(dep)

        # explore the hierarchy of unregistered schemas
        q = SimpleQueue()
        for s in missed:
            q.put(s)
        while not q.empty():
            s = q.get()
            if s in subschemas:
                continue
            subschemas[s] = collect_model_types(s, bound=MetadataSchema)
            if s in subschemas[s]:
                subschemas[s].remove(s)  # recursive model
            for dep in subschemas[s]:
                if dep not in subschemas:
                    q.put(dep)

        partials = {}  # schema -> its partial
        refs = {}  # partial name -> partial

        for schema in subschemas.keys():
            schema.update_forward_refs()

        for schema in subschemas.keys():
            partial = create_partial_model(schema)
            partials[schema] = partial
            refs[partial.__name__] = partial

        for schema, partial in partials.items():
            # partial.update_forward_refs(**refs)
            setattr(schema, "Partial", partial)
