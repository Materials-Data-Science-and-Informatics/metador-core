"""Schema plugin group."""

from __future__ import annotations

from typing import Dict, List, Optional, Set, Type

from ..plugin import interface as pg
from .core import MetadataSchema, PartialSchema, check_types, infer_parent
from .plugins import PluginBase

SCHEMA_GROUP_NAME = "schema"  # name of schema plugin group


class SchemaPlugin(PluginBase):
    """Schema-specific Plugin section."""


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


    Guidelines for field definition:

    * Stick to the following types to construct your field annotation:
      - basic types: (`bool, int, float, str`)
      - basic hints from `typing`: `Optional, Literal, Union, Set, List, Tuple`
      - default pydantic types (such as `AnyHttpUrl`)
      - default classes supported by pydantic (e.g. `enum.Enum`, `datetime`, etc.)
      - constrained types defined using the `phantom` package
      - valid schema classes (subclasses of `MetadataSchema`)

    * `Optional` is for values that are semantically *missing*,
      You must not assume that a `None` value represents anything else than that.

    * Prefer `Set` over `List` when order is irrelevant and duplicates are not needed

    * Avoid using plain `Dict`, always define a schema instead if you know the keys,
      unless you really need to "pass through" whatever is given, which is usually
      not necessary for schemas that you design from scratch.

    * Prefer types from `phantom` over using pydantic `Field` settings for expressing
      simple value constraints (e.g. minimal/maximal value or collection length, etc.),
      because `phantom` types can be subclassed to narrow them down.

    * In general, avoid using `Field` at all, except for defining an `alias` for
      attributes that are not valid as Python variables (e.g. `@id` or `$schema`).

    * When using `Field`, make sure to annotate it with `typing_extensions.Annotated`,
      instead of assigning the `Field` object to the field name.


    Rules for schema versioning:

    All schemas must be direct or indirect subclass of `MetadataSchema`.

    Semantic versioning (MAJOR, MINOR, PATCH) is to be followed.
    Bumping a version component means incrementing it and resetting the
    later ones to 0. When updating a schema, you must bump:

    * PATCH, if you do not modify the set of parsable instances,

    * MINOR, if if your changes strictly increase parsable instances,

    * MAJOR otherwise, i.e. some older metadata might not be valid anymore.

    If you update a nested or inherited schema to a version
    with higher X (MAJOR, MINOR or PATCH), the version
    of your schema must be bumped in X as well.


    Rules for schema subclassing:

    A child schema that only extends a parent with new fields is safe.
    To schemas that redefine parent fields additional rules apply:

    EACH instance of a schema MUST also be parsable by the parent schema

    This means that a child schema may only override parent fields
    with more specific types, i.e., only RESTRICT the set of acceptable
    values compared to the parent field (safe examples include
    adding new or narrowing existing bounds and constraints,
    or excluding some values that are allowed by the parent schema).

    As automatically verifying this in full generality is not feasible, but the
    ability to "restrict" fields is very much needed in practical use, the
    schema developer MUST create suitable represantative test cases that check
    whether this property is satisfied.

    Try expressing field value restrictions by:

    * removing alternatives from a `Union`
    * using a subclass of a schema or `phantom` type that was used in the parent

    These can be automatically checked most of the time.
    """

    class Plugin:
        name = SCHEMA_GROUP_NAME
        version = (0, 1, 0)
        requires = [pg.PG_GROUP_NAME]
        plugin_class = MetadataSchema
        plugin_info_class = SchemaPlugin

    def __post_init__(self):
        self._parent_schema: Dict[
            Type[MetadataSchema], Optional[Type[MetadataSchema]]
        ] = {}
        self._parents: Dict[str, List[str]] = {}  # base plugins
        self._children: Dict[str, Set[str]] = {}  # subclass plugins

        # used schemas inside schemas
        self._field_types: Dict[
            Type[MetadataSchema], Dict[str, Set[Type[MetadataSchema]]]
        ] = {}
        self._subschemas: Dict[MetadataSchema, Set[MetadataSchema]] = {}

        # partial schema classes
        self._partials: Dict[MetadataSchema, PartialSchema] = {}
        self._forwardrefs: Dict[str, MetadataSchema] = {}

    def check_plugin(self, name: str, plugin: Type[MetadataSchema]):
        # infer the parent schema plugin, if any
        self._parent_schema[plugin] = infer_parent(plugin)
        # overrides of inherited fields are valid?
        check_types(plugin)

    def _compute_parent_path(self, plugin: Type[MetadataSchema]) -> List[str]:
        # NOTE: schemas must be already loaded
        schema_name = plugin.Plugin.name
        ret = [schema_name]
        curr = plugin
        parent = self._parent_schema[curr]
        while parent is not None:
            ret.append(parent.Plugin.name)
            curr = self._get_unsafe(parent.Plugin.name)
            parent = self._parent_schema[curr]

        ret.reverse()
        return ret

    def init_plugin(self, name, plugin):
        # pre-compute parent schema path
        self._parents[name] = self._compute_parent_path(plugin)
        if name not in self._children:
            self._children[name] = set()

        # collect children schema set
        parents = self._parents[name][:-1]
        for parent in parents:
            if parent not in self._children:
                self._children[parent] = set()
            self._children[parent].add(name)

    # ----

    def parent_path(self, schema_name: str) -> List[str]:
        """Get sequence of registered parent schema names leading to the given schema.

        This sequence can be a subset of the parent sequences in the actual class
        hierarchy (not every subclass must be registered as a plugin).
        """
        self._ensure_is_loaded(schema_name)
        return list(self._parents[schema_name])

    def children(self, schema_name: str) -> Set[str]:
        """Get set of names of registered (strict) child schemas."""
        self._ensure_is_loaded(schema_name)
        return set(self._children[schema_name])


SchemaPlugin.update_forward_refs()
