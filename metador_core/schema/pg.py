"""Schema plugin group."""

from __future__ import annotations

from queue import SimpleQueue
from typing import Dict, List, Optional, Set, Type, get_type_hints

from overrides import overrides

from ..plugin import interface as pg
from .core import SCHEMA_GROUP_NAME, MetadataSchema, PluginBase
from .partial import PartialSchema
from .utils import LiftedRODict, collect_model_types, get_annotations


class SchemaPlugin(PluginBase):
    parent_schema: Optional[PGSchema.PluginRef]
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
    * using a subclass of a schema or `phantom` type used in the parent

    These can be statically type-checked by e.g. mypy most of the time.

    """

    class Plugin:
        name = SCHEMA_GROUP_NAME
        version = (0, 1, 0)
        requires = [pg.PG_GROUP_NAME]
        plugin_class = MetadataSchema
        plugin_info_class = SchemaPlugin

    def __post_init__(self):
        self._parents: Dict[str, List[str]] = {}  # base plugins
        self._children: Dict[str, Set[str]] = {}  # subclass plugins

        self._subschemas: Dict[
            MetadataSchema, Set[MetadataSchema]
        ] = {}  # inner used models
        self._partials: Dict[MetadataSchema, PartialSchema] = {}
        self._forwardrefs: Dict[str, MetadataSchema] = {}

    def _check_parent(self, name: str, plugin: Type[MetadataSchema]):
        """Sanity-checks for possibly defined parent schema."""
        parent_ref = plugin.Plugin.parent_schema
        if parent_ref is None:
            plugin.__overrides__ = set()
            return  # no parent schema listed -> nothing to do

        # check whether parent is known, compatible and really is a superclass
        parent = self.get(parent_ref.name)
        if not parent:
            msg = f"{name}: Parent schema '{parent_ref}' not found!"
            raise TypeError(msg)

        inst_parent_ref = self.fullname(parent_ref.name)
        if not inst_parent_ref.supports(parent_ref):
            msg = f"{name}: Installed parent schema '{parent_ref.name}' version incompatible "
            msg += f"(has: {inst_parent_ref.version}, needs: {parent_ref.version})!"
            raise TypeError(msg)

        if not issubclass(plugin, parent):
            msg = f"{name}: {plugin} is not subclass of "
            msg += f"claimed parent schema {parent} ({parent_ref})!"
            raise TypeError(msg)

        # tag the overridden fields (defined in parent and in child)
        # (could be automatically tested by pytest to check correct narrowing)
        parent_fields = get_annotations(parent)
        child_fields = get_annotations(plugin)
        plugin.__overrides__ = set(parent_fields.keys()).intersection(
            set(child_fields.keys())
        )

    def _check_types(self, name: str, plugin: Type[MetadataSchema]):
        """Check shape of defined fields."""
        hints = get_type_hints(plugin)
        for field, hint in hints.items():
            if field[0] == "_":
                continue  # private field
            if not PartialSchema._is_mergeable_type(hint):
                raise TypeError(f"{name}: '{field}' contains a forbidden pattern!")

    @overrides
    def check_plugin(self, name: str, plugin: Type[MetadataSchema]):
        self._check_parent(name, plugin)
        self._check_types(name, plugin)

        # make sure that subclasses don't shadow parent attributes
        # this is to strong an assumption, we actually want to allow to specialize them
        # without breaking parsability
        # TODO: this could go into a test suite helper - check that parsing as parent not broken
        # for attr_name in overrides:
        #     if child_fields[attr_name] != parent_fields[attr_name]:
        #         msg = f"{name}: '{attr_name}' ({child_fields[attr_name]}) is "
        #         msg += f"overriding type in parent schema ({parent_fields[attr_name]})!"
        #         raise TypeError(msg)

    def _compute_parent_path(self, plugin: Type[MetadataSchema]) -> List[str]:
        # NOTE: schemas must be already loaded
        schema_name = plugin.Plugin.name
        ret = [schema_name]
        curr = plugin
        parent = curr.Plugin.parent_schema
        while parent is not None:
            ret.append(parent.name)
            curr = self[parent.name]
            parent = curr.Plugin.parent_schema

        ret.reverse()
        return ret

    def plugin_deps(self, plugin):
        if p := plugin.Plugin.parent_schema:
            return {(self.name, p.name)}

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

    def _derive_partial(self, name, plugin):
        # collect possibly unregistered schemas (non-plugins)
        missed = set()
        for dep in self._subschemas[plugin]:
            if dep not in self._subschemas:
                missed.add(dep)

        # explore the hierarchy of unregistered schemas
        q = SimpleQueue()
        for s in missed:
            q.put(s)
        while not q.empty():
            s = q.get()
            if s in self._subschemas:
                continue
            self._subschemas[s] = collect_model_types(s, bound=MetadataSchema)
            if s in self._subschemas[s]:
                self._subschemas[s].remove(s)  # recursive model
            for dep in self._subschemas[s]:
                if dep not in self._subschemas:
                    q.put(dep)

        new_subschemas = set(self._subschemas.keys()) - set(self._partials.keys())
        for schema in new_subschemas:
            # create and collect partials
            partial = PartialSchema._create_partial(schema)
            partial_ref = PartialSchema._partial_forwardref_name(schema)
            self._partials[schema] = partial
            self._forwardrefs[partial_ref] = partial

        for schema in new_subschemas:
            partial = self._partials[schema]
            partial.update_forward_refs(**self._forwardrefs)
            setattr(schema, "Partial", partial)

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

        # update refs, otherwise issues with forward references in same module etc.
        plugin.update_forward_refs()

        # collect immediate subschemas used in a schema
        self._subschemas[plugin] = collect_model_types(plugin, bound=MetadataSchema)

        # attach the subschemas helper inner class to registered schemas
        self._attach_subschemas(plugin, self._subschemas[plugin])

        # derive recursive partial schema
        self._derive_partial(name, plugin)

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
