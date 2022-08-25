"""Plugin group for metadata harvesters."""
from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Literal, Sequence, Set, Type, TypeVar, Union

from overrides import overrides

from ..plugin import interface as pg
from ..plugin import plugingroups
from ..schema import MetadataSchema, schemas
from ..schema.pg import SCHEMA_GROUP_NAME, PGSchema

HARVESTER_GROUP_NAME = "harvester"


class HarvesterPlugin(pg.PluginBase):
    group = HARVESTER_GROUP_NAME
    returns: PGSchema.PluginRef

    class Fields(pg.PluginBase.Fields):
        returns: PGSchema.PluginRef
        """Schema returned by this harvester."""


S = TypeVar("S", bound=MetadataSchema)


class Harvester(ABC):
    """Base class for metadata harvesters.

    A harvester is a class that can be instantiated to extract
    metadata according to some schema plugin.
    """

    Plugin: HarvesterPlugin

    @property
    def schema(self):
        """Partial schema class returned by this harvester.

        Provided for implementation convenience.
        """
        return schemas[self.Plugin.returns.name].Partial

    @abstractmethod
    def __init__(self, *args, **kwargs):
        """Configure harvester instance.

        Override to accept arguments needed to configure the `harvest` method.

        You MUST NOT run the actual harvesting during `__init__`,
        this MUST be done in the `harvest` method.
        """

    @abstractmethod
    def harvest(self):
        """Run harvesting according to instance configuration and return metadata.

        Override this method with your custom metadata harvesting logic.

        All actual, possibly time-intensive harvesting computations (accessing
        resources, running external code, etc.) MUST be performed in this method.

        Ensure that your harvester does not interfere with anything else, such
        as other harvesters running on possibly the same data - open resources
        such as files in read-only mode. You SHOULD avoid creating tempfiles, or
        ensure that these are managed, fresh and cleaned up when you are done
        (e.g. if you need to run external processes that dump outputs to a
        file).

        Returns:
            A fresh instance of type `self.schema` containing harvested metadata.
        """
        raise NotImplementedError

    def merge(self, existing: S, harvested: S) -> S:
        """Return fresh object combining the metadata of the arguments.

        Override this ONLY if you are sure that you need custom logic for
        combining partial metadata objects. It is easy to mess this up.

        If you are sure, then:

        You MUST NOT modify the arguments you receive.

        You MUST preserve all metadata from both objects that you did not
        override intentionally due to your custom merge logic.

        The passed arguments can have either a more general or more specific
        type than the one you originally harvested. You MUST return an object is
        also of the same type as the passed ones.

        Args:
            prev: Metadata collected by previous harvesters
            harvested: Metadata produced by this harvester

        Returns:
            An fresh combined instance of the same type as the arguments.
        """
        return existing.merge(harvested)


def _schema_arg(obj: Union[str, Type[MetadataSchema]]) -> Type[MetadataSchema]:
    """Return installed schema by name or same schema as passed.

    Ensures that passed object is in fact a valid, installed schema.
    """
    name = obj if isinstance(obj, str) else obj.Plugin.name
    return schemas[name]


class PGHarvester(pg.PluginGroup[Harvester]):
    """Harvester plugin group interface."""

    class PluginRef(pg.PluginRef):
        group: Literal["harvester"]

    class Plugin(pg.PGPlugin):
        name = HARVESTER_GROUP_NAME
        version = (0, 1, 0)
        requires = [SCHEMA_GROUP_NAME]
        plugin_class = Harvester
        plugin_info_class = HarvesterPlugin

    @overrides
    def pre_load(self):
        self._harvesters_for: Dict[str, Set[str]] = {}

    @overrides
    def check_plugin(self, name: str, plugin: Type[Harvester]):
        hv_ref = plugin.Plugin.returns

        schema_name = hv_ref.name
        schema = schemas[schema_name]
        if not schema:
            raise TypeError(f"{name}: Schema '{schema_name}' not installed!")

        inst_ref = schema.Plugin.ref()
        if not inst_ref.supports(hv_ref):
            msg = f"{name}: Installed schema {inst_ref} incompatible with harvester schema {hv_ref}!"
            raise TypeError(msg)

    @overrides
    def init_plugin(self, plugin):
        """Add harvester to harvester lookup table."""
        h_name = plugin.Plugin.name
        schema = plugin.Plugin.returns
        if schema.name not in self._harvesters_for:
            self._harvesters_for[schema.name] = set()
        self._harvesters_for[schema.name].add(h_name)

    def for_schema(
        self,
        schema: Union[str, MetadataSchema],
        *,
        include_children: bool = False,
        include_parents: bool = False,
    ) -> Set[str]:
        """List installed harvesters for the given metadata schema.

        To extend the query to parent or child schemas, set `include_children`
        and/or `include_parents` accordingly.

        Harvesters for child schemas are always compatible with the schema.
        (assuming correct implementation of the child schemas),

        Harvesters for parent schemas can be incompatible in some circumstances
        (specifically, if the parent accepts values for some field that are
        forbidden in the more specialized schema).

        Args:
            schema: schema name or class for which to return harvesters
            include_children: Also include results for installed children
            include_parents: Also include results for parent schemas

        Returns:
            Set of harvesters.
        """
        schema_name = _schema_arg(schema).Plugin.name
        ret = set(self._harvesters_for[schema_name])
        if include_children:
            for child in schemas.children(schema_name):
                ret = ret.union(self.for_schema(child))
        if include_parents:
            for parent in schemas.parent_path(schema_name)[:-1]:
                ret = ret.union(self.for_schema(parent))
        return ret


# harvesting helpers


def _harvest_file(schema: Type[S], *paths: Path) -> S:
    """Harvest partial metadata from the passed path(s) into target schema.

    Will return empty partial schema if file does not exist.
    If it does exist, the provided fields must be valid, or
    an exception will be raised.
    """
    files = filter(Path.is_file, map(Path, paths))
    return schema.Partial.merge(*map(schema.Partial.parse_file, files))


def _harvest_source(
    schema: Type[S], obj: Union[Path, Harvester], *, ignore_invalid: bool = False
) -> S:
    """Harvest partial metadata from the passed Harvester or Path into target schema."""
    if not isinstance(obj, (Path, Harvester)):
        raise ValueError("Metadata source must be a Harvester or a Path!")
    if isinstance(obj, Harvester):
        # could use different schema -> cast
        return schema.Partial.cast(obj.harvest(), ignore_invalid=ignore_invalid)
    else:
        return _harvest_file(schema, obj)


# this is what one the users should use directly:


def harvest(
    schema: Type[S],
    sources: Sequence[Union[Path, Harvester]],
    *,
    ignore_invalid: bool = False,
    return_partial: bool = False,
) -> S:
    """Run a harvesting pipeline and return combined results.

    Will run the harvesters in the passed order, combining results.

    In general you can expect that if two harvesters provide the same field,
    the newer value by a later harvester will overwrite an existing one from an
    earlier harvester or combined with it in a suitable way.

    If converting some Harvester result to the desired schema fails,
    a `ValidationError` will be raised, unless `ignore_invalid` is set,
    which will make sure that suitable fields are still used, even if the
    given object is not fully compatible.

    Note that `ignore_invalid` does NOT affect file sources, which ALWAYS
    must be valid for all provided fields. This is to avoid the situation
    where a user intends to add or override harvester metadata, but the
    fields are silently ignored, leading to possible surprise and confusion.

    By default, it is assumed that the result of the whole pipeline can be
    converted into a fully valid schema instance that can be returned.
    If this final conversion fails, an exception will be raised.
    To prevent this conversion, set `return_partial`. In that case, you will
    get the partial instance as-is and can manually call `from_partial()`
    when it is finalized.

    Args:
        schema: Class of schema to be returned.
        sources: List of sources (Paths or Harvester instances).
        ignore_invalid: Whether to ignore invalid fields in Harvester results.
        return_partial: Whether to return raw partial instance (default: False).

    Returns:
        Metadata object with combined results.
    """
    # collect partial metadata (NOTE: in principle, this could be parallelized)
    results = map(
        lambda s: _harvest_source(schema, s, ignore_invalid=ignore_invalid), sources
    )
    # accumulate resultsp in provided order
    merged = schema.Partial.merge(*results)
    # retrieve (completed) metadata model
    return merged if return_partial else merged.from_partial()


harvesters: PGHarvester
harvesters = plugingroups.get(HARVESTER_GROUP_NAME, PGHarvester)
