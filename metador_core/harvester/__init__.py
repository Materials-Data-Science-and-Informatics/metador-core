"""Plugin group for metadata harvesters."""
from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import (
    Dict,
    Generic,
    List,
    Literal,
    Optional,
    Sequence,
    Set,
    Type,
    TypeVar,
    Union,
)

from overrides import overrides

from ..plugins import installed
from ..plugins import interface as pg
from ..schema import SCHEMA_GROUP_NAME, MetadataSchema, PGSchema

_SCHEMAS: PGSchema = installed.group(SCHEMA_GROUP_NAME, PGSchema)

HARVESTER_GROUP_NAME = "harvester"


class HarvesterPlugin(pg.PluginBase):
    group = HARVESTER_GROUP_NAME
    returns: PGSchema.PluginRef

    class Fields(pg.PluginBase.Fields):
        returns: PGSchema.PluginRef
        """Schema returned by this harvester."""


T = TypeVar("T", bound=MetadataSchema)
S = TypeVar("S", bound=MetadataSchema)


class Harvester(ABC, Generic[T]):
    """Base class for metadata harvesters.

    A harvester is a class that can be instantiated to extract
    metadata according to some schema plugin.
    """

    Plugin: HarvesterPlugin

    @property
    def schema(self) -> Type[T]:
        """Partial schema class returned by this harvester.

        Provided for implementation convenience.
        """
        return _SCHEMAS[self.Plugin.returns.name].Partial

    def __init__(self, *args, **kwargs):
        """Configure harvester instance.

        Override to accept arguments needed to configure the `harvest` method.

        You MUST NOT run the actual harvesting during `__init__`,
        this MUST be done in the `harvest` method.
        """

    @abstractmethod
    def harvest(self) -> T:
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

    def combine(self, existing: S, harvested: S) -> S:
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
        return existing.update(harvested)


def cast_partial(pmodel: Type[T], obj: MetadataSchema) -> T:
    """Try converting to pmodel if not already instance of it."""
    if isinstance(obj, pmodel):
        return obj
    return pmodel.to_partial(obj)


def partial_arg(pmodel: Type[T], obj: Union[str, Path, MetadataSchema]) -> T:
    """Try converting a path or a model instance into given partial model."""
    if isinstance(obj, (str, Path)):
        return pmodel.parse_file(Path(obj))
    else:
        return cast_partial(pmodel, obj)


def schema_arg(obj: Union[str, Type[MetadataSchema]]) -> Type[MetadataSchema]:
    """Return installed schema by name or same schema as passed.

    Ensures that passed object is in fact a valid, installed schema.
    """
    name = obj if isinstance(obj, str) else obj.Plugin.name
    return _SCHEMAS[name]


class PGHarvester(pg.PluginGroup[Harvester]):
    """Harvester plugin group interface."""

    class PluginRef(pg.PluginRef):
        group: Literal["harvester"]

    class Plugin(pg.PGPlugin):
        name = HARVESTER_GROUP_NAME
        version = (0, 1, 0)
        required_plugin_groups = [PGSchema.Plugin.name]
        plugin_class = Harvester
        plugin_info_class = HarvesterPlugin

    @overrides
    def check_plugin(self, name: str, plugin: Type[Harvester]):
        hv_ref = plugin.Plugin.returns

        schema_name = hv_ref.name
        schema = _SCHEMAS[schema_name]
        if not schema:
            raise TypeError(f"{name}: Schema '{schema_name}' not installed!")

        inst_ref = schema.Plugin.ref()
        if not inst_ref.supports(hv_ref):
            msg = f"{name}: Installed schema {inst_ref} incompatible with harvester schema {hv_ref}!"
            raise TypeError(msg)

    def post_load(self):
        """Initialize harvester lookup table."""
        self._harvesters_for: Dict[str, Set[str]] = {}
        for h_name, h in self.items():
            schema = h.Plugin.returns
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
        schema_name = schema_arg(schema).Plugin.name
        ret = set(self._harvesters_for[schema_name])
        if include_children:
            for child in _SCHEMAS.children(schema_name):
                ret = ret.union(self.for_schema(child))
        if include_parents:
            for parent in _SCHEMAS.parent_path(schema_name)[:-1]:
                ret = ret.union(self.for_schema(parent))
        return ret

    def harvest(
        self,
        schema: Type[T],
        harvesters: Sequence[Harvester[T]],
        *,
        start_with: Optional[Union[Path, MetadataSchema]] = None,
        merge_with: Optional[Union[Path, MetadataSchema]] = None,
        return_partial: bool = False,
    ) -> T:
        """Run a harvesting pipeline and return combined results.

        Will run the harvesters in the passed order, combining results.
        The combination of partial results can be customized in harvesters,
        but in general you can expect that if two harvesters provide the same field,
        the newer value by a later harvester will overwrite an existing one from an
        earlier harvester or combined with it in a suitable way.

        If `start_with` is given as a path or partial metadata object, will perform
        harvesting on top of the provided partial object instead of an empty one.

        If `merge_with` is given as a path or partial metadata object, will
        combine result of the harvester pipeline with the passed metadata
        before returning.

        By default, it is assumed that the result can be converted into a fully
        valid schema instance that can be returned. If this final conversion fails,
        an exception will be raised.

        If `return_partial` is set, then the partial instance is returned
        without the final conversion. In that case, you can manually add
        additional metadata and then call `from_partial()` to complete it.

        If converting between possibly different schemas fails at some point
        of the harvester pipeline, a `ValidationError` will be raised.

        Args:
            schema: Class of schema to be returned.
            harvesters: List of harvester instances to run.
            start_with: (Filepath of) partial metadata to be used as base (default: None)
            merge_with: (Filepath of) partial metadata to be merged with result (default: None).
            return_partial: Whether to return raw partial instance (default: False).

        Returns:
            Metadata object with combined results.
        """
        # partial to start sequence with (e.g. previous partial data):
        first: T = schema.Partial()
        if start_with:
            first = partial_arg(schema.Partial, start_with)
        # partial to complete sequence with (e.g. user overrides):
        last: Optional[T] = None
        if merge_with:
            last = partial_arg(schema.Partial, merge_with)

        # collect partial metadata (NOTE: in principle, this could be parallelized)
        results: List[T] = list(map(lambda h: h.harvest(), harvesters))

        # accumulate results in provided order
        merged = first
        for i, h in enumerate(harvesters):
            # harvesters can have different but compatible output
            # models, to we cast the result if needed
            result: T = cast_partial(schema.Partial, results[i])
            # combine metadata (like a "reduce" step, only that
            # at each step the reducing function can be modified)
            merged = h.combine(merged, result)
        if last:
            merged = merged.update(last)

        # retrieve (completed) metadata model
        return merged if return_partial else merged.from_partial()
