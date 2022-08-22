"""Plugin group for metadata harvesters."""
from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Literal, Optional, Type, TypeVar, Union

from overrides import overrides

from ..plugins import installed
from ..plugins import interface as pg
from ..schema import SCHEMA_GROUP_NAME, MetadataSchema, PGSchema

_SCHEMAS = installed.group(SCHEMA_GROUP_NAME)

HARVESTER_GROUP_NAME = "harvester"


class HarvesterPlugin(pg.PluginBase):
    group = HARVESTER_GROUP_NAME
    returns: PGSchema.PluginRef

    class Fields(pg.PluginBase.Fields):
        returns: PGSchema.PluginRef
        """Schema returned by this harvester."""


T = TypeVar("T", bound=MetadataSchema)


class Harvester(ABC):
    """Base class for metadata harvesters.

    A harvester is a class that can be instantiated to extract
    metadata according to some schema plugin.
    """

    Plugin: HarvesterPlugin

    @property
    def schema(self) -> MetadataSchema:
        """Partial schema class returned by this harvester.

        Provided for implementation convenience.
        """
        return _SCHEMAS[self.Plugin.returns.name].Partial

    @abstractmethod
    def harvest(self) -> MetadataSchema:
        """Run harvesting as configured by the object initialization.

        Override this method with your custom metadata harvesting logic.

        Override `__init__` to accept arguments needed for harvesting.

        Returns:
            A fresh instance of self.Plugin.returns.Partial with harvested metadata.
        """
        raise NotImplementedError

    def combine(self, existing: T, harvested: T) -> T:
        """Return fresh object combining the metadata of the arguments.

        Override this ONLY if you need custom logic to combine partial metadata objects.

        This method MUST NOT modify the passed arguments.

        Args:
            prev: Metadata collected by previous harvesters
            harvested: Metadata produced by this harvester

        Returns:
            An instance of the class `existing.Partial`,
            combining the metadata from `existing` with that from `harvested`.
        """
        return existing.update(harvested)


def cast_partial(pmodel, obj):
    """Try converting to pmodel if not already instance of it."""
    if isinstance(obj, pmodel):
        return obj
    return pmodel.to_partial(obj)


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

    def harvest(
        self,
        schema: MetadataSchema,
        harvesters: list[Harvester],
        *,
        merge_with: Optional[Union[Path, MetadataSchema]] = None,
        return_partial: bool = False,
    ):
        """Run a harvesting pipeline and return combined results.

        Will run the harvesters in the passed order, combining results according
        to their implementation. Usually, if two harvesters provide set the same
        field, then the newer value by a later harvester will overwrite an
        existing one from an earlier harvester.

        If `merge_with` is set to a partial metadata schema instance, will
        combine result of the harvester pipeline with the passed metadata
        before returning.

        By default, will assume that the result can be converted into a fully
        valid schema instance and will raise an exception if this is not the case.

        If this is not desired, set `return_partial` to get the partial instance
        without the final conversion. In that case, you can manually add
        additional metadata and then call `from_partial()` on it by hand.

        If converting between possibly different schemas fails at some point
        of the harvester pipeline, a `ValidationError` will be raised.

        Args:
            schema: Class of schema to be returned.
            harvesters: List of harvester instances to run.
            merge_with: Partial metadata object or filepath to be merged with result.
            return_partial: Whether to return raw partial instance.

        Returns:
            Metadata object with combined results.
        """
        result = schema.Partial()
        for h in harvesters:
            # harvesters can have different but compatible output
            # models, to we cast the result if needed
            current = cast_partial(schema.Partial, h.harvest())
            # combine metadata (like a "reduce" step, only that
            # at each step the reducing function can be modified)
            result = h.combine(result, current)

        if merge_with is not None:
            if isinstance(merge_with, (str, Path)):
                meta = schema.Partial.parse_file(Path(merge_with))
            else:
                meta = cast_partial(schema.Partial, merge_with)
            # combine with final partial metadata
            # (typically, some user-provided additions/overrides)
            result = result.update(meta)

        # retrieve (completed) metadata model
        return result if return_partial else result.from_partial()
