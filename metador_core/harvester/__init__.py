"""Plugin group for metadata harvesters."""
from __future__ import annotations

from abc import ABC, ABCMeta, abstractmethod
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Callable,
    ClassVar,
    Dict,
    Iterable,
    Set,
    Type,
    TypeVar,
    Union,
)

from overrides import overrides
from pydantic import Extra, ValidationError
from typing_extensions import TypeAlias

from ..plugin import interface as pg
from ..plugins import schemas
from ..schema import MetadataSchema
from ..schema.core import BaseModelPlus
from ..schema.partial import PartialFactory, PartialModel
from ..schema.plugins import PluginRef

HARVESTER_GROUP_NAME = "harvester"


if TYPE_CHECKING:
    SchemaPluginRef: TypeAlias = PluginRef
else:
    SchemaPluginRef = schemas.PluginRef


class HarvesterPlugin(pg.PluginBase):
    returns: SchemaPluginRef
    """Schema returned by this harvester."""


S = TypeVar("S", bound=MetadataSchema)


class HarvesterArgs(BaseModelPlus):
    """Base class for harvester arguments."""

    class Config(BaseModelPlus.Config):
        extra = Extra.forbid


class HarvesterArgsPartial(PartialFactory):
    """Base class for partial harvester arguments."""

    base_model = HarvesterArgs


class HarvesterMetaMixin(type):
    _args_partials: Dict[Type, Type] = {}

    @property
    # @cache not needed, partials take care of that themselves
    def PartialArgs(self):
        """Access the partial schema based on the current schema."""
        return HarvesterArgsPartial.get_partial(self.Args)


class HarvesterMeta(HarvesterMetaMixin, ABCMeta):
    ...


class Harvester(ABC, metaclass=HarvesterMeta):
    """Base class for metadata harvesters.

    A harvester is a class that can be instantiated to extract
    metadata according to some schema plugin (the returned schema
    must be defined as the `Plugin.returns` attribute)

    Override the inner `Args` class with a subclass to add arguments.
    This works exactly like schema definition and is simply
    another pydantic model, so you can use both field and root
    validators to check whether the arguments are making sense.

    If all you need is getting a file path, consider using
    `FileHarvester`, which already defines a `filepath` argument.

    Override the `run()` method to implement the metadata harvesting
    based on a validated configuration located at `self.args`.
    """

    Plugin: ClassVar[HarvesterPlugin]

    if TYPE_CHECKING:
        Args: TypeAlias = HarvesterArgs
    else:
        Args = HarvesterArgs
        """Arguments to be passed to the harvester."""

    args: Union[Args, HarvesterArgsPartial]

    @property
    def schema(self):
        """Partial schema class returned by this harvester.

        Provided for implementation convenience.
        """
        return schemas[self.Plugin.returns.name].Partial

    # ----
    # configuring and checking harvester instances:

    def __init__(self, **kwargs):
        """Initialize harvester with (partial) configuration."""
        self.args = type(self).PartialArgs.parse_obj(kwargs)

    def __call__(self, **kwargs):
        """Return copy of harvester with updated configuration."""
        args = type(self).PartialArgs.parse_obj(kwargs)
        merged = self.args.merge_with(args)
        return type(self)(**merged.dict())

    def __repr__(self):
        return f"{type(self).__name__}(args={repr(self.args)})"

    def harvest(self):
        """Check provided arguments and run the harvester.

        Call this when a harvester is configured and it should be executed.
        """
        try:
            self.args = self.args.from_partial()
        except ValidationError as e:
            hname = type(self).__name__
            msg = f"{hname} configuration incomplete or invalid:\n{str(e)}"
            raise ValueError(msg)
        return self.run()

    # ----
    # to be overridden

    @abstractmethod
    def run(self):
        """Do the harvesting according to instance configuration and return metadata.

        Override this method with your custom metadata harvesting logic, based
        on configuration provided in `self.args`.

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


class FileHarvester(Harvester):
    """Harvester for processing a single file path.

    The file path is not provided or set during __init__,
    but instead is passed during harvest_file.
    """

    class Args(Harvester.Args):
        filepath: Path


class MetadataLoader(FileHarvester):
    _partial_schema: PartialModel
    _sidecar_func: Callable[[Path], Path]

    def run(self):
        path = self.args.filepath
        func = type(self).__dict__.get("_sidecar_func")
        used_path = func(path)
        if not used_path.is_file():
            return self._partial_schema()
        return self._partial_schema.parse_file(used_path)


# ----


def _schema_arg(obj: Union[str, Type[MetadataSchema]]) -> Type[MetadataSchema]:
    """Return installed schema by name or same schema as passed.

    Ensures that passed object is in fact a valid, installed schema.
    """
    name = obj if isinstance(obj, str) else obj.Plugin.name
    return schemas[name]


class PGHarvester(pg.PluginGroup[Harvester]):
    """Harvester plugin group interface."""

    class Plugin:
        name = HARVESTER_GROUP_NAME
        version = (0, 1, 0)
        requires = [PluginRef(group="plugingroup", name="schema", version=(0, 1, 0))]
        plugin_class = Harvester
        plugin_info_class = HarvesterPlugin

    def __post_init__(self):
        self._harvesters_for: Dict[str, Set[str]] = {}

    def plugin_deps(self, plugin):
        if p := plugin.Plugin.returns:
            return {p}

    @overrides
    def check_plugin(self, ep_name: str, plugin: Type[Harvester]):
        hv_ref = plugin.Plugin.returns

        schema_name = hv_ref.name
        schema = schemas[schema_name]
        if not schema:
            raise TypeError(f"{ep_name}: Schema '{schema_name}' not installed!")

        inst_ref = schema.Plugin.ref()
        if not inst_ref.supports(hv_ref):
            msg = f"{ep_name}: Installed schema {inst_ref} incompatible with harvester schema {hv_ref}!"
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


def _harvest_source(schema: Type[S], obj: Union[Path, Harvester]) -> S:
    """Harvest partial metadata from the passed Harvester or Path into target schema."""
    if not isinstance(obj, (Path, Harvester)):
        raise ValueError("Metadata source must be a Harvester or a Path!")
    if isinstance(obj, Path):
        obj = metadata_loader(schema)(filepath=obj)
    return obj.harvest()


def _identity(x):
    return x


# harvesting helpers:


def def_sidecar_func(path: Path):
    """Return sidecar filename for a given file path using the default convention.

    This means, the path gets a '_meta.yaml' suffix attached.
    """
    return Path(f"{str(path)}_meta.yaml")


def metadata_loader(
    schema: Type[MetadataSchema], *, use_sidecar: bool = False, sidecar_func=None
) -> Type[MetadataLoader]:
    """Return harvester for partial metadata files of specified schema.

    Will return an empty partial schema in case the file does not exist.

    The fields provided in the file must be valid, or an exception will be raised.
    This is to avoid the situation where a user intends to add or override
    harvester metadata, but the fields are silently ignored, leading to possible
    surprise and confusion.

    By default, the returned harvester will attempt to parse the provided file.

    Set `use_sidecar` if you intend to pass it filenames of data files, but
    want it to instead load metadata from a sidecar file.

    Provide a `sidecar_func` if you want to change the used sidecar file convention.
    By default, `def_sidecar_func` is used to transform the path.
    """
    used_sidecar_f = _identity
    if use_sidecar:
        used_sidecar_f = sidecar_func if sidecar_func else def_sidecar_func

    class XLoader(MetadataLoader):
        _partial_schema = schema.Partial
        _sidecar_func = used_sidecar_f

    name = f"{schema.__name__}Loader"
    XLoader.__qualname__ = XLoader.__name__ = name
    return XLoader


def configure(*harvesters: Union[Harvester, Type[Harvester]], **kwargs):
    """Given a sequence of harvesters, configure them all at once.

    Can be used to set the same parameter in all of them easily.
    """
    return (h(**kwargs) for h in harvesters)


def file_harvester_pipeline(*args: Union[FileHarvester, Type[FileHarvester]]):
    """Generate a harvesting pipeline for a file.

    Args:
        FileHarvester classes or pre-configured instances to use.

        The passed objects must be preconfigured as needed,
        except for fixing a filepath (it will be overwritten).

    Returns:
        Function that takes a file path and will return
        the harvesters configured for the passed path.
    """
    return lambda path: configure(*args, filepath=path)


def harvest(
    schema: Type[S],
    sources: Iterable[Union[Path, Harvester]],
    *,
    ignore_invalid: bool = False,
    return_partial: bool = False,
) -> S:
    """Run a harvesting pipeline and return combined results.

    Will run the harvesters in the passed order, combining results.

    In general you can expect that if two harvesters provide the same field,
    the newer value by a later harvester will overwrite an existing one from an
    earlier harvester, or the values are combined in a suitable way.

    If converting some Harvester result to the desired schema fails,
    a `ValidationError` will be raised.  To change that behaviour,
    set `ignore_invalid`, will make sure that suitable fields are still used,
    even if the given object as a whole is not fully parsable.

    Note that `ignore_invalid` only affects the conversion AFTER running the
    harvesters, it does NOT affect the way the harvesters treat invalid metadata.

    By default, it is assumed that the result of the whole pipeline can be
    converted into a fully valid non-partial schema instance.
    If this final conversion fails, an exception will be raised.
    To prevent this conversion, set `return_partial`. In that case, you will
    get the partial instance as-is and can manually call `from_partial()`.

    Args:
        schema: Class of schema to be returned.
        sources: List of sources (Paths or Harvester instances).
        ignore_invalid: Whether to ignore invalid fields in Harvester outputs.
        return_partial: Whether to return raw partial instance (default: False).

    Returns:
        Metadata object with combined results.
    """

    def cast(meta):
        # configure cast to ignore or not ignore invalid fields
        return schema.Partial.cast(meta, ignore_invalid=ignore_invalid)

    # collect partial metadata (NOTE: in principle, this could be parallelized)
    results = map(lambda s: cast(_harvest_source(schema, s)), sources)

    # accumulate collected and casted results in provided order
    merged = schema.Partial.merge(*results)

    # retrieve (completed) metadata model
    return merged if return_partial else merged.from_partial()
