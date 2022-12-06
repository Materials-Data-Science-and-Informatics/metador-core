"""Interface for plugin groups."""
from __future__ import annotations

from abc import ABCMeta
from typing import (
    Any,
    ClassVar,
    Dict,
    Generic,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
    overload,
)

from importlib_metadata import EntryPoint
from typing_extensions import TypeAlias

from ..schema.plugins import PluginBase, PluginPkgMeta
from ..schema.plugins import PluginRef as AnyPluginRef
from ..util import eprint
from . import util
from .entrypoints import get_group, pkg_meta
from .metaclass import UndefVersion
from .types import (
    EP_NAME_REGEX,
    EPName,
    PluginLike,
    SemVerTuple,
    ep_name_has_namespace,
    from_ep_name,
    is_pluginlike,
    plugin_args,
    to_semver_str,
)

PG_GROUP_NAME = "plugingroup"


class PGPlugin(PluginBase):
    plugin_info_class: Optional[Type[PluginBase]] = None
    plugin_class: Optional[Any] = object


# TODO: plugin group inheritance is not checked yet because it adds complications
class PluginGroupMeta(ABCMeta):
    """Metaclass to initialize some things on creation."""

    def __init__(self, name, bases, dct):
        assert is_pluginlike(self, check_group=False)

        # attach generated subclass that auto-fills the group for plugin infos
        self.PluginRef: Type[AnyPluginRef] = AnyPluginRef._subclass_for(
            self.Plugin.name
        )

        if pgi_cls := self.Plugin.__dict__.get("plugin_info_class"):
            # attach group name to provided plugin info class
            pgi_cls.group = self.Plugin.name
        else:
            # derive generic plugin info class with the group defined
            class PGInfo(PluginBase):
                group = self.Plugin.name

            self.Plugin.plugin_info_class = PGInfo

        # sanity checks... this magic should not mess with the PluginBase
        assert self.Plugin.plugin_info_class is not PluginBase
        assert PluginBase.group == ""


T = TypeVar("T", bound=PluginLike)


class PluginGroup(Generic[T], metaclass=PluginGroupMeta):
    """All pluggable entities in metador are subclasses of this class.

    The type parameter is the (parent) class of all loaded plugins.

    They must implement the check method and be listed as plugin group.
    The name of their entrypoint defines the name of the plugin group.
    """

    PluginRef: TypeAlias = AnyPluginRef
    """Plugin reference class for this plugin group."""

    _PKG_META: ClassVar[Dict[str, PluginPkgMeta]] = pkg_meta
    """Package name -> package metadata."""

    class Plugin:
        """This is the plugin group plugin group, the first loaded group."""

        name = PG_GROUP_NAME
        version = (0, 1, 0)
        plugin_info_class = PGPlugin
        plugin_class: Type
        # plugin_class = PluginGroup  # can't set that -> check manually

    _ENTRY_POINTS: Dict[EPName, EntryPoint]
    """Dict of entry points of versioned plugins (not loaded)."""

    _VERSIONS: Dict[str, List[AnyPluginRef]]
    """Mapping from plugin name to pluginrefs of available versions."""

    _LOADED_PLUGINS: Dict[AnyPluginRef, Type[T]]
    """Dict from entry points to loaded plugins of that pluggable type."""

    def _add_ep(self, epname_str: str, ep_obj: EntryPoint):
        """Add an entrypoint loaded from importlib_metadata."""
        try:
            ep_name = EPName(epname_str)
        except TypeError:
            msg = f"{epname_str}: Invalid entrypoint name, must match {EP_NAME_REGEX}"
            raise ValueError(msg)
        if type(self) is not PluginGroup and not ep_name_has_namespace(ep_name):
            msg = f"{epname_str}: Plugin name has no qualifying namespace!"
            raise ValueError(msg)

        name, version = from_ep_name(ep_name)
        p_ref = AnyPluginRef(group=self.name, name=name, version=version)

        if ep_name in self._ENTRY_POINTS:
            self._LOADED_PLUGINS.pop(p_ref, None)  # unload, if loaded
            pkg = ep_obj.dist
            msg = f"WARNING: {ep_name} is probably provided by multiple packages!\n"
            msg += f"The plugin will now be provided by: {pkg.name} {pkg.version}"
            eprint(msg)
        self._ENTRY_POINTS[ep_name] = ep_obj

        if ep_name not in self._VERSIONS:
            self._VERSIONS[name] = []
        self._VERSIONS[name].append(p_ref)
        self._VERSIONS[name].sort()  # should be cheap

    def __init__(self, entrypoints):
        self._ENTRY_POINTS = {}
        self._VERSIONS = {}

        for k, v in entrypoints.values():
            self._add_ep(k, v)

        self._LOADED_PLUGINS = {}
        self.__post_init__()

    def __post_init__(self):
        if type(self) is PluginGroup:
            # make the magic plugingroup plugin add itself for consistency
            ep_name = util.to_ep_name(self.Plugin.name, self.Plugin.version)
            ep_path = f"{type(self).__module__}:{type(self).__name__}"
            ep = EntryPoint(ep_name, ep_path, self.name)
            self._add_ep(ep_name, ep)

            self_ref = AnyPluginRef(
                group=self.name, name=self.name, version=self.Plugin.version
            )
            self._LOADED_PLUGINS[self_ref] = self
            self.provider(self_ref).plugins[self.name].append(self_ref)

    @property
    def name(self) -> str:
        """Return name of the plugin group."""
        return self.Plugin.name

    @property
    def packages(self) -> Dict[str, PluginPkgMeta]:
        """Return metadata of all packages providing metador plugins."""
        return dict(self._PKG_META)

    def versions(
        self, p_name: str, version: Optional[SemVerTuple] = None
    ) -> List[AnyPluginRef]:
        """Return installed versions of a plugin (compatible with given version)."""
        refs = list(self._VERSIONS.get(p_name) or [])
        if version is None:
            return refs
        requested = self.PluginRef(name=p_name, version=version)
        return [ref for ref in refs if ref.supports(requested)]

    def resolve(
        self, p_name: str, version: Optional[SemVerTuple] = None
    ) -> Optional[AnyPluginRef]:
        """Return most recent compatible version of a plugin."""
        if refs := self.versions(p_name, version):
            return refs[-1]  # latest (compatible) version
        return None

    def provider(self, ref: AnyPluginRef) -> PluginPkgMeta:
        """Return package metadata of Python package providing this plugin."""
        if type(self) is PluginGroup and ref.name == PG_GROUP_NAME:
            # special case - the mother plugingroup plugin is not an EP,
            # so we cheat a bit (schema is in same package, but is an EP)
            return self.provider(self.resolve("schema"))

        ep_name = util.to_ep_name(ref.name, ref.version)
        ep = self._ENTRY_POINTS[ep_name]
        return self._PKG_META[cast(Any, ep).dist.name]

    def is_plugin(self, p_cls):
        """Return whether this class is a (possibly marked) installed plugin.

        Args:
            p_cls: class to be checked
        """
        if not isinstance(p_cls, type) or not issubclass(
            p_cls, self.Plugin.plugin_class
        ):
            return False

        c = UndefVersion._unwrap(p_cls) or p_cls  # get real underlying class
        # check its exactly a registered plugin, if it has a Plugin section
        if info := c.__dict__.get("Plugin"):
            if not isinstance(info, PluginBase):
                return False
            loaded_p = self._get_unsafe(info.name, info.version)
            return loaded_p is c
        else:
            return False

    # ----

    def __repr__(self):
        return f"<PluginGroup '{self.name}' {list(self.keys())}>"

    def __str__(self):
        def pg_line(name_refs):
            name, refs = name_refs
            vs = list(map(lambda x: to_semver_str(x.version), refs))
            # p = self.provider(pg_ref.name)
            # pkg = f"{p.name} {semver_str(p.version)}"
            return f"\t'{name}' ({', '.join(vs)})"

        pgs = "\n".join(map(pg_line, self._VERSIONS.items()))
        return f"Available '{self.name}' plugins:\n{pgs}"

    # ----
    # dict-like interface will provide latest versions of plugins by default

    def __contains__(self, key) -> bool:
        name, version = plugin_args(key)
        if pg_versions := self._VERSIONS.get(name):
            if not version:
                return True
            else:
                pg = self.PluginRef(name=name, version=version)
                return pg in pg_versions
        return False

    def __getitem__(self, key) -> Type[T]:
        if key not in self:
            raise KeyError(f"{self.name} not found: {key}")
        return self.get(key)

    def keys(self) -> Iterator[AnyPluginRef]:
        """Return all names of all plugins."""
        for pgs in self._VERSIONS.values():
            yield from pgs

    def values(self) -> Iterator[Type[T]]:
        """Return latest versions of all plugins (THIS LOADS ALL PLUGINS!)."""
        return map(self.__getitem__, self.keys())

    def items(self) -> Iterator[Tuple[AnyPluginRef, Type[T]]]:
        """Return pairs of plugin name and latest installed version (THIS LOADS ALL PLUGINS!)."""
        return map(lambda k: (k, self[k]), self.keys())

    # ----

    def _get_unsafe(self, p_name: str, version: Optional[SemVerTuple] = None):
        """Return most recent compatible version of given plugin name, without safety rails.

        Raises KeyError if no (compatible) schema found.

        For internal use only!
        """
        if ref := self.resolve(p_name, version):
            self._ensure_is_loaded(ref)
            return self._LOADED_PLUGINS[ref]
        else:  # error
            msg = f"{p_name}"
            if version:
                msg += f": no installed version is compatible with {version}"
            raise KeyError(msg)

    # inspired by this nice trick: https://stackoverflow.com/a/60362860
    PRX = TypeVar("PRX", bound="Type[T]")  # type: ignore

    @overload
    def get(self, key: str, version: Optional[SemVerTuple] = None) -> Optional[Type[T]]:
        ...  # pragma: no cover

    @overload
    def get(self, key: PRX, version: Optional[SemVerTuple] = None) -> Optional[PRX]:
        ...  # pragma: no cover

    def get(
        self, key: Union[str, PRX], version: Optional[SemVerTuple] = None
    ) -> Union[Type[T], PRX, None]:
        key_, version = plugin_args(key, version)

        # retrieve compatible plugin
        try:
            ret = self._get_unsafe(key_, version)
        except KeyError:
            return None

        if version is None:
            # no version constraint was passed or inferred -> mark it
            ret = UndefVersion._mark_class(ret)

        if isinstance(key, str):
            return cast(Type[T], ret)
        else:
            return ret

    # ----

    def _ensure_is_loaded(self, ref: AnyPluginRef):
        """Load plugin from entrypoint, if it is not loaded yet."""
        assert ref.group == self.name
        if ref in self._LOADED_PLUGINS:
            return  # already loaded, all good

        ep_name = util.to_ep_name(ref.name, ref.version)
        ret = self._ENTRY_POINTS[ep_name].load()
        self._LOADED_PLUGINS[ref] = ret

        self._load_plugin(ep_name, ret)

    def _explicit_plugin_deps(self, plugin) -> Set[AnyPluginRef]:
        """Return all plugin dependencies that must be taken into account."""
        def_deps = set(plugin.Plugin.requires)
        extra_deps = set(self.plugin_deps(plugin) or set())
        return def_deps.union(extra_deps)

    def plugin_deps(self, plugin) -> Set[AnyPluginRef]:
        """Return additional automatically inferred dependencies for a plugin."""

    def _load_plugin(self, ep_name: EPName, plugin):
        """Run checks and finalize loaded plugin."""
        from ..plugins import plugingroups

        # run inner Plugin class checks (with possibly new Fields cls)
        if not plugin.__dict__.get("Plugin"):
            raise TypeError(f"{ep_name}: {plugin} is missing Plugin inner class!")
        # pass ep_name to check that it agrees with the plugin info
        plugin.Plugin = self.Plugin.plugin_info_class.parse_info(
            plugin.Plugin, ep_name=ep_name
        )

        # do general checks first, if they fail no need to continue
        self._check_common(ep_name, plugin)
        self.check_plugin(ep_name, plugin)

        for dep_ref in self._explicit_plugin_deps(plugin):
            dep_grp = plugingroups[dep_ref.group]
            dep_grp._ensure_is_loaded(dep_ref)

        self.init_plugin(plugin)

    def _check_common(self, ep_name: EPName, plugin):
        """Perform both the common and specific checks a registered plugin.

        Raises a TypeError with message in case of failure.
        """
        # check correct base class of plugin, if stated
        if self.Plugin.plugin_class:
            util.check_is_subclass(ep_name, plugin, self.Plugin.plugin_class)

    def check_plugin(self, ep_name: EPName, plugin: Type[T]):
        """Perform plugin group specific checks on a registered plugin.

        Raises a TypeError with message in case of failure.

        To be overridden in subclasses for plugin group specific checks.

        Args:
            name: Declared entrypoint name.
            plugin: Object the entrypoint is pointing to.
        """
        # NOTE: following cannot happen as long as we enforce
        # overriding check_plugin.
        # keep that here for now, in case we loosen this
        # if type(self) is not PluginGroup:
        #    return  # is not the "plugingroup" group itself

        # these are the checks done for other plugin group plugins:
        util.check_is_subclass(ep_name, plugin, PluginGroup)
        util.check_is_subclass(ep_name, self.Plugin.plugin_info_class, PluginBase)
        if plugin != PluginGroup:  # exclude itself. this IS its check_plugin
            util.check_implements_method(ep_name, plugin, PluginGroup.check_plugin)

        # NOTE: following cannot happen as long as we set the group
        # automatically using the metaclass.
        # keep that in case we decide to change that / get rid of the metaclass
        # ---
        # make sure that the declared plugin_info_class for the group sets 'group'
        # and it is also equal to the plugin group 'name'.
        # this is the safest way to make sure that Plugin.ref() works correctly.
        # ppgi_cls = plugin.Plugin.plugin_info_class
        # if not ppgi_cls.group:
        #    raise TypeError(f"{ep_name}: {ppgi_cls} is missing 'group' attribute!")
        # if not ppgi_cls.group == plugin.Plugin.name:
        #    msg = f"{ep_name}: {ppgi_cls.__name__}.group != {plugin.__name__}.Plugin.name!"
        #    raise TypeError(msg)

    def init_plugin(self, plugin: Type[T]):
        """Override this to do something after the plugin has been checked."""
        if type(self) is not PluginGroup:
            return  # is not the "plugingroup" group itself
        create_pg(plugin)  # create plugin group if it does not exist


# ----

_plugin_groups: Dict[str, PluginGroup] = {}
"""Instances of initialized plugin groups."""


def create_pg(pg_cls):
    """Create plugin group instance if it does not exist."""
    pg_ref = AnyPluginRef(
        group=PG_GROUP_NAME, name=pg_cls.Plugin.name, version=pg_cls.Plugin.version
    )
    if pg_ref in _plugin_groups:
        return _plugin_groups[pg_ref]

    if not isinstance(pg_cls.Plugin, PluginBase):
        # magic - substitute Plugin class with parsed plugin object
        pg_cls.Plugin = PGPlugin.parse_info(pg_cls.Plugin)

    # TODO: currently we cannot distinguish entrypoints
    # for different versions of the plugin group.
    # should not be problematic for now,
    # as the groups should not change much
    pg = pg_cls(get_group(pg_ref.name))
    _plugin_groups[pg_ref] = pg
