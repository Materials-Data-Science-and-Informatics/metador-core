"""Interface for plugin groups."""
from __future__ import annotations

import re
from abc import ABCMeta
from importlib.metadata import EntryPoint
from typing import (
    Any,
    ClassVar,
    Dict,
    Generic,
    Iterable,
    KeysView,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
    overload,
)

from typing_extensions import TypeAlias

from metador_core.schema.types import SemVerTuple

from ..schema.plugins import IsPlugin, PluginBase, PluginPkgMeta
from ..schema.plugins import PluginRef as AnyPluginRef
from .entrypoints import get_group, pkg_meta
from .metaclass import UndefVersion

PG_GROUP_NAME = "plugingroup"

# helpers for checking plugins (also to be used in PluginGroup subclasses):


def _check_plugin_name(name: str):
    if not re.fullmatch("[A-Za-z0-9._-]+", name):
        msg = f"{name}: Invalid plugin name! Only use: A-z, a-z, 0-9, _ - and ."
        raise TypeError(msg)


def _check_plugin_name_prefix(name: str):
    xs = name.split(".")
    if len(xs) < 2 or not (2 < len(xs[0]) < 11):
        msg = f"{name}: Missing/invalid namespace prefix (must have length 3-10)!"
        raise TypeError(msg)


def check_is_subclass(name: str, plugin, base):
    """Check whether plugin has expected parent class (helper method)."""
    if not issubclass(plugin, base):
        msg = f"{name}: {plugin} is not subclass of {base}!"
        raise TypeError(msg)


def test_implements_method(plugin, base_method):
    ep_method = plugin.__dict__.get(base_method.__name__)
    return ep_method is not None and base_method != ep_method


def check_implements_method(name: str, plugin, base_method):
    """Check whether plugin overrides a method of its superclass."""
    if not test_implements_method(plugin, base_method):
        msg = f"{name}: {plugin} does not implement {base_method.__name__}!"
        raise TypeError(msg)


def is_plugin(p_cls, *, group: str = ""):
    """Check whether given class is a loaded plugin.

    If group not specified, will accept any kind of plugin.

    Args:
        p_cls: class of supposed plugin
        group: name of desired plugin group
    """
    if not hasattr(p_cls, "Plugin") or not issubclass(p_cls.Plugin, PluginBase):
        return False  # not suitable
    g = p_cls.Plugin.group
    return bool(g and (not group or group == g))


# ----


class PGPlugin(PluginBase):
    group = PG_GROUP_NAME
    plugin_info_class: Type[PluginBase]
    plugin_class: Optional[Any] = object


# TODO: plugin group inheritance is not checked yet because it adds complications
class PluginGroupMeta(ABCMeta):
    """Metaclass to initialize some things on creation."""

    def __init__(self, name, bases, dct):
        self.Plugin.plugin_info_class.group = self.Plugin.name
        self.PluginRef: Type[AnyPluginRef] = AnyPluginRef.subclass_for(self.Plugin.name)


T = TypeVar("T", bound=IsPlugin)


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

    _ENTRY_POINTS: Dict[str, Any]
    """Dict of entry points (not loaded)."""

    _LOADED_PLUGINS: Dict[str, Type[T]]
    """Dict from entry points to loaded plugins of that pluggable type."""

    def __init__(self, entrypoints):
        self._LOADED_PLUGINS = {}
        self._ENTRY_POINTS = entrypoints
        self.__post_init__()

    def __post_init__(self):
        if type(self) is PluginGroup:
            self._ENTRY_POINTS[PG_GROUP_NAME] = EntryPoint(
                PG_GROUP_NAME,
                f"{type(self).__module__}:{type(self).__name__}",
                PG_GROUP_NAME,
            )
            self._LOADED_PLUGINS[PG_GROUP_NAME] = self
            self.provider(PG_GROUP_NAME).plugins[PG_GROUP_NAME].add(PG_GROUP_NAME)

    @property
    def name(self) -> str:
        return self.Plugin.name

    @property
    def packages(self) -> Dict[str, PluginPkgMeta]:
        """Return metadata of all packages providing metador plugins."""
        return dict(self._PKG_META)

    def fullname(self, ep_name: str) -> PluginRef:
        plugin = self._get_unsafe(ep_name)
        return self.PluginRef(name=ep_name, version=plugin.Plugin.version)

    def provider(self, ep_name: str) -> PluginPkgMeta:
        """Return package metadata of Python package providing this plugin."""
        if type(self) is PluginGroup and ep_name == PG_GROUP_NAME:
            return self.provider("schema")

        ep = self._ENTRY_POINTS[ep_name]
        return self._PKG_META[ep.dist.name]

    def __contains__(self, key: str) -> bool:
        return key in self._ENTRY_POINTS

    def keys(self) -> KeysView[str]:
        """Return all names of all plugins."""
        return self._ENTRY_POINTS.keys()

    def values(self) -> Iterable[Type[T]]:
        """Return all plugins (THIS LOADS ALL PLUGINS!)."""
        return map(self.__getitem__, self.keys())

    def items(self) -> Iterable[Tuple[str, Type[T]]]:
        """Return pairs of names and plugins (THIS LOADS ALL PLUGINS!)."""
        return map(lambda k: (k, self[k]), self.keys())

    def __getitem__(self, key: str) -> Type[T]:
        if key not in self:
            raise KeyError(f"{self.name} not found: {key}")
        return self.get(key)

    # inspired by this nice trick: https://stackoverflow.com/a/60362860
    PRX = TypeVar("PRX", bound="Type[T]")  # type: ignore

    @overload
    def get(self, key: str, version: Optional[SemVerTuple] = None) -> Optional[Type[T]]:
        ...

    @overload
    def get(self, key: PRX, version: Optional[SemVerTuple] = None) -> Optional[PRX]:
        ...

    def get(
        self, key: Union[str, PRX], version: Optional[SemVerTuple] = None
    ) -> Union[Type[T], PRX, None]:
        passed_str = isinstance(key, str)
        key_: str = key if passed_str else key.Plugin.name  # type: ignore

        ret = self._get_unsafe(key_)

        if not version:
            ret = UndefVersion._mark_class(ret)
        else:
            # check for version compatibility:
            cur_ver = ret.Plugin.ref(version=ret.Plugin.version)
            req_ver = ret.Plugin.ref(version=version)
            if not cur_ver.supports(req_ver):
                msg = f"{ret.Plugin.name} {cur_ver} incompatible with required version {req_ver}!"
                raise RuntimeError(msg)

        if passed_str:
            return cast(Type[T], ret)
        else:
            return ret

    def _get_unsafe(self, key: str):
        # returns any version that is installed
        if key not in self:
            return None  # no such plugin installed
        self._ensure_is_loaded(key)
        return self._LOADED_PLUGINS[key]

    def _ensure_is_loaded(self, key: str):
        """Load plugin from entrypoint if it is not loaded yet."""
        if key in self._LOADED_PLUGINS:
            return  # already loaded, all good
        ret = self._ENTRY_POINTS[key].load()
        self._LOADED_PLUGINS[key] = ret
        self._load_plugin(key, ret)

    def _explicit_plugin_deps(self, plugin):
        """Return all plugin dependencies that must be taken into account."""
        def_deps = set(map(lambda n: (self.name, n), plugin.Plugin.requires))
        extra_deps = self.plugin_deps(plugin) or set()
        if not isinstance(extra_deps, set):
            extra_deps = set(extra_deps)
        return def_deps.union(extra_deps)

    def plugin_deps(self, plugin):
        """Return additional automatically inferred dependencies for a plugin."""

    def _load_plugin(self, name: str, plugin):
        """Run checks and finalize loaded plugin."""
        # print("load", self.name, name, plugin)
        from ..plugins import plugingroups

        # run inner Plugin class checks (with possibly new Fields cls)
        if not plugin.__dict__.get("Plugin"):
            raise TypeError(f"{name}: {plugin} is missing Plugin inner class!")
        plugin.Plugin = self.Plugin.plugin_info_class.parse_info(
            plugin.Plugin, ep_name=name
        )

        # do general checks first, if they fail no need to continue
        self._check_common(name, plugin)
        self.check_plugin(name, plugin)

        for depgroup, depname in self._explicit_plugin_deps(plugin):
            # print("check dep", depgroup, depname)
            plugingroups[depgroup]._ensure_is_loaded(depname)

        # print("init", self.name, name)
        self.init_plugin(name, plugin)

    def _check_common(self, name: str, plugin):
        """Perform both the common and specific checks a registered plugin.

        Raises a TypeError with message in case of failure.
        """
        _check_plugin_name(name)
        if type(self) is not PluginGroup:
            _check_plugin_name_prefix(name)

        # check correct base class of plugin, if stated
        if self.Plugin.plugin_class:
            check_is_subclass(name, plugin, self.Plugin.plugin_class)

    def check_plugin(self, name: str, plugin: Type[T]):
        """Perform plugin group specific checks on a registered plugin.

        Raises a TypeError with message in case of failure.

        To be overridden in subclasses for plugin group specific checks.

        Args:
            name: Declared entrypoint name.
            plugin: Object the entrypoint is pointing to.
        """
        if type(self) is not PluginGroup:
            return  # is not the "plugingroup" group itself

        # these are the checks done for other plugin group plugins:

        check_is_subclass(name, plugin, PluginGroup)
        check_is_subclass(name, self.Plugin.plugin_info_class, PluginBase)
        if plugin != PluginGroup:  # exclude itself. this IS its check_plugin
            check_implements_method(name, plugin, PluginGroup.check_plugin)

        # make sure that the declared plugin_info_class for the group sets 'group'
        # and it is also equal to the plugin group 'name'.
        # this is the safest way to make sure that Plugin.ref() works correctly.
        ppgi_cls = plugin.Plugin.plugin_info_class
        if not ppgi_cls.group:
            raise TypeError(f"{name}: {ppgi_cls} is missing 'group' attribute!")
        if not ppgi_cls.group == plugin.Plugin.name:
            msg = f"{name}: {ppgi_cls.__name__}.group != {plugin.__name__}.Plugin.name!"
            raise TypeError(msg)

    def init_plugin(self, name: str, plugin: Type[T]):
        """Do something after plugin has been checked."""
        if type(self) is not PluginGroup:
            return  # is not the "plugingroup" group itself
        create_pg(plugin)  # create plugin group if it does not exist


# ----

_plugin_groups: Dict[str, PluginGroup] = {}
"""Instances of initialized plugin groups."""


def create_pg(pg_cls):
    """Create plugin group instance if it does not exist."""
    pg_name = pg_cls.Plugin.name
    if pg_name in _plugin_groups:
        return _plugin_groups[pg_name]

    if not isinstance(pg_cls.Plugin, PluginBase):
        # magic - substitute Plugin class with parsed plugin object
        pg_cls.Plugin = PGPlugin.parse_info(pg_cls.Plugin)

    pg = pg_cls(get_group(pg_name))
    _plugin_groups[pg_name] = pg
