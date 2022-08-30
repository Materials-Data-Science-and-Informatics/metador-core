"""Meta-interface for pluggable entities."""
from __future__ import annotations

import re
from graphlib import TopologicalSorter
from typing import (
    Any,
    Dict,
    Generic,
    ItemsView,
    KeysView,
    Optional,
    Type,
    TypeVar,
    Union,
    ValuesView,
    cast,
    overload,
)

from ..schema.core import PluginBase, PluginPkgMeta
from ..schema.core import PluginRef as AnyPluginRef
from .entrypoints import get_plugins, plugin_pkgs

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
    if not hasattr(p_cls.Plugin, "_provided_by"):
        return False  # not loaded
    g = p_cls.Plugin.group
    return bool(g and (not group or group == g))


# ----

PG_GROUP_NAME = "plugingroup"


class PGPlugin(PluginBase):
    group = PG_GROUP_NAME
    plugin_info_class: Type[PluginBase]
    plugin_class: Optional[Any] = object


T = TypeVar("T")


class PluginGroupMeta(type):
    """Metaclass that will check a plugin group on creation."""

    def __init__(self, name, bases, dct):
        self.Plugin.plugin_info_class.group = self.Plugin.name
        self.PluginRef = AnyPluginRef.subclass_for(self.Plugin.name)


class PluginGroup(Generic[T], metaclass=PluginGroupMeta):
    """All pluggable entities in metador are subclasses of this class.

    The type parameter is the (parent) class of all loaded plugins.

    They must implement the check method and be listed as plugin group.
    The name of their entrypoint defines the name of the plugin group.
    """

    PluginRef: Any  # dynamic subclass of PluginRef

    class Plugin:
        """This is the plugin group plugin group, the first loaded group."""

        name = PG_GROUP_NAME
        version = (0, 1, 0)
        plugin_info_class = PGPlugin
        plugin_class: Type
        # plugin_class = PluginGroup  # can't set that -> check manually

    _PKG_META: Dict[str, PluginPkgMeta] = {}
    """Mapping from package name to metadata of the package.

    Class attribute, shared with subclasses.
    """

    _LOADED_PLUGINS: Dict[str, Type[T]] = {}
    """Dict from entry points to loaded plugins of that pluggable type."""

    PRX = TypeVar("PRX", bound="Type[T]")  # type: ignore

    def load(self):
        assert self._LOADED_PLUGINS
        self.pre_load()

        # load plugins in dependency-aligned order:
        def get_deps(plugin):
            return getattr(cast(Any, plugin).Plugin, "requires", [])

        pg_deps = {name: get_deps(plugin) for name, plugin in self.items()}
        for pg_name in TopologicalSorter(pg_deps).static_order():
            self._load_plugin(pg_name, self[pg_name])

        self.post_load()

    @property
    def name(self) -> str:
        return self.Plugin.name

    @property
    def packages(self) -> Dict[str, PluginPkgMeta]:
        """Return metadata of all packages providing metador plugins."""
        return dict(self._PKG_META)

    def keys(self) -> KeysView[str]:
        return self._LOADED_PLUGINS.keys()

    def values(self) -> ValuesView[Type[T]]:
        return self._LOADED_PLUGINS.values()

    def items(self) -> ItemsView[str, Type[T]]:
        return self._LOADED_PLUGINS.items()

    def __getitem__(self, key: str) -> Type[T]:
        if ret := self._LOADED_PLUGINS.get(key):
            return ret
        raise KeyError(f"{self.name} not found: {key}")

    @overload
    def get(self, key: str) -> Optional[Type[T]]:
        ...

    @overload
    def get(self, key: PRX) -> Optional[PRX]:
        ...

    def get(self, key: Union[str, PRX]) -> Union[Type[T], PRX, None]:
        passed_str = isinstance(key, str)
        key_: str = key if passed_str else key.Plugin.name  # type: ignore
        if ret := self._LOADED_PLUGINS.get(key_):
            if passed_str:
                return cast(Type[T], ret)
            else:
                return ret  # type: ignore
        else:
            return None

    def fullname(self, ep_name: str) -> PluginRef:
        plugin = self[ep_name]
        return self.PluginRef(name=ep_name, version=cast(Any, plugin).Plugin.version)

    def provider(self, ep_name: str) -> PluginPkgMeta:
        """Return package metadata of Python package providing this plugin."""
        pkg = cast(Any, self[ep_name]).Plugin._provided_by
        if pkg is None:
            msg = f"No package found providing this {self.name} plugin: {ep_name}"
            raise KeyError(msg)
        return self._PKG_META[pkg]

    # ----

    def _load_plugin(self, name: str, plugin):
        """Run checks and finalize loaded plugin."""
        self._check_common(name, plugin)
        self.check_plugin(name, plugin)
        self.init_plugin(plugin)

    def _check_common(self, name: str, plugin):
        """Perform both the common and specific checks a registered plugin.

        Raises a TypeError with message in case of failure.
        """
        _check_plugin_name(name)
        if type(self) is not PluginGroup:
            _check_plugin_name_prefix(name)

        if not plugin.__dict__.get("Plugin"):
            raise TypeError(f"{name}: {plugin} is missing Plugin inner class!")

        # run inner Plugin class checks (with possibly new Fields cls)
        plugin.Plugin = self.Plugin.plugin_info_class.parse_info(
            plugin.Plugin, ep_name=name
        )
        # check correct inheritance of plugin
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

        # if not hasattr(plugin, "PluginRef"):
        #     raise TypeError(f"{name}: {plugin} is missing 'PluginRef' attribute!")
        # pgref = cast(Any, plugin).PluginRef
        # check_is_subclass(name, pgref, PluginRef)
        # group_type = pgref.__fields__["group"].type_
        # t_const = get_origin(group_type)
        # t_args = get_args(group_type)
        # if t_const is not Literal or t_args != (name,):
        #     print(group_type)
        #     msg = f"{name}: PluginRef.group type must be Literal['{name}']!"
        #     raise TypeError(msg)

        # make sure that the declared plugin_info_class for the group sets 'group'
        # and it is also equal to the plugin group 'name'.
        # this is the safest way to make sure that Plugin.ref() works correctly.
        ppgi_cls = cast(Any, plugin).Plugin.plugin_info_class
        if not ppgi_cls.group:
            raise TypeError(f"{name}: {ppgi_cls} is missing 'group' attribute!")
        if not ppgi_cls.group == cast(Any, plugin).Plugin.name:
            msg = f"{name}: {ppgi_cls.__name__}.group != {plugin.__name__}.Plugin.name!"
            raise TypeError(msg)

    def init_plugin(self, plugin: Type[T]):
        """Do something after plugin has been checked."""
        if type(self) is not PluginGroup:
            return  # is not the "plugingroup" group itself

        if plugin is not PluginGroup:
            create_pg(plugin)

    def pre_load(self):
        """Do something before all plugins of this group are initialized."""
        if type(self) is not PluginGroup:
            return

        # the core package is also the one registering the "schema" plugin group.
        # Use that to hack in that it also provides the "plugingroup" plugin group:
        _this_pkg_name = self["schema"].Plugin._provided_by
        self.Plugin._provided_by = _this_pkg_name

        # add itself afterwards, so its not part of the plugin loading loop
        self._LOADED_PLUGINS[PG_GROUP_NAME] = PluginGroup

    def post_load(self):
        """Do something after all plugins of this group are initialized."""
        if type(self) is not PluginGroup:
            return

        # set up shared lookup for package metadata, shared by all plugin groups
        PluginGroup._PKG_META = {
            pkg: PluginPkgMeta.for_package(pkg) for pkg in plugin_pkgs
        }
        # fix up circularity (this package provides plugingroup as plugingroup)
        this_pkg = PluginGroup._PKG_META[PluginGroup.Plugin._provided_by]
        this_pkg.plugins[PG_GROUP_NAME][PG_GROUP_NAME] = self.PluginRef(
            name=PG_GROUP_NAME, version=self.Plugin.version
        )


# ----

_loaded_plugin_groups: Dict[str, PluginGroup] = {}


def create_pg(pg_cls):
    if not isinstance(pg_cls.Plugin, PluginBase):
        # magic - substitute Plugin class with parsed plugin object
        pg_cls.Plugin = PGPlugin.parse_info(pg_cls.Plugin)

    pg_name = pg_cls.Plugin.name

    if pg_name in _loaded_plugin_groups:
        return _loaded_plugin_groups[pg_name]

    pg = pg_cls()
    _loaded_plugin_groups[pg_name] = pg
    pg._LOADED_PLUGINS = get_plugins(pg_name)
    pg.load()


def load_plugins():
    """Initialize plugin system from currently available entry points."""
    if _loaded_plugin_groups:
        raise RuntimeError("Plugins have already been loaded and initialized!")

    from . import InstalledPlugins

    InstalledPlugins._installed = _loaded_plugin_groups
    create_pg(PluginGroup)
