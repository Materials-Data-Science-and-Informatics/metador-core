"""Meta-interface for pluggable entities."""
from __future__ import annotations

import re
from typing import (
    Any,
    Dict,
    Generic,
    ItemsView,
    KeysView,
    List,
    Optional,
    Type,
    TypeVar,
    Union,
    ValuesView,
    cast,
    overload,
)

from ..schema.core import PluginBase, PluginPkgMeta, PluginRef

# group prefix for metador plugin entry point groups.
PGB_GROUP_PREFIX: str = "metador_"
PG_GROUP_NAME = "plugingroup"


class PGPlugin(PluginBase):
    group: str = PG_GROUP_NAME
    required_plugin_groups: List[str]
    plugin_subclass: Any

    class Fields(PluginBase.Fields):
        required_plugin_groups: List[str]
        plugin_subclass: Any
        """List of plugin group names that must be loaded before this one.

        Should be added if plugins require plugins from those groups.
        """


# helpers for checking plugins (also to be used in PluginGroup subclasses):


def _check_plugin_name(name: str):
    """Perform common checks on a registered plugin (applies to any plugin group).

    Raises a TypeError with message in case of failure.
    """
    if not re.fullmatch("[A-Za-z0-9._-]+", name):
        msg = f"{name}: Invalid pluggable name! Only use: A-z, a-z, 0-9, _ and -"
        raise TypeError(msg)


def _check_plugin_valid(name: str, ep):
    """Check inner Plugin class that is required for all plugins."""
    # all plugins and other plugin groups:
    if not ep.__dict__.get("Plugin"):
        raise TypeError(f"{name}: {ep} is missing Plugin inner class!")
    check_is_subclass(name, ep.Plugin, PluginBase)
    ep.Plugin._check(ep_name=name)


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


# ----


T = TypeVar("T")


class PluginGroup(Generic[T]):
    """All pluggable entities in metador are subclasses of this class.

    The type parameter is the (parent) class of all loaded plugins.

    They must implement the check method and be listed as plugin group.
    The name of their entrypoint defines the name of the plugin group.
    """

    class Plugin(PGPlugin):
        name = PG_GROUP_NAME
        version = (0, 1, 0)
        required_plugin_groups: List[str] = []
        plugin_subclass = PGPlugin

    PRX = TypeVar("PRX", bound="Type[T]")  # type: ignore

    _PKG_META: Dict[str, PluginPkgMeta] = {}
    """Mapping from package name to metadata of the package.

    Class attribute, shared with subclasses.
    """

    _LOADED_PLUGINS: Dict[str, Type[T]]
    """Dict from entry points to loaded plugins of that pluggable type."""

    def __init__(self, loaded_plugins: Dict[str, Type[T]]):
        self._LOADED_PLUGINS = loaded_plugins

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
        return self._LOADED_PLUGINS[key]

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
        return cast(Any, self[ep_name]).Plugin.ref()

    def provider(self, ep_name: str) -> PluginPkgMeta:
        """Return package metadata of Python package providing this plugin."""
        pkg = cast(Any, self[ep_name]).Plugin._provided_by
        if pkg is None:
            msg = f"Did not find package providing {self.Plugin.name} plugin: {ep_name}"
            raise KeyError(msg)
        return self._PKG_META[pkg]

    # ----

    def _check(self, name: str, plugin):
        """Perform both the common and specific checks a registered plugin.

        Raises a TypeError with message in case of failure.
        """
        _check_plugin_name(name)
        # check and complete Plugin inner class
        if plugin != type(self):
            _check_plugin_valid(name, plugin)
            plugin.Plugin._group = self.name

        # run the (usually overridden) plugin validation
        self.check_plugin(name, plugin)

    def check_plugin(self, name: str, plugin: Type[T]):
        """Perform plugin group specific checks on a registered plugin.

        Raises a TypeError with message in case of failure.

        To be overridden in subclasses for plugin group specific checks.

        Args:
            name: Declared entrypoint name.
            plugin: Object the entrypoint is pointing to.
        """
        plugin_ = cast(Any, plugin)
        # the default implementation is the one for the plugin group plugin group
        # and must make sure that all child plugin groups override it
        if id(type(self)) == id(PluginGroup):
            check_is_subclass(name, plugin, PluginGroup)
            check_is_subclass(name, plugin_.Plugin, PGPlugin)

            if not plugin_.Plugin.plugin_subclass:
                raise TypeError(
                    f"'plugin_subclass' of {plugin_.Plugin} must be defined!"
                )
            check_is_subclass(name, plugin_.Plugin.plugin_subclass, PluginBase)
            if not hasattr(plugin_.Plugin.plugin_subclass, "group"):
                raise TypeError("PGPlugin subclass must set 'group'!")

            if plugin != PluginGroup:
                check_implements_method(name, plugin, PluginGroup.check_plugin)
