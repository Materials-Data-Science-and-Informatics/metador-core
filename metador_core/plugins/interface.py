"""Meta-interface for pluggable entities."""
from __future__ import annotations

import re
from typing import (
    Dict,
    Generic,
    ItemsView,
    KeysView,
    Optional,
    Type,
    TypeVar,
    Union,
    ValuesView,
    overload,
)

from overrides import EnforceOverrides, final

from ..schema.core import PluginPkgMeta, PluginRef

# group prefix for metador plugin entry point groups.
PGB_GROUP_PREFIX: str = "metador_"

# helpers for checking plugins (also to be used in PluginGroup subclasses):


def check_plugin_name(name: str):
    """Perform common checks on a registered plugin (applies to any plugin group).

    Raises a TypeError with message in case of failure.
    """
    if not re.fullmatch("[A-Za-z0-9_-]+", name):
        msg = f"{name}: Invalid pluggable name! Only use: A-z, a-z, 0-9, _ and -"
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


# ----


T = TypeVar("T")
DEF = TypeVar("DEF")


class PluginGroup(EnforceOverrides, Generic[T]):
    """All pluggable entities in metador are subclasses of this class.

    The type parameter is the (parent) class of all loaded plugins.

    They must implement the check method and be listed as plugin group.
    The name of their entrypoint defines the name of the plugin group.
    """

    _PKG_META: Dict[str, PluginPkgMeta] = {}
    """Mapping from package name to metadata of the package.

    Class attribute, shared with subclasses.
    """

    _NAME: str
    """Name of this pluggable group."""

    _LOADED_PLUGINS: Dict[str, Type[T]]
    """Dict from entry points to loaded plugins of that pluggable type."""

    _PLUGIN_PKG: Dict[str, str]
    """Dict from entry points to package name in environment providing them."""

    # @final # <- not working
    def __init__(
        self, name: str, plugin_pkg: Dict[str, str], loaded_plugins: Dict[str, Type[T]]
    ):
        self._NAME = name
        self._PLUGIN_PKG = plugin_pkg
        self._LOADED_PLUGINS = loaded_plugins

    @property
    # @final # <- not working
    def name(self) -> str:
        return self._NAME

    @property
    # @final # <- not working
    def packages(self) -> Dict[str, PluginPkgMeta]:
        """Return metadata of all packages providing metador plugins."""
        return dict(self._PKG_META)

    @final
    def keys(self) -> KeysView[str]:
        return self._LOADED_PLUGINS.keys()

    @final
    def values(self) -> ValuesView[Type[T]]:
        return self._LOADED_PLUGINS.values()

    @final
    def items(self) -> ItemsView[str, Type[T]]:
        return self._LOADED_PLUGINS.items()

    @final
    def __getitem__(self, key: str) -> Type[T]:
        return self._LOADED_PLUGINS[key]

    @overload
    def get(self, key: str) -> Optional[Type[T]]:
        ...

    @overload
    def get(self, key: str, default: DEF) -> Union[Type[T], DEF]:
        ...

    @final
    def get(self, key, default: Union[None, DEF] = None) -> Union[Type[T], DEF, None]:
        ret = self._LOADED_PLUGINS.get(key)
        if ret is None:
            return default
        return ret

    @final
    def provider(self, ep_name: str) -> PluginPkgMeta:
        """Return package metadata of Python package providing this plugin."""
        pkg = self._PLUGIN_PKG.get(ep_name)
        if pkg is None:
            msg = f"Did not find package providing {self._NAME} plugin: {ep_name}"
            raise KeyError(msg)
        return self._PKG_META[pkg]

    @final
    def fullname(self, ep_name: str) -> PluginRef:
        pkginfo = self.provider(ep_name)
        return PluginRef(
            pkg=pkginfo.name,
            pkg_version=pkginfo.version,
            group=self.name,
            name=ep_name,
        )

    # ----

    @final
    def _check(self, name: str, plugin: Type[T]):
        """Perform both the common and specific checks a registered plugin.

        Raises a TypeError with message in case of failure.
        """
        check_plugin_name(name)
        self.check_plugin(name, plugin)

    def check_plugin(self, name: str, plugin: Type[T]):
        """Perform plugin group specific checks on a registered plugin.

        Raises a TypeError with message in case of failure.

        To be overridden in subclasses for plugin group specific checks.

        Args:
            name: Declared entrypoint name.
            plugin: Object the entrypoint is pointing to.
        """
        # the default implementation is the one for the plugin group plugin group
        # and must make sure that all child plugin groups override it
        if id(type(self)) == id(PluginGroup):
            check_is_subclass(name, plugin, PluginGroup)
            if plugin != PluginGroup:
                check_implements_method(name, plugin, PluginGroup.check_plugin)
