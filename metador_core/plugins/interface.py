"""Meta-interface for pluggable entities."""
from __future__ import annotations

import re
from typing import Dict, Generic, ItemsView, KeysView, Type, TypeVar, ValuesView

from ..schema.core import FullPluginRef, PluginPkgMeta

# group prefix for metador plugin entry point groups.
PGB_GROUP_PREFIX: str = "metador_"

T = TypeVar("T")


class PluginGroup(Generic[T]):
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

    def __init__(
        self, name: str, plugin_pkg: Dict[str, str], loaded_plugins: Dict[str, Type[T]]
    ):
        self._NAME = name
        self._PLUGIN_PKG = plugin_pkg
        self._LOADED_PLUGINS = loaded_plugins

    @property
    def name(self) -> str:
        return self._NAME

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

    def get(self, key: str, default=None) -> Type[T]:
        return self._LOADED_PLUGINS.get(key, default)

    def provider(self, ep_name: str) -> PluginPkgMeta:
        """Return package metadata of Python package providing this plugin."""
        pkg = self._PLUGIN_PKG.get(ep_name)
        if pkg is None:
            msg = f"Did not find package providing {self._NAME} plugin: {ep_name}"
            raise KeyError(msg)
        return self._PKG_META[pkg]

    def fullname(self, ep_name: str) -> FullPluginRef:
        pkginfo = self.provider(ep_name)
        return FullPluginRef(
            pkg=pkginfo.name,
            pkg_version=pkginfo.version,
            group=self.name,
            name=ep_name,
        )

    # ----

    @classmethod
    def _check_plugin_common(cls, ep_name: str, ep: Type[T]):
        """Perform common checks on a registered plugin.

        Raises a TypeError with message in case of failure.
        """
        if not re.fullmatch("[A-Za-z0-9_-]+", ep_name):
            msg = f"{ep_name}: Invalid pluggable name! Only use: A-z, a-z, 0-9, _ and -"
            raise TypeError(msg)

    def check_is_subclass(self, ep_name: str, ep: Type[T], pg_base: Type):
        """Check whether plugin has expected parent class (helper method)."""
        if not issubclass(ep, pg_base):
            msg = f"{ep_name}: {self.name} plugin not subclass of {pg_base}!"
            raise TypeError(msg)

    def _check(self, ep_name: str, ep: Type[T]):
        """Perform both the common and specific checks a registered plugin.

        Raises a TypeError with message in case of failure.
        """
        self._check_plugin_common(ep_name, ep)
        self.check_plugin(ep_name, ep)

    def check_plugin(self, ep_name: str, ep: Type[T]):
        """Perform pluggable-specific checks on a registered plugin.

        Raises a TypeError with message in case of failure.

        To be overridden in subclasses for plugin group specific checks.
        """
        if type(self) != PluginGroup:
            raise NotImplementedError
        self.check_is_subclass(ep_name, ep, PluginGroup)
