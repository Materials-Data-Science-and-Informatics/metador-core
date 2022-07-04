"""Meta-interface for pluggable interfaces."""
from __future__ import annotations

import re
from typing import Any, Dict

from ..schema.core import PluginPkgMeta, FullPluginRef

# group prefix for metador plugin entry point groups.
PGB_GROUP_PREFIX: str = "metador_"


class PluggableMetaclass(type):
    """Metaclass to provide dict-like interface to access plugins by name."""

    _LOADED_PLUGINS: Dict[str, Any]
    """Dict from entry points to loaded plugins of that pluggable type."""

    _NAME: str
    """Name of this pluggable group."""

    @property
    def name(self):
        return self._NAME

    def keys(self):
        return self._LOADED_PLUGINS.keys()

    def values(self):
        return self._LOADED_PLUGINS.values()

    def items(self):
        return self._LOADED_PLUGINS.items()

    def __getitem__(self, key):
        return self._LOADED_PLUGINS[key]

    def get(self, key, default=None):
        return self._LOADED_PLUGINS.get(key, default)


class Pluggable(metaclass=PluggableMetaclass):
    """All pluggable entities in metador are subclasses of this class.

    They must implement the check method and be listed as pluggable.
    The name of their entrypoint defines the name of the pluggable group.
    """
    _PLUGIN_PKG: Dict[str, str]
    """Dict from entry points to package name in environment providing them."""

    _PKG_META: Dict[str, PluginPkgMeta] = {}
    """Mapping from package name to metadata of the package.

    Shared with subclasses.
    """

    @classmethod
    def packages(cls) -> Dict[str, PluginPkgMeta]:
        """Return metadata of all packages providing metador plugins."""
        return dict(cls._PKG_META)

    @classmethod
    def provider(cls, ep_name: str) -> PluginPkgMeta:
        """Return package metadata of Python package providing this plugin."""
        pkg = cls._PLUGIN_PKG.get(ep_name)
        if pkg is None:
            msg = f"Did not find package providing {cls._NAME} plugin: {ep_name}"
            raise KeyError(msg)
        return cls._PKG_META[pkg]

    @classmethod
    def fullname(cls, ep_name) -> FullPluginRef:
        pkginfo = cls.provider(ep_name)
        return FullPluginRef(
            pkg=pkginfo.name,
            pkg_version=pkginfo.version,
            group=cls.name,
            name=ep_name,
        )

    # ----
    @classmethod
    def _check_plugin_common(cls, ep_name: str, ep: Any):
        """Perform common checks on a registered plugin.

        Raises a TypeError with message in case of failure.
        """
        if not re.fullmatch("[A-Za-z0-9_-]+", ep_name):
            msg = f"{ep_name}: Invalid pluggable name! Only use: A-z, a-z, 0-9, _ and -"
            raise TypeError(msg)

    @classmethod
    def _check(cls, ep_name: str, ep: Any):
        """Perform both the common and specific checks a registered plugin.

        Raises a TypeError with message in case of failure.
        """
        cls._check_plugin_common(ep_name, ep)
        cls.check_plugin(ep_name, ep)

    @classmethod
    def check_plugin(cls, ep_name: str, ep: Any):
        """Perform pluggable-specific checks on a registered plugin.

        Raises a TypeError with message in case of failure.

        To be overridden in subclasses for plugin group specific checks.
        """
        if cls != Pluggable:
            raise NotImplementedError

        # we want this to be checked for Pluggable groups, but not other subclasses
        if not issubclass(ep, Pluggable):
            msg = f"{ep_name}: {ep} must be subclass of {cls}"
            raise TypeError(msg)
