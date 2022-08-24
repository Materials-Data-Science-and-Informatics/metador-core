"""Metador plugin loading from entry points."""
from __future__ import annotations

from typing import Any, Dict

from importlib_metadata import entry_points

# group prefix for metador plugin entry point groups.
PG_PREFIX: str = "metador_"

# get all entrypoints
_eps = entry_points()

# Collected package names that provide plugins (filled by get_plugins)
plugin_pkgs = set()


def get_plugins(group_name: str) -> Dict[str, Any]:
    """Get dict of all available entrypoints for a Metador plugin group."""
    ep_grp = f"{PG_PREFIX}{group_name}"
    plugins: Dict[str, Any] = {}

    for ep in _eps.select(group=ep_grp):

        if ep.name in plugins:
            msg = f"{group_name}: a plugin named '{ep.name}' is already registered!"
            raise TypeError(msg)

        plugin = ep.load()
        plugin.Plugin._provided_by = ep.dist.name

        plugins[ep.name] = plugin
        if ep.dist.name not in plugin_pkgs:
            plugin_pkgs.add(ep.dist.name)

    return plugins
