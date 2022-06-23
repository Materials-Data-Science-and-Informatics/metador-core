"""Base functionality for declaring validated plugin types for Metador."""
from __future__ import annotations

import re
from typing import Any, Dict, Optional

from importlib_metadata import entry_points
from typing_extensions import Final

from ..schema.core import PluginPkgMeta
from .interface import PGB_GROUP_PREFIX, Pluggable

# get all entrypoints
_eps = entry_points()

# dict: Package name -> Installed package metadata.
# Used as global cache for looking up info about required packages.
# (will be exposed through Pluggable)
_pgb_package_meta: Dict[str, PluginPkgMeta] = {}


def pkgmeta_from_dist(dist):
    """Extract required metadata from importlib_metadata distribution object."""
    ver = dist.version
    if not re.fullmatch("[0-9]+\\.[0-9]+\\.[0-9]+", ver):
        msg = f"Invalid version string of {dist.name}: {ver}"
        raise TypeError(msg)
    ver = tuple(map(int, ver.split(".")))

    epgs = filter(
        lambda x: x.startswith(PGB_GROUP_PREFIX),
        dist.entry_points.groups,
    )
    eps = {
        epg.lstrip(PGB_GROUP_PREFIX): list(
            map(lambda x: x.name, dist.entry_points.select(group=epg))
        )
        for epg in epgs
    }

    def is_repo_url(kv):
        return kv[0] == "Project-URL" and kv[1].startswith("Repository,")

    repo_url: Optional[str] = None
    try:
        repo_url = next(filter(is_repo_url, dist.metadata.items()))
        repo_url = repo_url[1].split()[1]  # from "Repository, http://..."
    except StopIteration:
        pass
    return PluginPkgMeta(
        name=dist.name, version=ver, plugins=eps, repository_url=repo_url
    )


class LoadedPlugin:
    """Runtime representation of a Metador plugin and its entrypoint.

    A plugin is defined in a pluggable entrypoint group located in some Python package.
    """

    pkg_name: str
    pgb_name: str
    ep_name: str
    ep: Any

    def __init__(self, pgb_name, ep):
        self.ep_name = ep.name
        self.pgb_name = pgb_name
        self.pkg_name = ep.dist.name
        self.ep = ep.load()

        if self.pkg_name not in _pgb_package_meta:
            # if that package metadata was not parsed yet, do it
            _pgb_package_meta[self.pkg_name] = pkgmeta_from_dist(ep.dist)

        self.pkg_meta = _pgb_package_meta[self.pkg_name]

    @classmethod
    def get_plugins(cls, pgb_name: str) -> Dict[str, LoadedPlugin]:
        """Get dict of all entrypoints of the subgroup metador_ARG."""
        grp = f"{PGB_GROUP_PREFIX}{pgb_name}"
        return {ep.name: LoadedPlugin(pgb_name, ep) for ep in _eps.select(group=grp)}


# initialize plugin discovery by getting pluggable groups (which are a pluggable).
PGB_PLUGGABLE: Final[str] = "pluggable"
_loaded_pluggables: Dict[str, Dict[str, LoadedPlugin]]
_loaded_pluggables = {PGB_PLUGGABLE: LoadedPlugin.get_plugins(PGB_PLUGGABLE)}
"""
Dict of Pluggable name -> Plugin entrypoint name -> Entrypoint info object.
"""


def project_eps(pgb_name, func):
    """Project out a component of the entry point objects of a pluggable group."""
    return {k: func(v) for k, v in _loaded_pluggables[pgb_name].items()}


# load actual registered pluggables
for pgb_name, pgb in _loaded_pluggables[PGB_PLUGGABLE].items():
    # check the pluggable itself
    Pluggable.check_plugin_common(pgb_name, pgb)
    if pgb_name in _loaded_pluggables:
        msg = f"Pluggable name already registered: '{pgb_name}' ({pgb})"
        raise TypeError(msg)
    if not issubclass(pgb.ep, Pluggable):
        msg = f"Pluggable must be parent class of pluggable: '{pgb_name}'"
        raise TypeError(msg)

    # load plugins for that pluggable
    _loaded_pluggables[pgb_name] = LoadedPlugin.get_plugins(pgb_name)
    for ep_name, ep in _loaded_pluggables[pgb_name].items():
        pgb.ep.check(ep_name, ep.ep)

    # on success attach the loaded ones to the class
    pgb.ep._NAME = pgb_name
    pgb.ep._LOADED_PLUGINS = project_eps(pgb_name, lambda v: v.ep)
    pgb.ep._PLUGIN_PKG = project_eps(pgb_name, lambda v: v.pkg_name)

# set up shared lookup for package metadata
Pluggable._NAME = PGB_PLUGGABLE
Pluggable._PKG_META = _pgb_package_meta

# Manually append the pluggable meta-interface as a proper pluggable itself
Pluggable._LOADED_PLUGINS = project_eps(PGB_PLUGGABLE, lambda v: v.ep)
Pluggable._LOADED_PLUGINS[PGB_PLUGGABLE] = Pluggable
Pluggable._PLUGIN_PKG = project_eps(PGB_PLUGGABLE, lambda v: v.pkg_name)
Pluggable._PLUGIN_PKG[PGB_PLUGGABLE] = Pluggable._PLUGIN_PKG["schema"]
