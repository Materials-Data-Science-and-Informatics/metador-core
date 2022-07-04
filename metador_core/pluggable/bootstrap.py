"""Base functionality for declaring validated plugin types for Metador."""
from __future__ import annotations

from typing import Any, Dict

from importlib_metadata import entry_points, distribution
from typing_extensions import Final

from ..schema.core import PluginPkgMeta
from .interface import PGB_GROUP_PREFIX, Pluggable
from .utils import pkgmeta_from_dist

# get all entrypoints
_eps = entry_points()

# dict: Package name -> Installed package metadata.
# Used as global cache for looking up info about required packages.
# (will be exposed through Pluggable)
_pgb_package_meta: Dict[str, PluginPkgMeta] = {}


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


def _project_eps(pgb_name, func):
    """Project out a component of the entry point objects of a pluggable group."""
    return {k: func(v) for k, v in _loaded_pluggables[pgb_name].items()}


# load actual registered pluggables
for pgb_name, pgb in _loaded_pluggables[PGB_PLUGGABLE].items():
    # check the pluggable itself
    Pluggable._check_plugin_common(pgb_name, pgb)
    if pgb_name in _loaded_pluggables:
        msg = f"{pgb_name}: Pluggable name already registered"
        raise TypeError(msg)

    # load plugins for that pluggable
    _loaded_pluggables[pgb_name] = LoadedPlugin.get_plugins(pgb_name)
    # attach the loaded ones to the class
    pgb.ep._NAME = pgb_name
    pgb.ep._LOADED_PLUGINS = _project_eps(pgb_name, lambda v: v.ep)
    pgb.ep._PLUGIN_PKG = _project_eps(pgb_name, lambda v: v.pkg_name)

# set up shared lookup for package metadata, shared by all plugin groups
Pluggable._NAME = PGB_PLUGGABLE
Pluggable._PKG_META = _pgb_package_meta

# the core package is the one registering the "schema" plugin group.
# Use that knowledge to hack in that this package also provides "pluggable":
_this_pkg_name = _loaded_pluggables["pluggable"]["schema"].pkg_name
_pgb_package_meta[_this_pkg_name].plugins[PGB_PLUGGABLE].append(PGB_PLUGGABLE)

# take care of setting up the Pluggable group object, as its not fully initialized yet:

# Manually append the pluggable meta-interface as a proper pluggable itself
Pluggable._LOADED_PLUGINS = _project_eps(PGB_PLUGGABLE, lambda v: v.ep)
Pluggable._LOADED_PLUGINS[PGB_PLUGGABLE] = Pluggable
Pluggable._PLUGIN_PKG = _project_eps(PGB_PLUGGABLE, lambda v: v.pkg_name)
# register this package as provider of pluggables (this package actually registers schema)
Pluggable._PLUGIN_PKG[PGB_PLUGGABLE] = _this_pkg_name

# now can use the class structures safely:

# check the plugins according to pluggable rules
# (at this point all installed plugins of same kind can be cross-referenced)
for pgb_name in _loaded_pluggables.keys():
    pgb = Pluggable[pgb_name]  # type: ignore
    for ep_name, ep in pgb.items():
        pgb._check(ep_name, ep)
