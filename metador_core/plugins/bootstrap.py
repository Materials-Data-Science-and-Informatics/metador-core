"""Base functionality for declaring validated plugin types for Metador."""
from __future__ import annotations

from typing import Any, Dict

from importlib_metadata import entry_points
from typing_extensions import Final

from ..schema.core import PluginPkgMeta
from . import installed
from .interface import PGB_GROUP_PREFIX, PluginGroup
from .utils import pkgmeta_from_dist

# get all entrypoints
_eps = entry_points()

# dict: Package name -> Installed package metadata.
# Used as global cache for looking up info about required packages.
# (will be exposed through PluginGroup)
_pgb_package_meta: Dict[str, PluginPkgMeta] = {}


class LoadedPlugin:
    """Runtime representation of a Metador plugin and its entrypoint.

    A plugin is defined in an entrypoint group located in some Python package.
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
PGB_PLUGGABLE: Final[str] = "plugin_group"
_loaded_pluggables: Dict[str, Dict[str, LoadedPlugin]]
_loaded_pluggables = {PGB_PLUGGABLE: LoadedPlugin.get_plugins(PGB_PLUGGABLE)}
"""
Dict of PluginGroup name -> Plugin entrypoint name -> Entrypoint info object.
"""


def _project_eps(pgb_name, func):
    """Project out a component of the entry point objects of a pluggable group."""
    return {k: func(v) for k, v in _loaded_pluggables[pgb_name].items()}


def _create_pgb_group(pgb_name, pgb_cls):
    installed[pgb_name] = pgb_cls(
        pgb_name,
        _project_eps(pgb_name, lambda v: v.pkg_name),
        _project_eps(pgb_name, lambda v: v.ep),
    )


# load actual registered pluggables
for pgb_name, pgb in _loaded_pluggables[PGB_PLUGGABLE].items():
    # check the pluggable itself
    PluginGroup._check_plugin_common(pgb_name, pgb.ep)
    if pgb_name in _loaded_pluggables:
        msg = f"{pgb_name}: PluginGroup name already registered"
        raise TypeError(msg)

    # load plugins for that pluggable
    _loaded_pluggables[pgb_name] = LoadedPlugin.get_plugins(pgb_name)

    # attach the loaded group to the module where they will be imported from
    _create_pgb_group(pgb_name, pgb.ep)

# set up shared lookup for package metadata, shared by all plugin groups
PluginGroup._PKG_META = _pgb_package_meta
# add plugin group group itself

# the core package is the one registering the "schema" plugin group.
# Use that knowledge to hack in that this package also provides "pluggable":
_this_pkg_name = _loaded_pluggables[PGB_PLUGGABLE]["schema"].pkg_name
_pgb_package_meta[_this_pkg_name].plugins[PGB_PLUGGABLE].append(PGB_PLUGGABLE)

# take care of setting up the PluginGroup group object, as its not fully initialized yet:
_create_pgb_group(PGB_PLUGGABLE, PluginGroup)
# Manually append the pluggable meta-interface as a proper pluggable itself
installed[PGB_PLUGGABLE]._LOADED_PLUGINS[PGB_PLUGGABLE] = PluginGroup
# register this package as provider of pluggables (this package actually registers schema)
installed[PGB_PLUGGABLE]._PLUGIN_PKG[PGB_PLUGGABLE] = _this_pkg_name

# now can use the class structures safely:

# check the plugins according to pluggable rules
# (at this point all installed plugins of same kind can be cross-referenced)
for pgb_name in _loaded_pluggables.keys():
    pgroup: PluginGroup = installed[pgb_name]
    for ep_name, ep in pgroup.items():
        pgroup._check(ep_name, ep)
