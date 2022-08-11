"""Base functionality for declaring validated plugin types for Metador."""
from __future__ import annotations

from typing import Any, Dict

from importlib_metadata import entry_points

from ..schema.core import PluginPkgMeta
from . import InstalledPlugins
from . import interface as pg

installed = InstalledPlugins._installed  # just for convenience

# get all entrypoints
_eps = entry_points()

# Collected package names providing plugins
_plugin_pkgs = set()


def get_plugins(group_name: str) -> Dict[str, Any]:
    """Get dict of all entrypoints of the subgroup metador_ARG."""
    grp = f"{pg.PGB_GROUP_PREFIX}{group_name}"
    plugins: Dict[str, Any] = {}
    for ep in _eps.select(group=grp):
        plugin = ep.load()
        plugins[ep.name] = plugin
        plugin.Plugin._provided_by = ep.dist.name
        if ep.dist.name not in _plugin_pkgs:
            _plugin_pkgs.add(ep.dist.name)
            # _dist_meta[ep.dist.name] = distmeta_for(ep.dist)
    return plugins


# initialize plugin discovery by getting pluggable groups (which are a pluggable).
# PluginGroup name -> entrypoint name -> Entrypoint target
_loaded_pgroups: Dict[str, Dict[str, Any]]


def _create_pgb_group(pg_cls):
    pg_name = pg_cls.Plugin.name
    installed[pg_name] = pg_cls(_loaded_pgroups[pg_name])


def load_plugins():
    global _loaded_pgroups
    _loaded_pgroups = {pg.PG_GROUP_NAME: get_plugins(pg.PG_GROUP_NAME)}
    pgpg = _loaded_pgroups[pg.PG_GROUP_NAME]  # "plugin group plugin group"
    _create_pgb_group(pg.PluginGroup)

    # hack so schema plugin group is loaded first. cleaner + more general:
    # use topo sort based on pgb.required_plugin_groups (TODO)
    group_loading_order = list(pgpg.keys())
    group_loading_order.remove("schema")
    group_loading_order.insert(0, "schema")

    # load plugin groups in suitable order
    for pgb_name in group_loading_order:
        pgb = pgpg[pgb_name]

        # check the pluggable itself
        pg._check_plugin_name(pgb_name)
        if pgb_name in _loaded_pgroups:
            msg = f"{pgb_name}: this {pg.PG_GROUP_NAME} name already registered!"
            raise TypeError(msg)

        # load plugins for that pluggable
        _loaded_pgroups[pgb_name] = get_plugins(pgb_name)
        # attach the loaded group to the module where they will be imported from
        _create_pgb_group(pgb)

    # the core package is the one registering the "schema" plugin group.
    # Use that knowledge to hack in that this package also provides the plugingroup plugin:
    _this_pkg_name = _loaded_pgroups[pg.PG_GROUP_NAME]["schema"].Plugin._provided_by
    pg.PluginGroup.Plugin._provided_by = _this_pkg_name

    # set up shared lookup for package metadata, shared by all plugin groups
    pg.PluginGroup._PKG_META = {
        pkg: PluginPkgMeta.for_package(pkg) for pkg in _plugin_pkgs
    }

    # circularity hack (this package provides plugingroup as plugingroup)
    _loaded_pgroups[pg.PG_GROUP_NAME][pg.PG_GROUP_NAME] = pg.PluginGroup
    pg.PluginGroup._PKG_META[_this_pkg_name].plugins[pg.PG_GROUP_NAME][
        pg.PG_GROUP_NAME
    ] = pg.PluginGroup.Plugin.ref()

    # now can use the class structures safely and check the plugingroup specifics:
    # (at this point all installed plugins of same kind can be cross-referenced)
    for pgb_name in _loaded_pgroups.keys():
        pgroup = installed[pgb_name]
        for ep_name, ep in pgroup.items():
            pgroup._check(ep_name, ep)
            pg.check_is_subclass(ep_name, ep.Plugin, pgroup.Plugin.plugin_subclass)
