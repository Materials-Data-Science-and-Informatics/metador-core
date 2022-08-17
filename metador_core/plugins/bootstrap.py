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
        if ep.name in plugins:
            msg = f"{group_name}: a plugin named '{ep.name}' is already registered!"
            raise TypeError(msg)
        plugin = ep.load()
        # plugin.Plugin.group = group_name
        plugin.Plugin._provided_by = ep.dist.name
        plugins[ep.name] = plugin
        if ep.dist.name not in _plugin_pkgs:
            _plugin_pkgs.add(ep.dist.name)
            # _dist_meta[ep.dist.name] = distmeta_for(ep.dist)
    return plugins


# initialize plugin discovery by getting pluggable groups (which are a pluggable).
# PluginGroup name -> entrypoint name -> Entrypoint target
_loaded_pgroups: Dict[str, Dict[str, Any]] = {}


def _create_pgb_group(pg_cls):
    pg_name = pg_cls.Plugin.name
    plugins = get_plugins(pg_name)
    _loaded_pgroups[pg_name] = plugins
    installed[pg_name] = pg_cls(plugins)


def resolve_loading_order(pgs):
    # hack so schema plugin group is loaded first. cleaner + more general:
    # use topo sort based on pgb.required_plugin_groups (TODO)
    ret = list(pgs)
    ret.remove("schema")
    ret.insert(0, "schema")
    return ret


def load_plugins():
    # prepare the "plugin group plugin group", mother of all plugins
    _create_pgb_group(pg.PluginGroup)
    pgpg = _loaded_pgroups[pg.PG_GROUP_NAME]
    pgpg_inst = installed[pg.PG_GROUP_NAME]
    pgpg_inst._check(pg.PG_GROUP_NAME, pg.PluginGroup)
    pgpg_inst.post_load()

    # load groups in a reasonable order
    pg_order = resolve_loading_order(pgpg.keys())
    for pg_name in pg_order:
        pgroup = pgpg[pg_name]
        pgpg_inst._check(pg_name, pgroup)
        _create_pgb_group(pgroup)  # all other plugin groups

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

    # now can use the class structures and check the plugin validity for other groups:
    # (at this point all installed plugins of same kind can be cross-referenced)
    for pgb_name in pg_order:
        pgroup = installed[pgb_name]
        for ep_name, ep in pgroup.items():
            pgroup._check(ep_name, ep)
        pgroup.post_load()
