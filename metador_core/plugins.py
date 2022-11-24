"""Central place to access all plugin groups."""
from typing import TYPE_CHECKING, Dict, List, TypeVar, cast

import wrapt

from .plugin.interface import PG_GROUP_NAME, AnyPluginRef, PluginGroup, plugin_args

S = TypeVar("S", bound=PluginGroup)


class PGPluginGroup(wrapt.ObjectProxy):
    """PluginGroup plugin group.

    This wrapper returns instances of other loaded plugin groups.

    To access the actual plugingroup class that gives out *classes*
    instead of instances (like all other plugingroups),
    request the "plugingroup" plugingroup.
    """

    _self_groups: Dict[AnyPluginRef, PluginGroup]

    def __reset__(self):
        self._self_groups.clear()
        self.__init__()

    def __init__(self):
        # initialize the meta-plugingroup
        from .plugin.interface import _plugin_groups, create_pg

        create_pg(PluginGroup)
        pgpg_ref = AnyPluginRef(
            group=PG_GROUP_NAME,
            name=PluginGroup.Plugin.name,
            version=PluginGroup.Plugin.version,
        )

        # wire it up with this wrapper
        self._self_groups = _plugin_groups
        self.__wrapped__ = _plugin_groups[pgpg_ref]

    # ----
    def get(self, key, version=None):
        """Get a registered plugin group by name."""
        key_, vers = plugin_args(key, version)
        if key_ == self.name and (vers is None or vers == self.Plugin.version):
            return self
        try:
            if grp_cls := self.__wrapped__._get_unsafe(key_, vers):
                # now if the PG was not existing, it is + is stored in _self_groups
                return cast(S, self._self_groups.get(grp_cls.Plugin.ref()))
        except KeyError:
            return None

    def __getitem__(self, key) -> PluginGroup:
        # call wrapped '__getitem__' with this object to use its 'get'
        return PluginGroup.__getitem__(self, key)  # type: ignore

    def values(self):
        # same idea, this uses '__getitem__'
        return PluginGroup.values(self)

    def items(self):
        # same idea, this uses '__getitem__'
        return PluginGroup.items(self)

    def is_plugin(self, obj):
        return obj in self.values()


# access to available plugin groups:

plugingroups: PGPluginGroup = PGPluginGroup()
plugingroup_classes = plugingroups.__wrapped__

# help mypy (obviously only for groups in this package):
# NOTE: this would be better: https://github.com/python/mypy/issues/13643
if TYPE_CHECKING:  # pragma: no cover
    from .harvester import PGHarvester
    from .packer import PGPacker
    from .schema.pg import PGSchema
    from .widget import PGWidget

    schemas: PGSchema
    harvesters: PGHarvester
    widgets: PGWidget
    packers: PGPacker

# ----
# Now some magic to lift all other groups to module level,
# this allows to import like: from metador_core.plugins import schemas

# define what to import with *
__all__ = list(sorted(map(lambda ref: f"{ref.name}s", plugingroups.keys())))


def __dir__() -> List[str]:
    # show the existing plugin groups for tab completion
    return __all__


def __getattr__(key: str):
    # get desired plugin group and add as module attribute
    # (i.e. this is called once per group)
    if not isinstance(key, str) or key[-1] != "s":
        raise AttributeError(key)
    if group := plugingroups.get(key[:-1]):
        globals()["__annotations__"][key] = type(group)
        globals()[key] = group
        return group
    raise AttributeError(key)
