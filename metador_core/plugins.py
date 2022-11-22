"""Central place to access all plugin groups."""
from typing import TYPE_CHECKING, Dict, List, TypeVar, cast

import wrapt

from .plugin.interface import PG_GROUP_NAME, PluginGroup

S = TypeVar("S", bound=PluginGroup)


class PGPluginGroup(wrapt.ObjectProxy):
    """PluginGroup plugin group.

    This wrapper returns instances of other loaded plugin groups.

    To access the actual plugingroup class that gives out *classes*
    instead of instances (like all other plugingroups),
    request the "plugingroup" plugingroup.
    """

    _self_groups: Dict[str, PluginGroup]

    def __init__(self):
        # initialize the meta-plugingroup
        from .plugin.interface import _plugin_groups, create_pg

        create_pg(PluginGroup)

        # wire it up with this wrapper
        self._self_groups = _plugin_groups
        self.__wrapped__ = _plugin_groups[PG_GROUP_NAME]

    # ----

    def __getitem__(self, key) -> PluginGroup:
        if ret := self.get(key):
            return ret
        raise KeyError(f"{self.name} not found: {key}")

    def get(self, key, version=None):
        """Get a registered plugin group by name."""
        if key == PG_GROUP_NAME:
            return self
        try:
            if grp_cls := self.__wrapped__._get_unsafe(key, version):
                # now if it was not existing, it is + stored in _self_groups
                return cast(S, self._self_groups.get(grp_cls.Plugin.name))
        except KeyError:
            return None


# access to available plugin groups:

plugingroups: PGPluginGroup = PGPluginGroup()
plugingroup_classes = plugingroups.__wrapped__

# help mypy (obviously only for groups in this package):
# NOTE: this would be better: https://github.com/python/mypy/issues/13643
if TYPE_CHECKING:
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
__all__ = list(map(lambda n: f"{n}s", plugingroups.keys()))


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
