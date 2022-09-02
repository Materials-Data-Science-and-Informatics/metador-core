"""Central place to access all plugin groups."""
from typing import Dict, List, TypeVar, cast

import wrapt

from .interface import PG_GROUP_NAME, PluginGroup

S = TypeVar("S", bound=PluginGroup)


class PGPluginGroup(wrapt.ObjectProxy):
    """PluginGroup plugin group.

    Returns instances of other loaded plugin groups.
    """

    _self_groups: Dict[str, PluginGroup]

    def __init__(self):
        # initialize the meta-plugingroup
        from .interface import _plugin_groups, create_pg

        create_pg(PluginGroup)
        # wire it up with this wrapper
        self._self_groups = _plugin_groups
        self.__wrapped__ = _plugin_groups[PG_GROUP_NAME]

    # ----

    def __getitem__(self, key: str) -> PluginGroup:
        if key not in self:
            raise KeyError(f"{self.name} not found: {key}")
        return self.get(key)

    def get(self, key):
        """Get plugin group interface of a registered plugin group by name.

        The optional pg_class can be used for type checking purposes as follows:
        Instead of using `group("schema")`, use `group("schema", PGSchema)` and
        then the object can be properly type checked by static analyzers.
        """
        if key == PG_GROUP_NAME:
            return self
        if grp_cls := self.__wrapped__.get(key):
            # now if it was not existing, it is + stored in _self_groups
            return cast(S, self._self_groups.get(grp_cls.Plugin.name))


# access to available plugin groups:

plugingroups: PGPluginGroup = PGPluginGroup()

# some magic to lift all other groups to module level
# this allows to import like: from metador_core.plugins import schemas

__annotations__ = {}  # dynamically create annotations, maybe it helps?

# define what to import with *
__all__ = list(map(lambda n: f"{n}s", plugingroups.keys()))


def __dir__() -> List[str]:
    # show the existing plugin groups for tab completion
    return __all__


def __getattr__(key: str):
    # get desired plugin group and add as module attribute
    # (i.e. this is done once per group)
    if not isinstance(key, str) or key[-1] != "s":
        raise AttributeError(key)
    if group := plugingroups.get(key[:-1]):
        globals()["__annotations__"][key] = type(group)
        globals()[key] = group
        return group
    raise AttributeError(key)
