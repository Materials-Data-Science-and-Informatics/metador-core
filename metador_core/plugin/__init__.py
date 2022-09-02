from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Optional,
    Type,
    TypeVar,
    Union,
    cast,
    overload,
)

import wrapt

if TYPE_CHECKING:
    from .interface import PluginGroup as _PluginGroup
else:
    _PluginGroup = Any

S = TypeVar("S", bound=_PluginGroup)


class PGPluginGroup(wrapt.ObjectProxy):
    """Class to provide access to all registered plugin groups via singleton instance.

    Includes some magic so we don't run into circular import problems so much,
    and in addition makes type checking possible for those who care.
    """

    _groups: Dict[str, _PluginGroup] = {}

    def __init__(self):
        from .interface import PG_GROUP_NAME, load_plugin_groups

        self._groups = load_plugin_groups()
        self.__wrapped__ = self._groups[PG_GROUP_NAME]

    # ----

    def __getitem__(self, key: str) -> _PluginGroup:
        if key not in self:
            raise KeyError(f"{self.name} not found: {key}")
        return self.get(key)

    # the get method takes the type of the returned PluginGroup as opt. second argument
    # see this nice trick: https://stackoverflow.com/a/60362860

    # get by group class
    @overload
    def get(self, key: Type[S]) -> S:
        ...

    # get by group name
    @overload
    def get(self, key: str) -> Optional[_PluginGroup]:
        ...

    def get(
        self,
        key: Union[str, Type[S]],
    ) -> Union[S, _PluginGroup]:
        """Get plugin group interface of a registered plugin group by name.

        The optional pg_class can be used for type checking purposes as follows:
        Instead of using `group("schema")`, use `group("schema", PGSchema)` and
        then the object can be properly type checked by static analyzers.
        """
        from .interface import PG_GROUP_NAME

        if key == PG_GROUP_NAME:
            return self

        grp_cls = self.__wrapped__.get(key)  # force load plugin group (if not done)
        return cast(S, self._groups[grp_cls.Plugin.name])


# To be imported for access to installed plugins
plugingroups: PGPluginGroup = PGPluginGroup()
"""Installed metador plugin groups, to be imported in code to access plugins."""
