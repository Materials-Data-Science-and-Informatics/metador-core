from typing import TYPE_CHECKING, Any, Dict, Type, TypeVar, Union, cast, overload

import lazy_object_proxy

if TYPE_CHECKING:
    from .interface import PluginGroup as _PluginGroup
else:
    _PluginGroup = Any

S = TypeVar("S", bound=_PluginGroup)


class InstalledPlugins:
    """Class to provide access to all registered plugin groups via singleton instance.

    Includes some magic so we don't run into circular import problems so much,
    and in addition makes type checking possible for those who care.
    """

    _installed: Dict[str, _PluginGroup] = {}
    # will be filled by plugins.bootstrap module

    def keys(self):
        from .interface import PG_GROUP_NAME

        return self._installed[PG_GROUP_NAME].keys()

    def __getitem__(self, key: str) -> _PluginGroup:
        return self.get(key)

    # the get method takes the type of the returned PluginGroup as opt. second argument
    # see this nice trick: https://stackoverflow.com/a/60362860

    # get by plugin class
    @overload
    def get(self, key: Type[S]) -> S:
        ...

    # get by plugin name (with pg_class indicating type)
    @overload
    def get(self, key: str, pg_class: Type[S]) -> S:
        ...

    # get by plugin name (without pg_class -> more general type)
    @overload
    def get(
        self, key: str, pg_class: Type[_PluginGroup] = _PluginGroup
    ) -> _PluginGroup:
        ...

    def get(
        self,
        key: Union[str, Type[S]],
        pg_class: Union[Type[S], Type[_PluginGroup]] = _PluginGroup,
    ) -> Union[S, _PluginGroup]:
        """Get plugin group interface of a registered plugin group by name.

        The optional pg_class can be used for type checking purposes as follows:
        Instead of using `group("schema")`, use `group("schema", PGSchema)` and
        then the object can be properly type checked by static analyzers.
        """
        # wrap in lazy_object_proxy because at initialization time might not exist yet
        key = key if isinstance(key, str) else key.Plugin.name

        def get_group():
            from .interface import PG_GROUP_NAME

            self._installed[PG_GROUP_NAME][key]  # force initializing the group
            return self._installed[key]

        return cast(S, lazy_object_proxy.Proxy(get_group))


# To be imported for access to installed plugins
plugingroups: InstalledPlugins = InstalledPlugins()
"""Installed metador plugin groups, to be imported in code to access plugins."""


def load_plugins():
    """Run to initialize Metador plugins."""
    from .interface import load_plugins

    return load_plugins()
