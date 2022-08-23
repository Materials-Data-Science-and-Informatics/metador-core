"""Meta-interface for pluggable entities."""
from __future__ import annotations

import re
from typing import (
    Any,
    Dict,
    Generic,
    ItemsView,
    KeysView,
    List,
    Literal,
    Optional,
    Type,
    TypeVar,
    Union,
    ValuesView,
    cast,
    overload,
)

from typing_extensions import get_args, get_origin

from ..schema.core import PluginBase, PluginPkgMeta, PluginRef

# group prefix for metador plugin entry point groups.
PGB_GROUP_PREFIX: str = "metador_"

PG_GROUP_NAME = "plugingroup"


class PGPluginRef(PluginRef):
    group: Literal["plugingroup"]


class PGPlugin(PluginBase):
    group = PG_GROUP_NAME

    plugin_class: Any = object
    plugin_info_class: Type[PluginBase]

    required_plugin_groups: List[str]

    class Fields(PluginBase.Fields):
        plugin_class: Optional[Any]
        plugin_info_class: Any
        required_plugin_groups: List[str]
        """List of plugin group names that must be loaded before this one.

        Should be added if plugins require plugins from those groups.
        """


# helpers for checking plugins (also to be used in PluginGroup subclasses):


def _check_plugin_name(name: str):
    if not re.fullmatch("[A-Za-z0-9._-]+", name):
        msg = f"{name}: Invalid plugin name! Only use: A-z, a-z, 0-9, _ - and ."
        raise TypeError(msg)


def _check_plugin_name_prefix(name: str):
    xs = name.split(".")
    if len(xs) < 2 or not (2 < len(xs[0]) < 11):
        msg = f"{name}: Missing/invalid namespace prefix (must have length 3-10)!"
        raise TypeError(msg)


def check_is_subclass(name: str, plugin, base):
    """Check whether plugin has expected parent class (helper method)."""
    if not issubclass(plugin, base):
        msg = f"{name}: {plugin} is not subclass of {base}!"
        raise TypeError(msg)


def test_implements_method(plugin, base_method):
    ep_method = plugin.__dict__.get(base_method.__name__)
    return ep_method is not None and base_method != ep_method


def check_implements_method(name: str, plugin, base_method):
    """Check whether plugin overrides a method of its superclass."""
    if not test_implements_method(plugin, base_method):
        msg = f"{name}: {plugin} does not implement {base_method.__name__}!"
        raise TypeError(msg)


def is_plugin(p_cls, *, group: str = ""):
    """Check whether given class is a loaded plugin.

    If group not specified, will accept any kind of plugin.

    Args:
        p_cls: class of supposed plugin
        group: name of desired plugin group
    """
    if not hasattr(p_cls, "Plugin") or not issubclass(p_cls.Plugin, PluginBase):
        return False  # not suitable
    if not hasattr(p_cls.Plugin, "_provided_by"):
        return False  # not loaded
    g = p_cls.Plugin.group
    return bool(g and (not group or group == g))


# ----


T = TypeVar("T")


class PluginGroup(Generic[T]):
    """All pluggable entities in metador are subclasses of this class.

    The type parameter is the (parent) class of all loaded plugins.

    They must implement the check method and be listed as plugin group.
    The name of their entrypoint defines the name of the plugin group.
    """

    class Plugin(PGPlugin):
        """This is the plugin group plugin group, the first loaded group."""

        name = PG_GROUP_NAME
        version = (0, 1, 0)
        required_plugin_groups: List[str] = []
        plugin_info_class = PGPlugin

    class PluginRef(PGPluginRef):
        """PluginRef where 'group' is fixed to this plugin group."""

    _PKG_META: Dict[str, PluginPkgMeta] = {}
    """Mapping from package name to metadata of the package.

    Class attribute, shared with subclasses.
    """

    _LOADED_PLUGINS: Dict[str, Type[T]]
    """Dict from entry points to loaded plugins of that pluggable type."""

    PRX = TypeVar("PRX", bound="Type[T]")  # type: ignore

    def __init__(self, loaded_plugins: Dict[str, Type[T]]):
        self._LOADED_PLUGINS = loaded_plugins

    @property
    def name(self) -> str:
        return self.Plugin.name

    @property
    def packages(self) -> Dict[str, PluginPkgMeta]:
        """Return metadata of all packages providing metador plugins."""
        return dict(self._PKG_META)

    def keys(self) -> KeysView[str]:
        return self._LOADED_PLUGINS.keys()

    def values(self) -> ValuesView[Type[T]]:
        return self._LOADED_PLUGINS.values()

    def items(self) -> ItemsView[str, Type[T]]:
        return self._LOADED_PLUGINS.items()

    def __getitem__(self, key: str) -> Type[T]:
        if ret := self._LOADED_PLUGINS.get(key):
            return ret
        raise KeyError(f"{self.name} not found: {key}")

    @overload
    def get(self, key: str) -> Optional[Type[T]]:
        ...

    @overload
    def get(self, key: PRX) -> Optional[PRX]:
        ...

    def get(self, key: Union[str, PRX]) -> Union[Type[T], PRX, None]:
        passed_str = isinstance(key, str)
        key_: str = key if passed_str else key.Plugin.name  # type: ignore
        if ret := self._LOADED_PLUGINS.get(key_):
            if passed_str:
                return cast(Type[T], ret)
            else:
                return ret  # type: ignore
        else:
            return None

    def fullname(self, ep_name: str) -> PluginRef:
        return cast(Any, self[ep_name]).Plugin.ref()

    def provider(self, ep_name: str) -> PluginPkgMeta:
        """Return package metadata of Python package providing this plugin."""
        pkg = cast(Any, self[ep_name]).Plugin._provided_by
        if pkg is None:
            msg = f"No package found providing this {self.name} plugin: {ep_name}"
            raise KeyError(msg)
        return self._PKG_META[pkg]

    # ----

    def _check(self, name: str, plugin):
        """Perform both the common and specific checks a registered plugin.

        Raises a TypeError with message in case of failure.
        """
        _check_plugin_name(name)
        if id(type(self)) != id(PluginGroup):
            _check_plugin_name_prefix(name)

        if not plugin.__dict__.get("Plugin"):
            raise TypeError(f"{name}: {plugin} is missing Plugin inner class!")

        # check inner Plugin class checks (with possibly new Fields cls)
        pgi_cls = self.Plugin.plugin_info_class
        check_is_subclass(name, pgi_cls, PluginBase)
        check_is_subclass(name, plugin.Plugin, pgi_cls)
        plugin.Plugin._check(ep_name=name)

        # check correct inheritance for plugin info
        if pg_cls := self.Plugin.plugin_class:
            check_is_subclass(name, plugin, pg_cls)

        # run the (usually overridden) plugin validation
        self.check_plugin(name, plugin)

    def check_plugin(self, name: str, plugin: Type[T]):
        """Perform plugin group specific checks on a registered plugin.

        Raises a TypeError with message in case of failure.

        To be overridden in subclasses for plugin group specific checks.

        Args:
            name: Declared entrypoint name.
            plugin: Object the entrypoint is pointing to.
        """
        if type(self) is not PluginGroup:
            return  # called not for "plugin group plugin group"

        # these are the checks done for PluginGroup plugins (schema, etc.)
        check_is_subclass(name, plugin, PluginGroup)
        if plugin != PluginGroup:  # exclude itself. this IS its check_plugin
            check_implements_method(name, plugin, PluginGroup.check_plugin)

        # check attached subclass of PluginRef
        if not hasattr(plugin, "PluginRef"):
            raise TypeError(f"{name}: {plugin} is missing 'PluginRef' attribute!")
        check_is_subclass(name, cast(Any, plugin).PluginRef, PluginRef)
        group_type = cast(Any, plugin).PluginRef.__fields__["group"].type_
        t_const = get_origin(group_type)
        t_args = get_args(group_type)
        if t_const is not Literal or t_args != (name,):
            msg = f"{name}: {plugin}.PluginRef.group type must be Literal['{name}']!"
            raise TypeError(msg)

        # make sure that the declared plugin_info_class for the group sets 'group',
        # and it is also equal to the plugin group 'name'.
        # this is the safest way to make sure that Plugin.ref() works correctly.
        ppgi_cls = cast(Any, plugin).Plugin.plugin_info_class
        if not hasattr(ppgi_cls, "group"):
            raise TypeError(f"{name}: {ppgi_cls} is missing 'group' attribute!")
        if not ppgi_cls.group == cast(Any, plugin).Plugin.name:
            msg = f"{name}: {ppgi_cls.__name__}.group != {plugin.__name__}.Plugin.name!"
            raise TypeError(msg)

    def post_load(self):
        """Do something after all plugins of a group are loaded."""
