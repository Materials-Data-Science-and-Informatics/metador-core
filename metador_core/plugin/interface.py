"""Interface for plugin groups."""
from __future__ import annotations

import re
from abc import ABCMeta
from importlib.metadata import EntryPoint
from typing import (
    Any,
    ClassVar,
    Dict,
    Generic,
    Iterable,
    KeysView,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
    overload,
)

from typing_extensions import TypeAlias

from metador_core.schema.types import SemVerTuple, semver_str

from ..schema.plugins import IsPlugin, PluginBase, PluginPkgMeta
from ..schema.plugins import PluginRef as AnyPluginRef
from .entrypoints import get_group, pkg_meta
from .metaclass import UndefVersion

PG_GROUP_NAME = "plugingroup"

# helpers for checking plugins (also to be used in PluginGroup subclasses):

PLUGIN_NAME_REGEX = r"[A-Za-z0-9._-]+"
"""Regular expression that all metador plugins must match."""

ENTRYPOINT_NAME_REGEX = rf"{PLUGIN_NAME_REGEX}__\d+\.\d+\.\d+"
"""Regular expression that all metador plugin entry points must match."""


def _check_name(name: str, pat: str):
    if not re.fullmatch(pat, name):
        msg = f"{name}: Invalid name, must match {pat}"
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
    g = p_cls.Plugin.group
    return bool(g and (not group or group == g))


# ----


EP_NAME_VER_SEP: str = "__"
"""Separator between plugin name and semantic version in entry point name."""


def _to_ep_name(p_name: str, p_version: Optional[SemVerTuple] = None) -> str:
    """Return canonical entrypoint name `PLUGIN_NAME__MAJ.MIN.FIX`."""
    return f"{p_name}{EP_NAME_VER_SEP}{semver_str(p_version)}"


def _from_ep_name(ep_name: str) -> Tuple[str, SemVerTuple]:
    """Split entrypoint name into `(PLUGIN_NAME, (MAJ,MIN,FIX))`."""
    _check_name(ep_name, ENTRYPOINT_NAME_REGEX)
    pname, pverstr = ep_name.split(EP_NAME_VER_SEP)
    pver: SemVerTuple = cast(Any, tuple(map(int, pverstr.split("."))))
    return (pname, pver)


# ----


class PGPlugin(PluginBase):
    group = PG_GROUP_NAME
    plugin_info_class: Type[PluginBase]
    plugin_class: Optional[Any] = object


# TODO: plugin group inheritance is not checked yet because it adds complications
class PluginGroupMeta(ABCMeta):
    """Metaclass to initialize some things on creation."""

    def __init__(self, name, bases, dct):
        self.Plugin.plugin_info_class.group = self.Plugin.name
        self.PluginRef: Type[AnyPluginRef] = AnyPluginRef.subclass_for(self.Plugin.name)


T = TypeVar("T", bound=IsPlugin)


class PluginGroup(Generic[T], metaclass=PluginGroupMeta):
    """All pluggable entities in metador are subclasses of this class.

    The type parameter is the (parent) class of all loaded plugins.

    They must implement the check method and be listed as plugin group.
    The name of their entrypoint defines the name of the plugin group.
    """

    PluginRef: TypeAlias = AnyPluginRef
    """Plugin reference class for this plugin group."""

    _PKG_META: ClassVar[Dict[str, PluginPkgMeta]] = pkg_meta
    """Package name -> package metadata."""

    class Plugin:
        """This is the plugin group plugin group, the first loaded group."""

        name = PG_GROUP_NAME
        version = (0, 1, 0)
        plugin_info_class = PGPlugin
        plugin_class: Type
        # plugin_class = PluginGroup  # can't set that -> check manually

    _ENTRY_POINTS: Dict[str, EntryPoint]
    """Dict of entry points of versioned plugins (not loaded)."""

    _VERSIONS: Dict[str, List[AnyPluginRef]]
    """Mapping from plugin name to pluginrefs."""

    _LOADED_PLUGINS: Dict[AnyPluginRef, Type[T]]
    """Dict from entry points to loaded plugins of that pluggable type."""

    def _add_ep(self, ep_name: str, ep_obj: EntryPoint):
        self._ENTRY_POINTS[ep_name] = ep_obj

        name, version = _from_ep_name(ep_name)
        p_ref = AnyPluginRef(group=self.name, name=name, version=version)

        if name not in self._VERSIONS:
            self._VERSIONS[name] = []
        self._VERSIONS[name].append(p_ref)
        self._VERSIONS[name].sort()

    def __init__(self, entrypoints):
        self._ENTRY_POINTS = {}
        self._VERSIONS = {}

        for k, v in entrypoints.values():
            self._add_ep(k, v)

        self._LOADED_PLUGINS = {}
        self.__post_init__()

    def __post_init__(self):
        if type(self) is PluginGroup:
            # make the magic plugingroup plugin add itself for consistency
            ep_name = f"{self.Plugin.name}__{semver_str(self.Plugin.version)}"
            ep = EntryPoint(
                ep_name, f"{type(self).__module__}:{type(self).__name__}", self.name
            )
            self._add_ep(ep_name, ep)

            ref = AnyPluginRef(
                group=self.name, name=self.Plugin.name, version=self.Plugin.version
            )
            self._LOADED_PLUGINS[ref] = self
            self.provider(ref).plugins[self.name].add(ref)

    @property
    def name(self) -> str:
        """Return name of the plugin group."""
        return self.Plugin.name

    @property
    def packages(self) -> Dict[str, PluginPkgMeta]:
        """Return metadata of all packages providing metador plugins."""
        return dict(self._PKG_META)

    def versions(
        self, p_name: str, version: Optional[SemVerTuple] = None
    ) -> List[AnyPluginRef]:
        """Return installed versions of a plugin (compatible with given version)."""
        refs = list(self._VERSIONS.get(p_name) or [])
        if version is None:
            return refs
        requested = self.PluginRef(name=p_name, version=version)
        return [ref for ref in refs if ref.supports(requested)]

    def resolve(
        self, p_name: str, version: Optional[SemVerTuple] = None
    ) -> Optional[AnyPluginRef]:
        """Return most recent compatible version of a plugin."""
        if refs := self.versions(p_name, version):
            return refs[-1]  # latest (compatible) version
        return None

    def provider(self, ref: AnyPluginRef) -> PluginPkgMeta:
        """Return package metadata of Python package providing this plugin."""
        if type(self) is PluginGroup and ref.name == PG_GROUP_NAME:
            # special case - the mother plugingroup plugin is not an EP,
            # so we cheat a bit (schema is in same package, but is an EP)
            return self.provider(self.resolve("schema"))

        ep = self._ENTRY_POINTS[_to_ep_name(ref.name, ref.version)]
        return self._PKG_META[cast(Any, ep).dist.name]

    # ----

    def __repr__(self):
        return f"<PluginGroup '{self.name}' {list(self.keys())}>"

    def __str__(self):
        def pg_line(name):
            p = self.provider(name)
            pkg = f"{p.name} {semver_str(p.version)}"
            pg = self._get_unsafe(name)
            return f"\t'{name}' {semver_str(pg.Plugin.version)} ({pkg})"

        pgs = "\n".join(map(pg_line, self.keys()))
        return f"Available '{self.name}' plugins:\n{pgs}"

    # ----
    # dict-like interface will provide latest versions of plugins by default

    def __contains__(self, key: str) -> bool:
        return key in self._VERSIONS

    def keys(self) -> KeysView[str]:
        """Return all names of all plugins."""
        return self._VERSIONS.keys()

    def values(self) -> Iterable[Type[T]]:
        """Return latest versions of all plugins (THIS LOADS ALL PLUGINS!)."""
        return map(self.__getitem__, self.keys())

    def items(self) -> Iterable[Tuple[str, Type[T]]]:
        """Return pairs of plugin name and latest installed version (THIS LOADS ALL PLUGINS!)."""
        return map(lambda k: (k, self[k]), self.keys())

    def __getitem__(self, key: str) -> Type[T]:
        if key not in self:
            raise KeyError(f"{self.name} not found: {key}")
        return self.get(key)

    # ----

    def _get_unsafe(self, p_name: str, version: Optional[SemVerTuple] = None):
        """Return most recent compatible version of given plugin name, without safety rails.

        For internal use only!
        """
        if ref := self.resolve(p_name, version):
            self._ensure_is_loaded(ref)
            return self._LOADED_PLUGINS[ref]
        else:
            msg = f"{p_name}"
            if version:
                msg += f": no installed version is compatible with {version}"
            raise KeyError(msg)

    # inspired by this nice trick: https://stackoverflow.com/a/60362860
    PRX = TypeVar("PRX", bound="Type[T]")  # type: ignore

    @overload
    def get(self, key: str, version: Optional[SemVerTuple] = None) -> Optional[Type[T]]:
        ...

    @overload
    def get(self, key: PRX, version: Optional[SemVerTuple] = None) -> Optional[PRX]:
        ...

    def get(
        self, key: Union[str, PRX], version: Optional[SemVerTuple] = None
    ) -> Union[Type[T], PRX, None]:
        if isinstance(key, str):
            key_: str = key
        else:
            # if a plugin class is passed, use its name + version
            key_: str = key.Plugin.name  # type: ignore
            version = version or key.Plugin.version  # use if no version passed

        # retrieve compatible plugin
        ret = self._get_unsafe(key_, version)
        if ret is not None and version is None:
            # no version constraint was passed or inferred -> mark it
            ret = UndefVersion._mark_class(ret)

        if isinstance(key, str):
            return cast(Type[T], ret)
        else:
            return ret

    # ----

    def _ensure_is_loaded(self, ref: AnyPluginRef):
        """Load plugin from entrypoint, if it is not loaded yet."""
        assert ref.group == self.name
        if ref in self._LOADED_PLUGINS:
            return  # already loaded, all good

        ep_name = _to_ep_name(ref.name, ref.version)
        ret = self._ENTRY_POINTS[ep_name].load()
        self._LOADED_PLUGINS[ref] = ret

        self._load_plugin(ep_name, ret)

    def _explicit_plugin_deps(self, plugin):
        """Return all plugin dependencies that must be taken into account."""
        def_deps = set(map(lambda n: (self.name, n), plugin.Plugin.requires))
        extra_deps = self.plugin_deps(plugin) or set()
        if not isinstance(extra_deps, set):
            extra_deps = set(extra_deps)
        return def_deps.union(extra_deps)

    def plugin_deps(self, plugin):
        """Return additional automatically inferred dependencies for a plugin."""

    def _load_plugin(self, ep_name: str, plugin):
        """Run checks and finalize loaded plugin."""
        # print("load", self.name, name, plugin)
        from ..plugins import plugingroups

        # run inner Plugin class checks (with possibly new Fields cls)
        if not plugin.__dict__.get("Plugin"):
            raise TypeError(f"{ep_name}: {plugin} is missing Plugin inner class!")
        plugin.Plugin = self.Plugin.plugin_info_class.parse_info(
            plugin.Plugin, ep_name=ep_name
        )

        # do general checks first, if they fail no need to continue
        self._check_common(ep_name, plugin)
        self.check_plugin(ep_name, plugin)

        for dep_grp, dep_ref in self._explicit_plugin_deps(plugin):
            dep_grp = plugingroups[dep_grp]
            # print("check dep", depgroup, depname)
            dep_grp._ensure_is_loaded(dep_ref)

        self.init_plugin(plugin)

    def _check_common(self, ep_name: str, plugin):
        """Perform both the common and specific checks a registered plugin.

        Raises a TypeError with message in case of failure.
        """
        _check_name(ep_name, PLUGIN_NAME_REGEX)
        if type(self) is not PluginGroup:
            _check_plugin_name_prefix(ep_name)

        # check correct base class of plugin, if stated
        if self.Plugin.plugin_class:
            check_is_subclass(ep_name, plugin, self.Plugin.plugin_class)

    def check_plugin(self, ep_name: str, plugin: Type[T]):
        """Perform plugin group specific checks on a registered plugin.

        Raises a TypeError with message in case of failure.

        To be overridden in subclasses for plugin group specific checks.

        Args:
            name: Declared entrypoint name.
            plugin: Object the entrypoint is pointing to.
        """
        if type(self) is not PluginGroup:
            return  # is not the "plugingroup" group itself

        # these are the checks done for other plugin group plugins:

        check_is_subclass(ep_name, plugin, PluginGroup)
        check_is_subclass(ep_name, self.Plugin.plugin_info_class, PluginBase)
        if plugin != PluginGroup:  # exclude itself. this IS its check_plugin
            check_implements_method(ep_name, plugin, PluginGroup.check_plugin)

        # make sure that the declared plugin_info_class for the group sets 'group'
        # and it is also equal to the plugin group 'name'.
        # this is the safest way to make sure that Plugin.ref() works correctly.
        ppgi_cls = plugin.Plugin.plugin_info_class
        if not ppgi_cls.group:
            raise TypeError(f"{ep_name}: {ppgi_cls} is missing 'group' attribute!")
        if not ppgi_cls.group == plugin.Plugin.name:
            msg = f"{ep_name}: {ppgi_cls.__name__}.group != {plugin.__name__}.Plugin.name!"
            raise TypeError(msg)

    def init_plugin(self, plugin: Type[T]):
        """Override this to do something after the plugin has been checked."""
        if type(self) is not PluginGroup:
            return  # is not the "plugingroup" group itself
        create_pg(plugin)  # create plugin group if it does not exist


# ----

_plugin_groups: Dict[str, PluginGroup] = {}
"""Instances of initialized plugin groups."""


def create_pg(pg_cls):
    """Create plugin group instance if it does not exist."""
    pg_name = pg_cls.Plugin.name
    if pg_name in _plugin_groups:
        return _plugin_groups[pg_name]

    if not isinstance(pg_cls.Plugin, PluginBase):
        # magic - substitute Plugin class with parsed plugin object
        pg_cls.Plugin = PGPlugin.parse_info(pg_cls.Plugin)

    pg = pg_cls(get_group(pg_name))
    _plugin_groups[pg_name] = pg
