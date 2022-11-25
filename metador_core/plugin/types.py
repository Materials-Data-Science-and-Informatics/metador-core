"""Simple types and utilities for plugins."""
from typing import Any, ClassVar, Optional, Protocol, Tuple, runtime_checkable

from phantom.re import FullMatch
from pydantic import NonNegativeInt
from typing_extensions import Final, TypeAlias

# ----

SemVerTuple: TypeAlias = Tuple[NonNegativeInt, NonNegativeInt, NonNegativeInt]
"""Type to be used for SemVer triples."""

DIGIT: Final[str] = r"[0-9]"  # python 3: \d matches also weird other symbols!!!
SEMVER_STR_REGEX: Final[str] = rf"{DIGIT}+\.{DIGIT}+\.{DIGIT}+"


class SemVerStr(FullMatch, pattern=SEMVER_STR_REGEX):
    """Semantic version string (x.y.z)."""


def to_semver_str(ver: SemVerTuple):
    return ".".join(map(str, ver))


def from_semver_str(ver: SemVerStr) -> SemVerTuple:
    return tuple(map(int, ver.split(".")))  # type: ignore


# ----

LETTER: Final[str] = r"[a-z]"
ALNUM: Final[str] = r"[a-z0-9]"
LETSEP: Final[str] = r"[_-]"
NSSEP: Final[str] = r"[.]"
NAME: Final[str] = rf"{LETTER}{ALNUM}({LETSEP}?{ALNUM})*"
"""An unqualified name begins with a letter and ends with a letter or number.

It consists of: lowercase letters, digits, _ and -
"""

QUAL_NAME: Final[str] = rf"{NAME}({NSSEP}{NAME})*"
"""A qualified name is a sequence of unqualified names separated with ."""

EP_NAME_VER_SEP: str = "__"
"""Separator between plugin name and semantic version in entry point name."""

EP_NAME_REGEX: Final[str] = rf"{QUAL_NAME}{EP_NAME_VER_SEP}{SEMVER_STR_REGEX}"
"""Regular expression that all metador plugin entry points must match."""


class EPName(FullMatch, pattern=EP_NAME_REGEX):
    """Valid entry point for a metador plugin."""


def to_ep_name(p_name: str, p_version: SemVerTuple) -> EPName:
    """Return canonical entrypoint name `PLUGIN_NAME__MAJ.MIN.FIX`."""
    return EPName(f"{p_name}{EP_NAME_VER_SEP}{to_semver_str(p_version)}")


def from_ep_name(ep_name: EPName) -> Tuple[str, SemVerTuple]:
    """Split entrypoint name into `(PLUGIN_NAME, (MAJ,MIN,FIX))`."""
    pname, pverstr = ep_name.split(EP_NAME_VER_SEP)
    return (pname, from_semver_str(SemVerStr(pverstr)))


def ep_name_has_namespace(ep_name: EPName):
    """Check whether the passed name has a namespace prefix."""
    name, ver = from_ep_name(ep_name)
    return len(name.split(".", 1)) > 1


PG_PREFIX: str = "metador_"
"""Group prefix for metador plugin entry point groups."""


class EPGroupName(FullMatch, pattern=rf"{PG_PREFIX}.*"):
    """Valid internal group name for a metador plugin group."""


def is_metador_ep_group(ep_group_name: str):
    return ep_group_name.startswith(PG_PREFIX)


def to_ep_group_name(group_name: str) -> EPGroupName:
    return EPGroupName(f"{PG_PREFIX}{group_name}")


def from_ep_group_name(ep_group_name: EPGroupName) -> str:
    plen = len(PG_PREFIX)
    return ep_group_name[plen:]


# ----


class PluginLike(Protocol):
    """A Plugin has a Plugin inner class with plugin infos."""

    Plugin: ClassVar[Any]  # actually its PluginBase, but this happens at runtime


@runtime_checkable
class HasNameVersion(Protocol):
    name: Any
    version: Any


@runtime_checkable
class PluginInfoLike(HasNameVersion, Protocol):
    group: Any


def is_pluginlike(cls, *, check_group=True) -> bool:
    if pgi := cls.__dict__.get("Plugin"):
        return isinstance(pgi, PluginInfoLike if check_group else HasNameVersion)
    else:
        return False


def plugin_args(
    plugin="",  # actually takes: Union[str, PluginRef, PluginLike]
    version: Optional[SemVerTuple] = None,
    *,
    require_version: bool = False,
    # group: Optional[str]
) -> Tuple[str, Optional[SemVerTuple]]:
    """Return requested plugin name and version based on passed arguments.

    Helper for function argument parsing.
    """
    name: str = ""
    vers: Optional[SemVerTuple] = version

    if isinstance(plugin, str):
        name = plugin
    elif isinstance(plugin, HasNameVersion):
        name = plugin.name
        if not vers:
            vers = plugin.version
    elif pgi := getattr(plugin, "Plugin", None):
        if isinstance(pgi, HasNameVersion):
            return plugin_args(pgi, version, require_version=require_version)

    if require_version and vers is None:
        raise ValueError(f"No version of {name} specified, but is required!")
    return (name, vers)
