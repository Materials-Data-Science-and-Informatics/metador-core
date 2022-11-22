from typing import Tuple

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
