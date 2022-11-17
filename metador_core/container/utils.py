"""
Constants and helper functions.

Provides syntactic path transformations to implement the Metador container layout.
"""

from typing_extensions import Final

METADOR_SPEC_VERSION: Final[str] = "1.0"
"""Version of container spec created by this package."""
# NOTE: don't forget to change it when something about the container structure changes!

METADOR_PREF: Final[str] = "metador_"
"""Reserved prefix for group and dataset names."""

METADOR_META_PREF: Final[str] = METADOR_PREF + "meta_"
"""Sub-prefix for group that stores group or dataset metadata."""

METADOR_TOC_PATH: Final[str] = f"/{METADOR_PREF}container"
"""Path of group with the Metador metadata index structure of the container."""

METADOR_VERSION_PATH: Final[str] = f"{METADOR_TOC_PATH}/version"
"""Path of dataset with the Metador container spec version of the container."""

METADOR_UUID_PATH: Final[str] = f"{METADOR_TOC_PATH}/uuid"
"""Path of dataset with the Metador container version of the container."""

METADOR_PACKAGES_PATH: Final[str] = f"{METADOR_TOC_PATH}/packages"
"""Path of group with package info of packages providing used schemas in the container."""

METADOR_SCHEMAS_PATH: Final[str] = f"{METADOR_TOC_PATH}/schemas"
"""Path of group with info about used schemas in the container."""

METADOR_LINKS_PATH: Final[str] = f"{METADOR_TOC_PATH}/links"
"""Path of group with links to schema instances in the container."""


def is_internal_path(path: str, pref: str = METADOR_PREF) -> bool:
    """Return whether the path of this node is Metador-internal (metador_*).

    Optional argument can set a different prefix that path segments are checked for.
    """
    # first case is for relative paths, second for later path segments and absolute paths
    return path.startswith(pref) or path.find(f"/{pref}") >= 0


# filtering and transforming between node and metadata base path


def is_meta_base_path(path: str) -> bool:
    """Return whether the path is a metadata base dir (but not an inner path!)."""
    return path.split("/")[-1].startswith(METADOR_META_PREF)


def to_meta_base_path(node_path: str, is_dataset: bool) -> str:
    """Return path to base group containing metadata for given node."""
    segs = node_path.split("/")
    if is_dataset:
        segs[-1] = METADOR_META_PREF + segs[-1]
    elif segs == ["", ""]:  # name was "/"
        segs[-1] = METADOR_META_PREF
    else:
        segs.append(METADOR_META_PREF)
    return "/".join(segs)


def to_data_node_path(meta_dir_path: str) -> str:
    """Given a metadata group path, infer the correct node path.

    Path can be relative or absolute.
    Will not check validity of the passed path, assumes it is fitting the scheme!
    """
    segs = meta_dir_path.split("/")
    pl = len(METADOR_META_PREF)
    segs[-1] = segs[-1][pl:]
    if segs[-1] == "" and (len(segs) > 2 or segs[0] != ""):
        segs.pop()
    return "/".join(segs)
