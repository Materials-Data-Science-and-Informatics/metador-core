"""
Constants and helper functions.

Provides syntactic path transformations to implement the Metador container layout.
"""
from itertools import dropwhile
from typing import Tuple

from typing_extensions import Final

METADOR_SPEC_VERSION: Final[str] = "1.0"
"""Version of container spec created by this package."""
# NOTE: don't forget to change it when something about the container structure changes!

METADOR_PREF: Final[str] = "metador_"
"""Reserved prefix for group and dataset names."""

METADOR_META_PREF: Final[str] = METADOR_PREF + "meta_"
"""Sub-prefix for group that stores group or dataset metadata."""

METADOR_VERSION_PATH: Final[str] = f"/{METADOR_PREF}version"
"""Path of dataset with the Metador container version of the container."""

METADOR_UUID_PATH: Final[str] = f"/{METADOR_PREF}container_uuid"
"""Path of dataset with the Metador container version of the container."""

METADOR_TOC_PATH: Final[str] = f"/{METADOR_PREF}container_toc"
"""Path of group with the Metador metadata index structure of the container."""

METADOR_PKGS_PATH: Final[str] = f"/{METADOR_PREF}container_pkgs"
"""Path of group with package info of packages providing used schemas in the container."""

METADOR_SCHEMAS_PATH: Final[str] = f"/{METADOR_PREF}container_schemas"
"""Path of group with schema plugin refs of used schemas in the container."""


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


# Transforming from/to paths of stored metadata objects


def meta_obj_to_meta_path(metaobj_path: str) -> str:
    """Given a path to a stored metadata object, return the base path prefix."""
    # do we need this? jumping up the raw node parents would be quicker
    # e.g. when going through TOC results


def meta_obj_to_toc_path(metaobj_path: str) -> str:
    """Return correct path in TOC for the given metadata object path."""
    # NOTE: can't be inverted, but link at the returned path will point back to the arg.!
    segs = list(
        dropwhile(
            lambda x: not x.startswith(METADOR_META_PREF), metaobj_path.split("/")
        )
    )
    segs[0] = METADOR_TOC_PATH
    return "/".join(segs)


def split_meta_obj_path(metaobj_path: str) -> Tuple[str, str, str, str]:
    """Split full path of a metadata object inside the metadata directory of a node.

    Returns tuple with:
        Path to metadata base directory
        full schema parent path
        schema name
        object uuid

    "/some/path/to/metador_meta_something/schema/subschema/=uuid" results in:
    ('/some/path/to/metador_meta_something', 'schema/subschema', 'subschema', 'uuid')
    """
    segs = metaobj_path.lstrip("/").split("/")
    uuid = segs.pop()[1:]
    node_meta_base = ""
    schema_path = ""
    schema_name = ""
    found_meta_base = False
    for seg in segs:
        if found_meta_base:
            schema_path += f"/{seg}"
            schema_name = seg
        else:
            node_meta_base += f"/{seg}"
        if seg.startswith(METADOR_META_PREF):
            found_meta_base = True
    return (node_meta_base, schema_path[1:], schema_name, uuid)
