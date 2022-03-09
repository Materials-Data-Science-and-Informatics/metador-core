"""Generic functionality concerning container creation and modification."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional

import h5py
from jsonschema import ValidationError, validate

HEADER_GROUP = "/head"
"""Address of header group in the container."""

PAYLOAD_GROUP = "/body"
"""Address of payload group in the container."""

# TODO: create and load the schemas
HEADER_SCHEMAS = {
    "general": True,
    "biblio": True,
    "scientific": True,
    "previews": True,
}
"""Map from possible nodes in the header section to JSON Schemas to validate against."""


def create(target: Path) -> h5py.File:
    """Create a new Ardiem HDF5 container."""
    return h5py.File(target, "x")


def add_err(errs, key, val):
    errs[key] = errs.get(key, []).append(val)


def get_group(errs, container: h5py.File, gpath: str) -> Optional[h5py.Group]:
    if gpath not in container.keys():
        add_err(errs, gpath, f"'{gpath}' is missing.")
        return None
    g = container[gpath]
    if not isinstance(g, h5py.Group):
        add_err(errs, gpath, f"'{gpath}' is not a group.")
        return None
    return g


def check_header_general(container: h5py.File) -> Dict[str, List[str]]:
    """Given a container, check its header."""
    errs: Dict[str, List[str]] = {}
    hdr = get_group(errs, container, HEADER_GROUP)
    assert hdr is not None

    if len(hdr.attrs) > 0:
        msg = f"unexpected attributes: {set(hdr.attrs)}"
        add_err(errs, HEADER_GROUP, msg)

    hdr_files = set(hdr)  # present header json files
    exp_files = set(HEADER_SCHEMAS.keys())  # expected header json files
    missing = exp_files - hdr_files
    if len(missing) > 0:
        msg = f"missing header entries: {missing}"
        add_err(errs, HEADER_GROUP, msg)

    unknown = hdr_files - exp_files
    if len(unknown) > 0:
        msg = f"unexpected header entries: {unknown}"
        add_err(errs, HEADER_GROUP, msg)

    for hdrfile in hdr_files:
        hdrpath = f"{HEADER_GROUP}/{hdrfile}"

        atrs = container[hdrpath].attrs
        if len(atrs) > 0:
            msg = f"unexpected attributes: {set(atrs)}"
            add_err(errs, hdrpath, msg)

        try:
            validate(instance=container[hdrpath], schema=HEADER_SCHEMAS[hdrfile])
        except ValidationError as e:
            add_err(errs, hdrpath, e.message)

    return errs


def check_payload_general(container: h5py.File) -> Dict[str, str]:
    """Given a container, check its payload."""
    assert PAYLOAD_GROUP in container.keys()
    return {}


def finalize_container(container: h5py.File, ext_meta: Dict[str, str]):
    """
    Based on container with payolad and extra metadata, attach container header.

    This mints a container id, adds required attributes, harvests special attributes
    from the payload into the header and embeds the provided `ext_meta` in the header.
    """
    assert set(container.keys()) == {PAYLOAD_GROUP}
    assert not check_payload_general(container)
