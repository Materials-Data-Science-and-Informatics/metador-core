"""IH5 skeletons and stubs.

A skeleton is documenting the tree structure of a HDF5-like container,
ignoring the actual data content (attribute values and datasets).

This can be used to implement manifest file and support "patching in thin air",
i.e. without having the actual container.
"""
from typing import Any, Dict, Tuple

import h5py

from .overlay import H5Type
from .record import IH5Dataset, IH5Record, IH5UserBlock

IH5TypeSkeleton = Dict[str, Tuple[H5Type, Any]]

# NOTE: If we choose a non-flat skeleton, we could get rid of the restriction
# of not using @ for group/dataset names.
# On the other hand the @-addressing could be useful, so it might be a good
# restriction anyway.


def ih5_type_skeleton(ds: IH5Record) -> IH5TypeSkeleton:
    """Return mapping from all paths in a IH5 record to their type.

    The attributes are represented as special paths with the shape `a/b/.../n@attr`,
    pointing to the attribute named `attr` at the path `a/b/.../n`.

    First component is a H5Type enum value,
    Second component is more detailed type for attribute values and `IH5Dataset`s.
    """
    ret: IH5TypeSkeleton = {}
    for k, v in ds.attrs.items():
        ret[f"@{k}"] = (H5Type.attribute, type(v))

    def add_paths(name, node):
        if isinstance(node, IH5Dataset):
            typ = (H5Type.dataset, type(node[()]))
        else:
            typ = (H5Type.group, None)
        ret[name] = typ
        for k, v in node.attrs.items():
            ret[f"{name}@{k}"] = (H5Type.attribute, type(v))

    ds.visititems(add_paths)
    return ret


def ih5_skeleton(ds: IH5Record) -> Dict[str, str]:
    """Create a skeleton capturing the raw structure of a IH5 record."""
    return {k: v[0].value for k, v in ih5_type_skeleton(ds).items()}


# NOTE: we pass in the empty container as first argument in the following
# in order to make this generic over subtypes (IH5Record, IH5MFRecord)!


def init_stub_skeleton(ds: IH5Record, skel: Dict[str, str]):
    """Fill a passed fresh container with stub structure based on a skeleton."""
    if not ds.is_empty:
        raise ValueError("Container not empty, cannot initialize stub structure here!")

    for k, v in skel.items():
        if v == H5Type.group:
            if k not in ds:
                ds.create_group(k)
        elif v == H5Type.dataset:
            ds[k] = h5py.Empty(None)
        elif v == H5Type.attribute:
            k, atr = k.split("@")  # split off attribute name
            k = k or "/"  # special case - root attributes
            if k not in ds:
                ds[k] = h5py.Empty(None)
            ds[k].attrs[atr] = h5py.Empty(None)
        else:
            raise ValueError(f"Invalid skeleton entry: {k} -> {v}")


def init_stub_base(target: IH5Record, src_ub: IH5UserBlock, src_skel: Dict[str, str]):
    """Prepare a stub base container, given empty target, source user block and skeleton.

    Will commit the base container to prevent accidental changes.

    Patches on top of this container will work with the original container.
    """
    init_stub_skeleton(target, src_skel)
    # mark as base container
    target._set_ublock(-1, src_ub.copy(update={"prev_patch": None}))
    # commit() will also fix the hashsum
    # passed arg is a marker flag (for IH5MF commit())
    target.commit(__is_stub__=True)
