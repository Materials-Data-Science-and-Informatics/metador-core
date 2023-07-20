"""IH5 skeletons and stubs (low-level structures used by IH5MFRecord).

A skeleton is documenting the tree structure of a HDF5-like container,
ignoring the actual data content (attribute values and datasets).

This can be used to implement manifest file and support "patching in thin air",
i.e. without having the actual container.
"""
from typing import Dict, Union

import h5py
from pydantic import BaseModel
from typing_extensions import Literal

from .overlay import H5Type, IH5Dataset, IH5Group
from .record import IH5Record, IH5UserBlock


class SkeletonNodeInfo(BaseModel):
    node_type: Literal[H5Type.group, H5Type.dataset]
    patch_index: int  # if its a dataset -> patch where to get data, group: creation patch
    attrs: Dict[str, int]  # names and patch indices of attributes

    def with_patch_index(self, idx: int):
        ret = self.copy()
        ret.attrs = dict(self.attrs)
        ret.patch_index = idx
        for k in ret.attrs.keys():
            ret.attrs[k] = idx
        return ret

    @classmethod
    def for_node(cls, node: Union[IH5Group, IH5Dataset]):
        if isinstance(node, IH5Dataset):
            dt = H5Type.dataset
        else:
            dt = H5Type.group

        def cidx_to_patch_idx(cidx):
            return node._record._ublock(cidx).patch_index

        pidx = cidx_to_patch_idx(node._cidx)
        ats = {
            key: cidx_to_patch_idx(node.attrs._find(key)) for key in node.attrs.keys()
        }

        return cls(node_type=dt, patch_index=pidx, attrs=ats)


class IH5Skeleton(BaseModel):
    __root__: Dict[str, SkeletonNodeInfo]

    def with_patch_index(self, idx: int):
        """Copy of skeletonwith  patch index of all nodes and attributes modified."""
        ret = self.copy()
        ret.__root__ = dict(self.__root__)
        for k, v in ret.__root__.items():
            ret.__root__[k] = v.with_patch_index(idx)
        return ret

    @classmethod
    def for_record(cls, rec: IH5Record):
        """Return mapping from all paths in a IH5 record to their type.

        The attributes are represented as special paths with the shape `a/b/.../n@attr`,
        pointing to the attribute named `attr` at the path `a/b/.../n`.

        First component is a H5Type enum value,
        Second component is more detailed type for attribute values and `IH5Dataset`s.
        """
        skel = {"/": SkeletonNodeInfo.for_node(rec["/"])}

        def add_paths(_, node):
            skel[node.name] = SkeletonNodeInfo.for_node(node)

        rec.visititems(add_paths)
        return cls(__root__=skel)


# NOTE: we pass in the empty container as first argument in the following
# in order to make this generic over subtypes (IH5Record, IH5MFRecord)!


def init_stub_skeleton(ds: IH5Record, skel: IH5Skeleton):
    """Fill a passed fresh container with stub structure based on a skeleton."""
    if len(ds) or len(ds.attrs):
        raise ValueError("Container not empty, cannot initialize stub structure here!")

    for k, v in skel.__root__.items():
        if v.node_type == H5Type.group:
            if k not in ds:
                ds.create_group(k)
        elif v.node_type == H5Type.dataset:
            ds[k] = h5py.Empty(None)

        for a in v.attrs.keys():
            ds[k].attrs[a] = h5py.Empty(None)


def init_stub_base(target: IH5Record, src_ub: IH5UserBlock, src_skel: IH5Skeleton):
    """Prepare a stub base container, given empty target, source user block and skeleton.

    Patches on top of this container will work with the original container.
    """
    init_stub_skeleton(target, src_skel)
    # mark as base container
    target._set_ublock(-1, src_ub.copy(update={"prev_patch": None}))
