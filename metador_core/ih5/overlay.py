r"""
Overlay wrappers to access a virtual record consisting of a base container + patches.

The wrappers take care of dispatching requests to records,
groups and attributes to the correct path.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

import h5py
import numpy as np

# dataset value marking a deleted group, dataset or attribute
DEL_VALUE = np.void(b"\x7f")  # ASCII DELETE


def _is_del_mark(val) -> bool:
    return isinstance(val, np.void) and val.tobytes() == DEL_VALUE.tobytes()


def _node_is_del_mark(node) -> bool:
    """Return whether node is marking a deleted group/dataset/attribute value."""
    val = node[()] if isinstance(node, h5py.Dataset) else node
    return _is_del_mark(val)


# attribute key marking group substitution (instead of pass-through default for groups)
# attribute value does not matter, but should be np.Empty(None)
# if present, all children and attributes are interpreted as normal nodes, not as a patch
SUBST_KEY = "\x1a"  # ASCII SUBSTITUTE


def _node_is_virtual(node) -> bool:
    """Virtual node (i.e. transparent and only carrier for child nodes and attributes)."""
    return isinstance(node, h5py.Group) and SUBST_KEY not in node.attrs


@dataclass(frozen=True)
class IH5Node:
    """An overlay node wraps a group, dataset or attribute manager.

    It takes care of finding the correct container to look for the data
    and helps with patching data in a new patch container.

    It essentially lifts the interface of h5py from a single file to an IH5 record
    that may consist of a base container file and a number of patch containers.
    """

    _files: List[h5py.File]
    """List of open containers in patch order (the actual data store)."""

    _gpath: str
    """Location in record this node represents (absolute wrt. root of record)."""

    _cidx: int
    """Left boundary index for lookups, i.e. this node will not consider
    containers with smaller index than that.
    """

    def __post_init__(self):
        """Instantiate an overlay node."""
        if not self._files:
            raise ValueError("List of opened record containers must be non-empty!")
        if not self._gpath:
            raise ValueError("Path must be non-empty!")
        if not (0 <= self._cidx <= self._last_idx):
            raise ValueError("Creation index must be in index range of file list!")

    def __hash__(self):
        """Hash an overlay node.

        Two nodes are equivalent if they are linked to the same list of open
        HDF5 files and address the same entity.
        """
        return hash((id(self._files), self._gpath, self._cidx))

    def _parent_path(self):
        """Return path of the parent node (the root is its own parent)."""
        if self._gpath == "/":
            return "/"
        segs = self._gpath.split("/")[:-1]
        if segs == ['']:
            return "/"
        return "/".join(segs)

    @property
    def name(self) -> str:
        return self._gpath

    @property
    def _last_idx(self):
        """Index of the latest container."""
        return len(self._files) - 1

    def _is_read_only(self) -> bool:
        """Return true if the newest container is read-only and nothing can be written."""
        return self._files[-1].mode == "r"

    def _expect_open(self):
        """Check that the record is open (if it was closed, the files are gone)."""
        if not self._files:
            raise ValueError("Record is not open!")

    def _rel_path(self, path: str):
        """Return relative path based on current location if given path is absolute.

        If relative, returns the path back unchanged.
        """
        if path[0] != "/":
            return path
        if path.find(self._gpath) != 0:
            raise RuntimeError("Invalid usage, cannot strip non-matching prefix!")
        start_idx = len(self._gpath) + int(self._gpath != "/")
        return path[start_idx:]

    def _abs_path(self, path: str):
        """Return absolute path based on current location if given path is relative.

        If absolute, returns the path back unchanged.
        """
        pref = self._gpath if self._gpath != "/" else ""
        return path if path and path[0] == "/" else f"{pref}/{path}"

    def _inspect_path(self, path):  # pragma: no cover
        """Print the path node of all containers where the path is contained in."""
        print(f"Path {path}:")
        for j in range(len(self._files)):
            if path in self._files[j]:
                node = self._files[j][path]
                print(f"  idx={j}: {type(node).__name__}")
                if isinstance(node, h5py.Dataset):
                    print("    ", node[()])


class IH5InnerNode(IH5Node):
    """Common Group and AttributeManager overlay with familiar dict-like interface.

    Will grant either access to child records/subgroups,
    or to the attributes attached to the group/dataset at a path in a record.
    """

    def __init__(
        self,
        files: List[h5py.File],
        gpath: str,
        creation_idx: int,
        attrs: bool = False,
    ):
        """See `IH5Node` constructor.

        This variant represents an "overlay container", of which there are two types -
        a group (h5py.Group) and a set of attributes (h5py.AttributeManager).

        This class takes care of both (in order to avoid lots of code duplication),
        distinguishing them through the additional `attrs` flag.
        """
        super().__init__(files, gpath, creation_idx)
        # if attrs set, represents AttributeManager, otherwise its a group
        self._attrs: bool = attrs

    def _check_key(self, key: str):
        """Check a key used with bracket accessor notation.

        (i.e. used for `__getitem__, __setitem__, __delitem__`)
        """
        if key == "":
            raise ValueError("Invalid empty path!")
        if key.find("@") >= 0:  # used as attribute separator in the skeleton!
            raise ValueError(f"Invalid symbol '@' in key: '{key}'!")
        if self._attrs and (key.find("/") >= 0 or key == SUBST_KEY):
            raise ValueError(f"Invalid attribute key: '{key}'!")
        if re.match(r"^[!-~]+$", key) is None:
            raise ValueError("Invalid key: Only printable ASCII is allowed!")

    def _get_child_raw(self, key: str, cidx: int) -> Any:
        """Return given child (dataset, group, attribute) from given container."""
        if self._attrs:
            return self._files[cidx][self._gpath].attrs[key]
        else:
            return self._files[cidx][self._abs_path(key)]

    def _get_child(self, key: str, cidx: int) -> Any:
        """Like _get_child_raw, but wraps the result with an overlay class if needed."""
        val = self._get_child_raw(key, cidx)
        path = self._abs_path(key)
        if isinstance(val, h5py.Group):
            return IH5Group(self._files, path, cidx)
        elif isinstance(val, h5py.Dataset):
            return IH5Dataset(self._files, path, cidx)
        else:
            return val

    def _children(self) -> Dict[str, int]:
        """Return dict mapping from a child name to the most recent overriding patch idx.

        For records, dereferencing the child path in that container will give the data.
        For groups, the returned number is to be treated as the lower bound, i.e.
        the child creation_idx to recursively get the descendents.
        """
        self._expect_open()

        children: Dict[str, int] = {}
        is_virtual: Dict[str, bool] = {}
        for i in reversed(range(self._cidx, len(self._files))):
            if self._gpath not in self._files[i]:
                continue

            obj = self._files[i][self._gpath]
            if self._attrs:
                obj = obj.attrs
            assert isinstance(obj, h5py.Group) or isinstance(obj, h5py.AttributeManager)

            # keep most recent version of child node / attribute
            for k in obj.keys():
                if k not in children:
                    is_virtual[k] = _node_is_virtual(self._get_child_raw(k, i))
                    children[k] = i
                elif is_virtual[k]:  # .. and k in children!
                    # decrease lower bound
                    children[k] = min(children[k], i)

        # return resulting child nodes / attributes (without the deleted ones)
        # in alphabetical order,
        # in case of attributes, also excludes special SUBST marker attribute
        return {
            k: idx
            for k, idx in sorted(children.items(), key=lambda x: x[0])
            if (not self._attrs or not k == SUBST_KEY)
            and not _node_is_del_mark(self._get_child_raw(k, idx))
        }

    def _get_children(self) -> List[Any]:
        """Get alphabetically ordered list of child nodes."""
        return [
            self._get_child(self._abs_path(k), idx)
            for k, idx in self._children().items()
        ]

    def _create_group(self, gpath) -> IH5Group:
        """Create group, overwriting whatever was at that path."""
        if self._is_read_only():
            raise ValueError(f"Cannot create group '{gpath}', create a patch first!")

        path = self._abs_path(gpath)
        nodes = self._node_seq(gpath)
        if not isinstance(nodes[-1], IH5Group):
            raise ValueError(f"Cannot create group, {nodes[-1]._gpath} is a dataset!")
        if nodes[-1]._gpath == path:
            raise ValueError("Cannot create group, it already exists!")

        # remove "deleted" marker, if set at current path in current patch container
        if path in self._files[-1] and _node_is_del_mark(self._files[-1][path]):
            del self._files[-1][path]
        # create group (or fail if something else exists there already)
        self._files[-1].create_group(path)
        # if this is a patch: mark as non-virtual, i.e. "overwrite" with empty group
        # because the intent here is to "create", not update something.
        if len(self._files) > 1:
            self._files[-1][path].attrs[SUBST_KEY] = h5py.Empty(None)

        return IH5Group(self._files, path, self._last_idx)

    def _create_dataset(
        self, gpath, shape=None, dtype=None, data=None, **kwargs
    ) -> IH5Dataset:
        unknown_kwargs = set(kwargs.keys()) - set(["compression", "compression_opts"])
        if unknown_kwargs:
            raise ValueError(f"Unkown kwargs: {unknown_kwargs}")
        path = self._abs_path(gpath)
        self._files[-1].create_dataset(
            path, shape=shape, dtype=dtype, data=data, **kwargs
        )
        return self[path]

    def _node_seq(self, path: str) -> List[IH5Node]:
        """Return node sequence (one node per path prefix) to given path.

        Returns:
            Sequence starting with the current node (if path is relative)
            or the root node (if absolute) followed by all successive
            children along the requested path that exist.
        """
        curr: IH5InnerNode = IH5Group(self._files) if path[0] == "/" else self

        ret: List[IH5Node] = [curr]
        if path == "/" or path == ".":  # special case
            return ret

        # access entity through child group sequence
        segs = path.strip("/").split("/")
        nxt_cidx = 0
        for i in range(len(segs)):
            seg, is_last_seg = segs[i], i == len(segs) - 1
            # find most recent container with that child
            nxt_cidx = curr._children().get(seg, -1)
            if nxt_cidx == -1:
                return ret  # not found -> return current prefix
            curr = curr._get_child(seg, nxt_cidx)  # proceed to child
            ret.append(curr)
            # catch invalid access, e.g. /foo is record, user accesses /foo/bar:
            if not is_last_seg and isinstance(curr, IH5Dataset):
                raise ValueError(f"Cannot access path inside a value: {curr._gpath}")
        # return path index sequence
        return ret

    def _find(self, key: str, create: bool = False) -> Optional[int]:
        """Return index of container holding that key (attribute or path), if any.

        Args:
            key: nonempty string (attribute, or relative/absolute path)

        Returns:
            Index >= 0 of most recent container patching that path if found, else None.
        """
        if self._attrs:  # access an attribute by key (always "relative")
            return self._children().get(key, None)
        # access a path (absolute or relative)
        nodes = self._node_seq(key)
        return nodes[-1]._cidx if nodes[-1]._gpath == self._abs_path(key) else None

    def _create_virtual(self, key: str) -> bool:
        nodes = self._node_seq(key)
        path = self._abs_path(key)
        if (
            nodes[-1]._gpath == path
            and nodes[-1]._cidx == self._last_idx
            and not _node_is_del_mark(nodes[-1])
        ):
            return False  # something at that path in most recent container exists

        # most recent entity is a deletion marker or not existing?
        if nodes[-1]._gpath != path or _node_is_del_mark(nodes[-1]):
            suf_segs = nodes[-1]._rel_path(path).split("/")
            # create "overwrite" group in most recent patch...
            self._create_group(f"{nodes[-1]._gpath}/{suf_segs[0]}")
            # ... and create (nested) virtual group node(s), if needed
            if len(suf_segs) > 1:
                self._files[-1].create_group(key)

        return True

    def visititems(self, func: Callable[[str, object], Optional[Any]]) -> Any:
        self._expect_open()
        stack = list(reversed(self._get_children()))
        while stack:
            curr = stack.pop()
            val = func(self._rel_path(curr._gpath), curr)
            if val is not None:
                return val
            if isinstance(curr, IH5Group):
                stack += reversed(curr._get_children())

    def visit(self, func: Callable[[str], Optional[Any]]) -> Any:
        return self.visititems(lambda x, _: func(x))

    def __setitem__(self, key: str, val):
        self._expect_open()
        self._check_key(key)
        if self._is_read_only():
            raise ValueError(f"Cannot set '{key}', create a patch first!")
        if _is_del_mark(val):
            raise ValueError(f"Value '{val}' is forbidden, cannot assign!")
        if isinstance(val, IH5Node):
            raise ValueError("Hard links are not supported, cannot assign!")
        if isinstance(val, h5py.SoftLink) or isinstance(val, h5py.ExternalLink):
            raise ValueError("SymLink and ExternalLink not supported, cannot assign!")

        if self._attrs:
            # if path does not exist in current patch, just create "virtual node"
            if self._gpath not in self._files[-1]:
                self._files[-1].create_group(self._gpath)
            # deletion marker at `key` (if set) is overwritten automatically here
            # so no need to worry about removing it before assigning `val`
            self._files[-1][self._gpath].attrs[key] = val
        else:
            path = self._abs_path(key)
            fidx = self._find(path)
            if fidx is not None:
                prev_val = self._get_child(path, fidx)
                if isinstance(prev_val, IH5Group) or isinstance(prev_val, IH5Dataset):
                    raise ValueError("Path exists, in order to replace - delete first!")

            if path in self._files[-1] and _node_is_del_mark(
                self._get_child_raw(path, self._last_idx)
            ):
                # remove deletion marker in latest patch, if set
                del self._files[-1][path]
            elif path not in self._files[-1]:
                # create path and overwrite-group in latest patch
                self._create_virtual(path)
                assert path in self._files[-1]
                del self._files[-1][path]

            # assign new value
            self._create_dataset(path, data=val)

    def __delitem__(self, key: str):
        self._expect_open()
        self._check_key(key)
        if self._is_read_only():
            raise ValueError(f"Cannot delete '{key}', create a patch first!")

        found_cidx = self._find(key)
        if found_cidx is None or _node_is_del_mark(self._get_child(key, found_cidx)):
            raise KeyError(f"Cannot delete '{key}', it does not exist!")

        # remove the entity if it is found in newest container,
        # mark the path as deleted if doing a patch and not working on base container
        has_patches = len(self._files) > 1
        if self._attrs:
            if found_cidx == self._last_idx:
                del self._files[-1][self._gpath].attrs[key]
            if has_patches:
                if self._gpath not in self._files[-1]:  # no node at path in latest?
                    self._files[-1].create_group(self._gpath)  # create "virtual" node
                self._files[-1][self._gpath].attrs[key] = DEL_VALUE  # mark deleted
        else:
            path = self._abs_path(key)
            if path in self._files[-1]:
                del self._files[-1][path]
            if has_patches:
                self._files[-1][path] = DEL_VALUE

    def __getitem__(self, key: str):
        self._expect_open()
        self._check_key(key)
        found_cidx = self._find(key)
        if found_cidx is None:
            raise KeyError(f"Cannot get item, '{key}' does not exist!")
        return self._get_child(key, found_cidx)

    def get(self, key: str, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def __contains__(self, key: str):
        self._check_key(key)
        return self._find(key) is not None

    def __iter__(self):
        return iter(self._children().keys())

    def __len__(self):
        return len(self.keys())

    def keys(self):
        return self._children().keys()

    def _dict(self):
        return {k: self._get_child(k, idx) for k, idx in self._children().items()}

    def values(self):
        return self._dict().values()

    def items(self):
        return self._dict().items()


class IH5Dataset(IH5Node):
    """`IH5Node` representing a `h5py.Dataset`, i.e. a leaf of the tree."""

    def __init__(self, files, gpath, creation_idx):
        super().__init__(files, gpath, creation_idx)

    @property
    def attrs(self):
        self._expect_open()
        return IH5AttributeManager(self._files, self._gpath, self._cidx)

    @property
    def parent(self):
        return IH5Group(self._files)[self._parent_path()]

    # for a dataset, instead of paths the numpy data is indexed. at this level
    # the patching mechanism ends, so it's just passing through to h5py

    def __getitem__(self, key):
        # just pass through dataset indexing to underlying dataset
        self._expect_open()
        return self._files[self._cidx][self._gpath][key]  # type: ignore

    def __setitem__(self, key, val):
        self._expect_open()
        if self._is_read_only():
            raise ValueError(f"Cannot set '{key}', create a patch first!")
        if self._cidx != self._last_idx:
            raise ValueError(f"Cannot set '{key}', node is not from the latest patch!")
        # if we're in the latest patch, allow writing as usual (pass through)
        self._files[-1][self._gpath][key] = val  # type: ignore

    def copy_into_patch(self):
        """Copy the most recent value at this path into the current patch.

        This is useful e.g. for editing inside a complex value, such as an array.
        """
        self._expect_open()
        if self._is_read_only():
            raise ValueError("Cannot copy, create a patch first!")
        if self._cidx == self._last_idx:
            raise ValueError("Cannot copy, this node is already from latest patch!")
        # copy value from older container to current patch
        self._files[-1][self._gpath] = self[()]


class IH5Group(IH5InnerNode):
    """`IH5Node` representing a `h5py.Group`."""

    @property
    def parent(self):
        return self[self._parent_path()]

    @classmethod
    def _latest_container_idx(cls, files, gpath) -> Optional[int]:
        """Return latest patch index where the group/dataset was overwritten/created.

        Returns None if not found or most recent value is a deletion mark.
        """
        idx = None
        for i in reversed(range(len(files))):
            if _node_is_del_mark(files[i][gpath]):
                return None
            elif _node_is_virtual(files[i][gpath]):
                idx = i
            else:
                return i  # some patch overrides the group
        return idx

    def __init__(self, files, gpath: str = "/", creation_idx: Optional[int] = None):
        if creation_idx is None:
            creation_idx = self._latest_container_idx(files, gpath)
            assert (
                creation_idx is not None
            )  # this is only used with '/' (always exists)
        super().__init__(files, gpath, creation_idx, False)

    @property
    def attrs(self):
        self._expect_open()
        return IH5AttributeManager(self._files, self._gpath, self._cidx)

    def create_group(self, gpath) -> IH5Group:
        self._expect_open()
        return self._create_group(gpath)

    def create_dataset(self, gpath, *args, **kwargs) -> IH5Dataset:
        self._expect_open()
        return self._create_dataset(gpath, *args, **kwargs)

    def create_copy(self, gpath, src_group_or_dataset):
        """Copy data to target path from a source IH5 (or H5) group or dataset.

        Will also copy all the attached attributes.

        Can also be used to copy data from a normal HDF5 File into an IH5 record.
        """
        return h5_copy_from_to(src_group_or_dataset, self, gpath)


class IH5AttributeManager(IH5InnerNode):
    """`IH5Node` representing an `h5py.AttributeManager`."""

    def __init__(self, files, gpath, creation_idx):
        super().__init__(files, gpath, creation_idx, True)


# ----
# Helpers for IH5 / H5 interop (its all h5py at the bottom anyway, so its easy)


class H5Type(str, Enum):
    """Type of an entity in a HDF5-like container.

    We list only those we care about, ignoring various
    link types etc.

    This will be used in wrappers around HDF5-like objects
    instead of using isinstance/subclass checks to implement
    duck-typing based decorator functionality that can
    work with (at least) raw HDF5, IH5 and IH5+Manifest.
    """

    group = "group"  # possibly nested, dict-like
    dataset = "dataset"  # = wrapped, indexable data
    attribute_set = "attribute-set"  # = not further nested, dict-like
    attribute = "attribute"  # = unwrapped data


_h5types = {
    # normal h5py files
    h5py.Group: H5Type.group,
    h5py.Dataset: H5Type.dataset,
    h5py.AttributeManager: H5Type.attribute_set,
    # IH5 datasets
    IH5Group: H5Type.group,
    IH5Dataset: H5Type.dataset,
    IH5AttributeManager: H5Type.attribute_set,
}


def node_h5type(node):
    """Return whether node is Group, Dataset or AttributeManager (or None)."""
    return _h5types.get(type(node))


def h5_copy_group_to_group(source_node, target_node):
    """Copy contents of one group into another group.

    The target group must exist, but be empty (no datasets or attributes).
    """
    assert node_h5type(source_node) == H5Type.group
    assert node_h5type(target_node) == H5Type.group
    assert not len(target_node) and not len(target_node.attrs)

    for k, v in source_node.attrs.items():  # copy root attributes
        target_node.attrs[k] = v
    for name in source_node.keys():  # copy each entity (will recurse)
        h5_copy_from_to(source_node[name], target_node, name)


def h5_copy_from_to(source_node, target_node, target_path: str):
    """Copy a dataset or group from one container to a fresh location.

    Will also copy all the attached attributes.

    This works also between HDF5 and IH5.

    Source node must be group or dataset object.
    Target path must be fresh absolute path or fresh path relative to target node.
    """
    if not target_path:
        raise ValueError("Target path is empty!")
    if target_path in target_node:
        raise ValueError(f"Target path {target_path} already exists!")
    if target_path[0] != "/" and node_h5type(target_node) != H5Type.group:
        raise ValueError("Cannot copy to relative path if target is not a group!")

    src_type = node_h5type(source_node)
    if src_type is None or src_type == H5Type.attribute_set:
        raise ValueError("Can only copy from a group or dataset!")

    if src_type == H5Type.dataset:
        node = target_node.create_dataset(target_path, data=source_node[()])
        for k, v in source_node.attrs.items():  # copy dataset attributes
            node.attrs[k] = v
    else:
        node = target_node.create_group(target_path)  # create local root for copy
        for k, v in source_node.attrs.items():  # copy source node attributes
            node.attrs[k] = v

        def copy_children(name, obj):
            # name is relative to source root, so we can use it
            ntype = node_h5type(obj)
            if ntype == H5Type.group:
                node.create_group(name)
            elif ntype == H5Type.dataset:
                node[name] = obj[()]

            new_atrs = node[name].attrs  # copy node attributes
            for k, v in obj.attrs.items():
                new_atrs[k] = v

        source_node.visititems(copy_children)
