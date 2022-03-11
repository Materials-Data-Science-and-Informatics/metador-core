r"""
Overlay wrappers to access a virtual dataset consisting of a base container + patches.

The wrappers take care of dispatching requests to datasets,
groups and attributes to the correct path.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

import h5py
import numpy as np

H5Node = Union[h5py.Dataset, h5py.Group]


def _latest_container_idx(files, gpath) -> Optional[int]:
    """Return latest patch index where the group/dataset was overwritten/created."""
    idx = None
    for i in reversed(range(len(files))):
        if _node_is_virtual(files[i][gpath]):
            idx = i
        else:
            return i  # some patch overrides the group
    return idx


# dataset value marking a deleted group, dataset or attribute
DEL_VALUE = np.void(b"\x7f")  # ASCII DELETE


def _is_del_value(value) -> bool:
    """Return whether given value is equal to the special `DEL_VALUE`."""
    return isinstance(value, np.void) and value.tobytes() == DEL_VALUE.tobytes()


def _node_is_deletion(node: H5Node) -> bool:
    """Return whether node is marking a deleted group/dataset/attribute value."""
    return _is_del_value(node[()] if isinstance(node, h5py.Dataset) else node)


# attribute key marking group substitution (instead of pass-through default for groups)
# attribute value does not matter, but should be np.Empty(None)
# if present, all children and attributes are interpreted as normal nodes, not as a patch
SUBST_KEY = "\x1a"  # ASCII SUBSTITUTE


def _node_is_virtual(node: H5Node) -> bool:
    """Virtual node (i.e. transparent and only carrier for child nodes and attributes)."""
    return isinstance(node, h5py.Group) and SUBST_KEY not in node.attrs


class ArdiemNode:
    """An overlay node stands in for a group, dataset or attribute manager.

    It takes care of finding the correct container to look for the data
    and helps with patching data in a new patch container.
    """

    def __init__(self, files: List[h5py.File], gpath: str, creation_idx: int):
        """Instantiate an overlay node.

        Args:
            files: list of open containers in patch order (the actual data store)
            gpath: location this node represents
            creation_idx: left boundary index for lookups, i.e. this node will not
                consider containers with smaller index than that.
        """
        if not files:
            raise ValueError("Need a non-empty list of opened dataset containers!")
        if not gpath:
            raise ValueError("Node path cannot be empty!")
        if not (0 <= creation_idx < len(files)):
            raise ValueError("Creation index for node not in valid range!")

        self._files: List[h5py.File] = files
        self._gpath: str = gpath
        self._cidx: int = creation_idx

    @property
    def _last_idx(self):
        """Index of the latest container."""
        return len(self._files) - 1


class ArdiemInnerNode(ArdiemNode):
    """Common Group and AttributeManager overlay with familiar dict-like interface.

    It lifts the interface of h5py from a single file to an Ardiem dataset
    that may consist of a base container file and a number of patch containers.
    This overlay takes care of returning the correct, most recent values.

    Will grant either access to child datasets/subgroups,
    or to the attributes attached to the group/dataset at a path in a dataset.
    """

    def __init__(
        self,
        files: List[h5py.File],
        gpath: str,
        creation_idx: int,
        attrs: bool = False,
    ):
        """See `ArdiemNode` constructor.

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
        if self._attrs and key.find("/") >= 0:
            raise ValueError(f"Invalid attribute key: '{key}'!")

    def _abs_path(self, path: str):
        """Return absolute path based on current location if given path is relative.

        If absolute, returns the path back unchanged.
        """
        pref = self._gpath if self._gpath != "/" else ""
        return path if path[0] == "/" else f"{pref}/{path}"

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
            return ArdiemGroup(self._files, path, cidx)
        elif isinstance(val, h5py.Dataset):
            return ArdiemValue(self._files, path, cidx)
        else:
            return val

    @property
    def _children(self) -> Dict[str, int]:
        """Return dict mapping from a child name to the most recent overriding patch idx.

        For datasets, dereferencing the child path in that container will give the data.
        For groups, the returned number is to be treated as the lower bound, i.e.
        the child creation_idx to recursively get the descendents.
        """
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
        # in case of attributes, also excludes special SUBST marker attribute
        return {
            k: idx
            for k, idx in children.items()
            if (not self._attrs or not k == SUBST_KEY)
            and not _node_is_deletion(self._get_child_raw(k, idx))
        }

    def _create_group(self, gpath):
        """Create group, overwriting whatever was at that path."""
        path = self._abs_path(gpath)
        # remove "deleted" marker, if set at current path in current patch container
        if path in self._files[-1] and _node_is_deletion(self._files[-1][path]):
            del self._files[-1][path]
        # create group (or fail if something else exists there already)
        self._files[-1].create_group(path)
        # if this is a patch: mark as non-virtual, i.e. "overwrite" with empty group
        # because the intent here is to "create", not update something.
        if len(self._files) > 1:
            self._files[-1][path].attrs[SUBST_KEY] = h5py.Empty(None)

    def _find(self, key: str, create: bool = False) -> Optional[int]:
        """Return index of container holding that key (attribute or path), if any.

        Args:
            key: nonempty string (attribute, or relative/absolute path)
            create: if set, will create a virtual node if path does not exist

        Returns:
            Index >= 0 of most recent container patching that path if found,
            None if not found and -1 if create is set and a virtual node is created.
        """
        if self._attrs:  # access an attribute by key
            return self._children.get(key, None)
        # take care if the path is absolute -> need the root group!
        curr = ArdiemGroup(self._files) if key[0] == "/" else self
        if key == "/":  # special case
            return curr._cidx  # return index of latest root group

        # access entity through child group sequence
        segs = key.strip("/").split("/")
        nxt_cidx = 0
        for i in range(len(segs)):
            seg = segs[i]
            is_last_seg = i == len(segs) - 1

            # find most recent container with that child
            nxt_cidx = curr._children.get(seg, -1)
            if nxt_cidx == -1 and not create:
                return None  # not found and no overlay created

            # create virtual node in most recent container, if path not found there
            if create:
                # most recent entity is a deletion marker or not existing?
                if nxt_cidx == -1 or _node_is_deletion(curr):
                    # create "overwrite" group in most recent patch...
                    self._create_group(f"{curr._gpath}/{seg}")
                    # ... and create (nested) virtual group node(s), if needed
                    if not is_last_seg:
                        self._files[-1].create_group(key)

                    return -1  # found in previous, but created overlay

            # proceed to child
            curr = curr._get_child(seg, nxt_cidx)
            # catch invalid access, e.g. /foo is dataset, user accesses /foo/bar:
            if not is_last_seg and isinstance(curr, ArdiemValue):
                raise ValueError(f"Cannot access path inside a dataset: {curr._gpath}")

        return nxt_cidx  # found in some previous container, w/o creating overlay node

    def _inspect_path(self, path):
        print(f"Path {path}:")
        for j in range(len(self._files)):
            if path in self._files[j]:
                node = self._files[j][path]
                print(f"  idx={j}: {type(node).__name__}")
                if isinstance(node, h5py.Dataset):
                    print("    ", node[()])

    def __setitem__(self, key: str, val):
        self._check_key(key)
        try:
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
                    if isinstance(prev_val, ArdiemGroup):
                        raise ValueError("Path is a group, cannot overwrite with data!")

                if path in self._files[-1] and _node_is_deletion(
                    self._get_child_raw(path, self._last_idx)
                ):
                    # remove deletion marker in latest patch, if set
                    del self._files[-1][path]
                elif path not in self._files[-1]:
                    # create path and overwrite-group in latest patch
                    self._find(path, create=True)
                    assert path in self._files[-1]
                    del self._files[-1][path]

                # assign new value
                self._files[-1][path] = val

        except (RuntimeError, ValueError):
            raise ValueError("Dataset not writable! Create a patch in order to write!")

    def __delitem__(self, key: str):
        self._check_key(key)
        found_cidx = self._find(key)
        if found_cidx is None or _is_del_value(self._get_child(key, found_cidx)):
            raise KeyError(f"Cannot delete, '{key}' does not exist!")
        # remove entity if found in newest container
        if found_cidx == self._last_idx:
            if self._attrs:
                del self._files[-1][self._gpath].attrs[key]
            else:
                path = self._abs_path(key)
                del self._files[-1][path]
        # mark path as deleted if doing a patch
        if len(self._files) > 1:
            if self._attrs:
                self._files[-1][self._gpath].attrs[key] = DEL_VALUE
            else:
                self._files[-1][key] = DEL_VALUE

    def __getitem__(self, key: str):
        self._check_key(key)
        found_cidx = self._find(key)
        if found_cidx is None:
            raise KeyError(f"Cannot get item, '{key}' does not exist!")
        return self._get_child(key, found_cidx)

    def __contains__(self, key: str):
        self._check_key(key)
        return self._find(key) is not None

    def keys(self):
        return self._children.keys()

    def _dict(self):
        return {k: self._get_child(k, idx) for k, idx in self._children.items()}

    def values(self):
        return self._dict().values()

    def items(self):
        return self._dict().items()


class ArdiemValue(ArdiemNode):
    """`ArdiemNode` representing a `h5py.Dataset`, i.e. a leaf of the tree."""

    def __init__(self, files, gpath, creation_idx):
        super().__init__(files, gpath, creation_idx)

    @property
    def attrs(self):
        return ArdiemAttributeManager(self._files, self._gpath, self._cidx)

    # for a dataset, instead of paths the numpy data is indexed. at this level
    # the patching mechanism ends, so it's just passing through to h5py

    def __getitem__(self, key):
        # just pass through dataset indexing to underlying dataset
        return self._files[self._cidx][self._gpath][key]  # type: ignore

    def __setitem__(self, key, val):
        if self._cidx != self._last_idx:
            raise ValueError("This value is not from the latest patch, cannot write!")
        # if we're in the latest patch, allow writing as usual (pass through)
        self._files[self._cidx][self._gpath][key] = val  # type: ignore


class ArdiemGroup(ArdiemInnerNode):
    """`ArdiemNode` representing a `h5py.Group`."""

    def __init__(self, files, gpath: str = "/", creation_idx: Optional[int] = None):
        if creation_idx is None:
            creation_idx = _latest_container_idx(files, gpath)
            assert creation_idx is not None  # this is only used with /, which exists
        super().__init__(files, gpath, creation_idx, False)

    @property
    def attrs(self):
        return ArdiemAttributeManager(self._files, self._gpath, self._cidx)

    def create_group(self, gpath):
        """Create a group (overrides whatever was at the path in previous versions)."""
        self._create_group(gpath)


class ArdiemAttributeManager(ArdiemInnerNode):
    """`ArdiemNode` representing an `h5py.AttributeManager`."""

    def __init__(self, files, gpath, creation_idx):
        super().__init__(files, gpath, creation_idx, True)
