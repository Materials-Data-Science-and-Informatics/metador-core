r"""
Overlay wrappers to access a virtual dataset consisting of a base container + patches.

The wrappers take an `ArdiemDataset` and take care of dispatching
requests to datasets, groups and attributes to the correct path.

## Patch Semantics

Normal datasets and groups that are present in the patch completely overwrite
the previous entities at their location, including attached attributes.
This is used for creating and substituting leaves or subtrees in the container.

Datasets/groups/attributes are deleted by replacing the group path target or attribute
value with the special value np.void(b'\x7f') (i.e. pure removal without replacement).
If the original location did not exist, this has no effect and is ignored.

Finally, granular modification of datasets/groups/attributes is done by special
pass-through groups that have an attribute named '\x10' (DLE) (with any value attached).
The meaning of this special node is to
* modify (create/substitute/delete) attributes of the node at this location
* if previous entity at this location is a group,
  recursively interpret patches in child nodes
  (if previous entity at this location is a dataset, children are ignored!)

If the most recent location did not exist (at all or was DELeted),
the pass-through group has no effect and is ignored.

## Restrictions on HDF5 File Contents

The patching mechanism imposes these small restrictions on "regular" HDF5 file contents:
    * there MUST NOT be an attribute with key b'\x10' (ASCII-DLE) attached to a group
    * there MUST NOT be a dataset or attribute with value np.void(b'\x7f') (ASCII-DEL)

## Overlay Access Algorithm

**Input:**

* valid container sequence (base container and patches in ascending order),
* path to a dataset/group node,
* optional: attribute key.

**Output:** Patch index of container that has the most recent version of the path/attr.

The corresponding container will immediately give the desired value in case of attributes
and datasets and is the lower-bound container for search of the set of children (in case
of groups) or all attributes (in both cases).

**Procedure *findContainer*:**

```
left_boundary := 0
For each pref in path_prefixes(path): (i.e. for /foo/bar: /, /foo, /foo/bar)
    i := len(container)-1 (most recent patch index)
    While i >= left_boundary:
        If pref in container[i]:
            val = container[i][pref]
            If val is a DEL node:
                raise KeyError (most recent update is that this path is deleted!)
            If val is dataset and pref != path:
                raise KeyError (cannot go "deeper", most recent node is not a group!)
            If val is not pass-through (i.e. not a "DLE-marked" group):
                left_boundary := i
        i := i - 1
    i := i + 1
    If pref not in container[i] or container[i][pref] is pass-through:
        raise KeyError (this node does not exist, at least since last substitution)

If not requesting an attribute:
    return left_boundary
Else:
    (similar loop as above)
    find most recent value between left_boundary and most recent patch
    If not found: throw KeyError
    If attribute value is DEL: throw KeyError
    return left_boundary

**Procedure *mostRecentNode*:**
as above

**Procedure *listChildren*:**
Note: basically same for *listAttributes*

Input: path, left_boundary

Go down to left_boundary and collect non-passthrough children
without overwriting (we hit the most recent one first)

Wrapper builds overlay tree storing its own left boundary and that of its children

once we have that tree, assembly is rather simple... just create most recent
non-del leaves and attributes
"""
from typing import Any, Dict, List, Optional

import h5py
import numpy as np


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


def _node_is_deletion(node) -> bool:
    """Return whether node is marking a deleted group/dataset/attribute value."""
    return _is_del_value(node[()] if isinstance(node, h5py.Dataset) else node)


# attribute key marking group substitution (instead of pass-through default for groups)
# attribute value does not matter, but should be np.Empty(None)
# if present, all children and attributes are interpreted as normal nodes, not as a patch
SUBST_KEY = "\x1a"  # ASCII SUBSTITUTE


def _node_is_virtual(node) -> bool:
    """Virtual node (i.e. transparent and only carrier for child nodes and attributes)."""
    return isinstance(node, h5py.Group) and SUBST_KEY not in node.attrs


class ArdiemNode:
    """An overlay node stands in for a group, dataset or attribute manager.

    It takes care of finding the correct container to look for the data
    and helps with patching data in a new patch container.
    """

    def __init__(self, files: List[h5py.File], gpath: str, creation_idx: int):
        """Instantiate an overlay node."""
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
        super().__init__(files, gpath, creation_idx)
        self._attrs: bool = attrs  # whether this represents an AttributeManager

    def _get_child_raw(self, key: str, cidx: int) -> Any:
        """Return given child (dataset, group, attribute) from given container."""
        if self._attrs:
            return self._files[cidx][self._gpath].attrs[key]
        else:
            path = key if key[0] == "/" else f"{self._gpath}/{key}"
            return self._files[cidx][path]

    def _get_child(self, key: str, cidx: int) -> Any:
        """Like _get_child_raw, but wraps the result with an overlay class if needed."""
        val = self._get_child_raw(key, cidx)
        pref = self._gpath if self._gpath != "/" else ""
        path = key if key[0] == "/" else f"{pref}/{key}"
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

    def _find(self, key: str) -> Optional[int]:
        """Return index of container holding that key (attribute or path), if any.

        Expects nonempty key string (attribute, or relative/absolute path).
        """
        if self._attrs:  # access attribute by name
            return self._children.get(key, None)

        segs = key.strip("/").split("/")
        if segs == [""]:  # special case: key was "/" -> segs was ['']
            return self._cidx

        # take care if the path is absolute -> need the root group!
        curr = self if key[0] != "/" else ArdiemGroup(self._files)
        nxt_cidx = 0
        # access group through possibly nested absolute or relative path
        for seg in segs:
            nxt_cidx = curr._children.get(seg, -1)
            if nxt_cidx == -1:
                return None
            curr = curr._get_child(seg, nxt_cidx)
        return nxt_cidx

    def _check_key(self, key: str):
        if key == "":
            raise ValueError("Invalid empty path!")
        if self._attrs and key.find("/") >= 0:
            raise ValueError(f"Invalid attribute key: '{key}'!")

    def __contains__(self, key: str):
        self._check_key(key)
        return self._find(key) is not None

    def __getitem__(self, key: str):
        self._check_key(key)
        found_cidx = self._find(key)
        if found_cidx is None:
            raise KeyError(f"Cannot get item, '{key}' does not exist!")
        return self._get_child(key, found_cidx)

    def __setitem__(self, key: str, val):
        self._check_key(key)
        path = key if self._attrs or key[0] == "/" else f"{self._gpath}/{key}"
        try:
            if self._attrs:
                self._files[-1][self._gpath].attrs[key] = val
            else:
                self._files[-1][path] = val
        except (RuntimeError, ValueError):
            raise ValueError("Dataset not writable! Create a patch in order to write!")

    def __delitem__(self, key: str):
        self._check_key(key)
        found_cidx = self._find(key)
        if found_cidx is None or _is_del_value(self._get_child_raw(key, found_cidx)):
            raise KeyError(f"Cannot delete, '{key}' does not exist!")

        if found_cidx == self._last_idx:
            # remove if found in newest container
            if self._attrs:
                del self._files[found_cidx][self._gpath].attrs[key]
            else:
                path = key if self._attrs or key[0] == "/" else f"{self._gpath}/{key}"
                del self._files[found_cidx][path]
        if len(self._files) > 1:
            self[key] = DEL_VALUE  # mark as deleted if doing a patch

    def keys(self):
        return self._children.keys()

    def _dict(self):
        return {k: self._get_child(k, idx) for k, idx in self._children.items()}

    def values(self):
        return self._dict().values()

    def items(self):
        return self._dict().items()


class ArdiemGroup(ArdiemInnerNode):
    """`ArdiemNode` representing a `h5py.Group`."""

    def __init__(self, files, gpath: str = "/", creation_idx: Optional[int] = None):
        if creation_idx is None:
            creation_idx = _latest_container_idx(files, gpath)
            assert creation_idx is not None  # this is only used with /, which exists
        super().__init__(files, gpath, creation_idx, False)

    @property
    def attrs(self):
        if self._gpath in self._files[-1]:
            return ArdiemAttributeManager(self._files, self._gpath, self._cidx)
        else:
            self._files[-1].create_group(self._gpath)  # create virtual group
            return ArdiemAttributeManager(self._files, self._gpath, self._last_idx)

    def create_group(self, gpath):
        """Create group, overwriting whatever was at that path."""
        path = gpath if gpath[0] == "/" else f"{self._gpath}/{gpath}"
        # replace "deleted" marker, if set
        if path in self._files[-1] and _node_is_deletion(self._files[-1][path]):
            del self._files[-1][path]
        # create group (or fail if something else exists there already)
        self._files[-1].create_group(path)
        # if this is a patch: mark as non-virtual, i.e. "overwrite" with empty group
        if len(self._files) > 1:
            self._files[-1][path].attrs[SUBST_KEY] = h5py.Empty(None)


class ArdiemAttributeManager(ArdiemInnerNode):
    """`ArdiemNode` representing an `h5py.AttributeManager`."""

    def __init__(self, files, gpath, creation_idx):
        super().__init__(files, gpath, creation_idx, True)


class ArdiemValue(ArdiemNode):
    """`ArdiemNode` representing a `h5py.Dataset`, i.e. a leaf of the tree."""

    def __init__(self, files, gpath, creation_idx):
        super().__init__(files, gpath, creation_idx)

    @property
    def attrs(self):
        if self._gpath in self._files[-1]:
            return ArdiemAttributeManager(self._files, self._gpath, self._cidx)
        else:
            self._files[-1].create_group(self._gpath)  # create virtual group
            return ArdiemAttributeManager(self._files, self._gpath, self._last_idx)

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
