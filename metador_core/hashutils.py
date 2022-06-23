from __future__ import annotations

import hashlib
import itertools
import os
from enum import Enum
from pathlib import Path
from typing import Any, BinaryIO, Dict, Iterable, List, Optional, Union

from pydantic import BaseModel

_hash_alg = {
    # "md5": hashlib.md5,
    # "sha1": hashlib.sha1,
    "sha256": hashlib.sha256,
    "sha512": hashlib.sha512,
}
"""Supported hashsum algorithms."""


def hashsum(data: BinaryIO, alg: str):
    """Compute hashsum from given binary file stream using selected algorithm."""
    try:
        h = _hash_alg[alg]()
    except KeyError:
        raise ValueError(f"Unsupported hashsum: {alg}")

    while True:
        chunk = data.read(h.block_size)
        if not chunk:
            break
        h.update(chunk)

    return h.hexdigest()


DEF_HASH_ALG = "sha256"
"""Algorithm to use and string to prepend to a resulting hashsum."""


def qualified_hashsum(data: BinaryIO, alg: str = DEF_HASH_ALG):
    """Like hashsum, but prepends the algorithm to the string."""
    return f"{alg}:{hashsum(data, alg)}"


def file_hashsum(path: Path, alg: str = DEF_HASH_ALG):
    with open(path, "rb") as f:
        return qualified_hashsum(f, alg)


DirHashsums = Dict[str, Any]
"""
Nested dict representing a directory.

str values represent files by their checksum or symlinks,
dict values represent sub-directories.
"""


def rel_symlink(base: Path, dir: Path) -> Optional[Path]:
    """
    From base path and a symlink path, normalize it to be relative to base.

    Mainly used to eliminate .. in paths.

    If path points outside base, returns None.
    """
    path = dir.parent / os.readlink(str(dir))
    try:
        return path.resolve().relative_to(base.resolve())
    except ValueError:
        return None  # link points outside of base directory


def dir_hashsums(dir: Path, alg: str = DEF_HASH_ALG) -> DirHashsums:
    """Return hashsums of all files.

    Resulting paths are relative to the provided `dir`.

    In-directory symlinks are treated like files and the target is stored
    instead of computing a checksum.

    Out-of-directory symlinks are not allowed.
    """
    ret: Dict[str, Any] = {}
    for path in dir.rglob("*"):
        is_file, is_sym = path.is_file(), path.is_symlink()
        relpath = path.relative_to(dir)

        fname = None
        val = ""

        if is_file or is_sym:
            fname = relpath.name
            relpath = relpath.parent  # directory dicts to create = up to parent

        if is_file:
            val = file_hashsum(path, alg)  # value = hashsum
        elif is_sym:
            sym_trg = rel_symlink(dir, path)
            if sym_trg is None:
                raise ValueError(f"Symlink inside '{dir}' points to the outside!")
            val = "symlink:" + str(sym_trg)  # value = symlink target

        # create nested dicts, if not existing yet
        curr = ret
        for seg in str(relpath).split("/"):
            if seg == ".":
                continue
            if seg not in curr:
                curr[seg] = dict()
            curr = curr[seg]
        # store file hashsum or symlink target
        if is_file or is_sym:
            assert fname is not None
            curr[fname] = val

    return ret


def dir_paths(base_dir: Path):
    """Recursively list all paths in given directory, relative to itself."""
    return map(lambda p: p.relative_to(base_dir), sorted(base_dir.rglob("*")))


class DiffNode(BaseModel):
    """Node representing a file, symlink or directory in a diff."""

    class Status(str, Enum):
        """Change that happened to a DiffNode."""

        removed = "-"
        modified = "~"
        added = "+"
        unchanged = "0"

    class ObjType(str, Enum):
        """Entities represented in a DiffNode."""

        directory = "d"
        file = "f"
        symlink = "s"

    path: Path
    """Location represented by this node."""

    prev: Union[None, str, Dict[str, Any]]
    """Previous entity at this location."""

    curr: Union[None, str, Dict[str, Any]]
    """Current entity at this location."""

    removed: Dict[Path, DiffNode] = {}
    """Deleted files and subdirectories."""

    modified: Dict[Path, DiffNode] = {}
    """Modified or replaced files and subdirectories."""

    added: Dict[Path, DiffNode] = {}
    """New files and subdirectories."""

    def _type(self, entity) -> Optional[DiffNode.ObjType]:
        if isinstance(entity, dict):
            return DiffNode.ObjType.directory
        elif entity:
            if entity.find("symlink:") == 0:
                return DiffNode.ObjType.symlink
            else:
                return DiffNode.ObjType.file
        return None

    @property
    def prev_type(self) -> Optional[DiffNode.ObjType]:
        return self._type(self.prev)

    @property
    def curr_type(self) -> Optional[DiffNode.ObjType]:
        return self._type(self.curr)

    def children(self) -> Iterable[DiffNode]:
        """Return immediate children in no specific order."""
        return itertools.chain(
            *(x.values() for x in [self.removed, self.modified, self.added])
        )

    def nodes(self) -> List[DiffNode]:
        """Return list of all nodes in this diff.

        The order will be recursively:
        removed children, modified children, itself, then added children.
        Within the same category the children are sorted in alphabetical order.
        """
        ret = []
        buckets = [self.removed, self.modified, None, self.added]
        for b in buckets:
            if b is None:
                ret.append(self)
            else:
                for v in sorted(b.values(), key=lambda x: x.path):
                    ret += v.nodes()
        return ret

    def status(self) -> DiffNode.Status:
        """Check the given path (which is assumed to be relative to this diff node).

        Returns whether the path was added, removed or modified, respectively.
        """
        if self.prev is None:
            return DiffNode.Status.added
        elif self.curr is None:
            return DiffNode.Status.removed
        else:  # we can assume self.prev != self.curr
            return DiffNode.Status.modified

    @classmethod
    def compare(
        cls,
        prev: Optional[DirHashsums],
        curr: Optional[DirHashsums],
        path: Path,
    ) -> Optional[DiffNode]:
        """Compare two nested file and directory hashsum dicts.

        Returns None if no difference is found, otherwise a DiffNode tree
        containing only the additions, removals and changes.
        """
        ret = cls(path=path, prev=prev, curr=curr)
        if not isinstance(prev, dict) and not isinstance(curr, dict):
            if prev == curr:
                return None  # same (non-)file -> no diff
            else:
                return ret  # indicates that a change happened

        if (prev is None or isinstance(prev, str)) and isinstance(curr, dict):
            # file -> dir: everything inside "added"
            for k, v in curr.items():
                kpath = ret.path / k
                d = cls.compare(None, v, kpath)
                assert d is not None
                ret.added[kpath] = d
            return ret

        if isinstance(prev, dict) and (curr is None or isinstance(curr, str)):
            # dir -> file: everything inside "removed"
            for k, v in prev.items():
                kpath = ret.path / k
                d = cls.compare(v, None, kpath)
                assert d is not None
                ret.removed[kpath] = d
            return ret

        assert isinstance(prev, dict) and isinstance(curr, dict)
        # two directories -> compare
        prev_keys = set(prev.keys())
        curr_keys = set(curr.keys())

        added = curr_keys - prev_keys
        removed = prev_keys - curr_keys
        intersection = (prev_keys | curr_keys) - added - removed

        for k in added:  # added in curr
            kpath = ret.path / k
            d = cls.compare(None, curr[k], kpath)
            assert d is not None
            ret.added[kpath] = d
        for k in removed:  # removed in curr
            kpath = ret.path / k
            d = cls.compare(prev[k], None, kpath)
            assert d is not None
            ret.removed[kpath] = d
        for k in intersection:  # changed in curr
            kpath = ret.path / k
            diff = cls.compare(prev[k], curr[k], kpath)
            if diff is not None:  # add child if there is a difference
                ret.modified[kpath] = diff

        # all children same -> directories same
        same_dir = not ret.added and not ret.removed and not ret.modified
        if same_dir:
            return None

        # return dir node with the changes
        return ret


class DirDiff:
    """Interface to directory diffs based on comparing `DirHashsums`.

    An instance represents a change at the path.

    Granular change inspection is accessible through the provided methods.

    Typically, you will want to capture `dir_hashsums` of the same `dir`
    at two points in time and will be interested in the dict
    `DirDiff.compare(prev, curr).annotate(dir)`.
    """

    _diff_root: Optional[DiffNode]

    @property
    def is_empty(self):
        return self._diff_root is None

    @classmethod
    def compare(cls, prev: DirHashsums, curr: DirHashsums) -> DirDiff:
        """Compute a DirDiff based on two DirHashsum trees.

        To be meaningful, `prev` and `curr` should be trees obtained from the
        same directory at two points in time.
        """
        ret = cls.__new__(cls)
        ret._diff_root = DiffNode.compare(prev, curr, Path(""))
        return ret

    def get(self, path: Path) -> Optional[DiffNode]:
        path = Path(path)  # if it was a str
        assert not path.is_absolute()
        if self._diff_root is None:
            return None
        curr: DiffNode = self._diff_root
        prefixes = [path] + list(path.parents)
        prefixes.pop()  # drop '.'
        while prefixes:
            path = prefixes.pop()
            node: Optional[DiffNode] = next(
                (x for x in curr.children() if x.path == path), None
            )
            if node is None:
                return None
            curr = node
        return curr

    def status(self, node: Optional[DiffNode]) -> DiffNode.Status:
        """Return the type of change that happened to a diff node.

        Wraps `node.status()` additionally covering the case that `node` is `None`.
        Useful when processing possible path nodes returned by `annotate`.
        """
        if node is None:
            return DiffNode.Status.unchanged
        return node.status()

    def annotate(self, base_dir: Path) -> Dict[Path, Optional[DiffNode]]:
        """Return a dict of path -> status mappings based on passed directory.

        The keys are all paths that exist in this diff object as well as
        all paths that currently exist in the passed `base_dir`.
        The keys will all have `base_dir` as prefix.

        The values will be the corresponding DiffNodes, if there is a change.
        If there was no change at that path, the value will be None.

        The iteration order will be such that for each entity, first the removed
        children, then changed subentities, then the entity itself, and finally
        added subentities are listed. This is useful for updating operations
        of directories and ensures that children can be deleted before their parents
        and parents can be created before their children.
        """
        if self._diff_root is None:
            return {}

        nodes = self._diff_root.nodes()  # nodes of all paths in the diff

        path_nodes = {node.path: node for node in nodes}
        # paths in base_dir, but not in the diff
        missing = sorted(set(dir_paths(base_dir)) - set(path_nodes.keys()))

        # construct result, keeping correct order and prepending base_dir
        ret: Dict[Path, Optional[DiffNode]] = {
            base_dir / str(k): v for k, v in path_nodes.items()
        }
        for path in missing:
            ret[base_dir / str(path)] = None
        return ret
