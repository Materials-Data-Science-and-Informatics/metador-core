from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, BinaryIO, Dict, List, Optional, Union

from pydantic import BaseModel
from typing_extensions import Literal

ValidationErrors = Dict[str, List[Any]]
"""Common type used to collect errors.

Maps path in container or directory to list of errors with that path.

The list should contain either strings or other ValidationErrors dicts,
but Python type checkers are unable to understand recursive types.
"""

_hash_alg = {
    "md5": hashlib.md5,
    "sha1": hashlib.sha1,
    "sha256": hashlib.sha256,
    "sha512": hashlib.sha512,
}


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


DirHashsums = Dict[str, Any]
"""
Nested dict representing a directory.

str values represent files by their checksum,
dict values represent sub-directories.
"""


def dir_hashsums(dir: Path, alg: str) -> DirHashsums:
    """Return hashsums of all files.

    Resulting paths are relative to the provided `dir`.
    """
    ret: Dict[str, Any] = {}
    for path in dir.rglob("*"):
        relpath = path.relative_to(dir)
        is_file = path.is_file()
        fname = None
        chksum = None
        if is_file:
            fname = relpath.name
            relpath = relpath.parent
            with open(path, "rb") as f:
                chksum = f"{alg}:" + hashsum(f, alg)

        curr = ret
        for seg in str(relpath).split("/"):
            if seg == ".":
                continue
            if seg not in curr:
                curr[seg] = dict()
            curr = curr[seg]
        if is_file:
            assert fname is not None
            curr[fname] = chksum
    return ret


def dir_paths(base_dir: Path):
    """Recursively list all paths in given directory, relative to itself."""
    return map(lambda p: p.relative_to(base_dir), sorted(base_dir.rglob("*")))


PathStatus = Optional[Literal["+", "-", "~"]]


class DirDiff(BaseModel):
    """Interface to directory diffs based on comparing `DirHashsums`.

    An instance represents a change at the path.

    Granular change inspection is accessible through the provided methods.

    Typically, you will want to capture `dir_hashsums` of the same `dir` at two points
    in time and will be interested in the dict `DirDiff.compare(prev, curr).annotate()`.
    """

    path: Path
    """Location represented by this node."""

    prev: Union[None, str, Dict[str, Any]]
    """Previous entity at this location."""

    curr: Union[None, str, Dict[str, Any]]
    """Current entity at this location."""

    added: Dict[Path, Any] = {}
    """New files and subdirectories."""

    removed: Dict[Path, Any] = {}
    """Deleted files and subdirectories."""

    modified: Dict[Path, Any] = {}
    """Modified or replaced files and subdirectories."""

    def annotate(self, base_dir: Path) -> Dict[Path, PathStatus]:
        """Return a dict of path -> status mappings based on passed directory.

        The keys are all paths that exist in this diff object as well as
        all paths that currently exist in the passed `base_dir`.
        The keys will all have `base_dir` as a prefix path, but not contain itself.
        The values will be the results of applying `status` to these paths.
        """
        paths = sorted(set(dir_paths(base_dir)) | set(self.paths()))
        return {base_dir / str(p): self.status(p) for p in paths}

    def paths(self) -> List[Path]:
        """Return list of all paths in this diff.

        Will include all paths except the base directory (".") of the diff.
        """
        ret = []
        if self.path != Path(""):
            ret.append(self.path)
        buckets = [self.added, self.removed, self.modified]
        for b in buckets:
            for k, v in b.items():
                ret += v.paths()
        return ret

    def status(self, rel_path: Path) -> PathStatus:
        """Check the given path (which is assumed to be relative to this diff node).

        Returns None, if the path is not included in the diff (i.e. assumed unchanged).
        Returns "+", "-" or "~" if the path was added, removed or modified, respectively.
        """
        if rel_path == self.path:  # match
            if self.prev is None:
                return "+"
            elif self.curr is None:
                return "-"
            else:  # we can assume self.prev != self.curr
                return "~"

        buckets = [self.added, self.removed, self.modified]
        for b in buckets:
            for k, v in b.items():
                if k == rel_path or k in rel_path.parents:
                    return v.status(rel_path)
        return None  # not found -> not included in diff -> unchanged

    @classmethod
    def compare(
        cls,
        prev: Optional[DirHashsums],
        curr: Optional[DirHashsums],
        path: Path = Path(""),
    ) -> Optional[DirDiff]:
        """Compare two nested file and directory hashsum dicts.

        Will return a DirDiff object that contains only further nested objects,
        if there are differences between the prev and curr DirHashsums.
        """
        ret = cls(path=path, prev=prev, curr=curr)
        if not isinstance(prev, dict) and not isinstance(curr, dict):
            if prev == curr:
                # same (non-)file -> no diff
                # for top-level path: return object even if contents are equal
                return ret if ret.path == Path("") else None
            else:
                return ret  # indicates that a change happened

        if (prev is None or isinstance(prev, str)) and isinstance(curr, dict):
            # file -> dir: everything inside "added"
            for k, v in curr.items():
                kpath = ret.path / k
                ret.added[kpath] = cls.compare(None, v, kpath)
            return ret

        if isinstance(prev, dict) and (curr is None or isinstance(curr, str)):
            # dir -> file: everything inside "removed"
            for k, v in prev.items():
                kpath = ret.path / k
                ret.removed[kpath] = cls.compare(v, None, kpath)
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
            ret.added[kpath] = cls.compare(None, curr[k], kpath)
        for k in removed:  # removed in curr
            kpath = ret.path / k
            ret.removed[kpath] = cls.compare(prev[k], None, kpath)
        for k in intersection:  # changed in curr
            kpath = ret.path / k
            diff = cls.compare(prev[k], curr[k], kpath)
            if diff is not None:  # add child if there is a difference
                ret.modified[kpath] = diff

        # all children same -> directories same
        same_dir = not ret.added and not ret.removed and not ret.modified

        # for top-level path: return object even if contents are equal
        if same_dir and ret.path != Path(""):
            return None
        return ret
