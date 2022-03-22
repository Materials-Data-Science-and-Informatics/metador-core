from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, BinaryIO, Dict, List, Union

from pydantic import BaseModel

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


class DirDiff(BaseModel):
    """Interface to directory diffs based on comparing `DirHashsums`.

    An instance represents a change at the path.

    Granular change inspection is accessible through the provided methods.
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

    @classmethod
    def compare(cls, prev, curr, path=""):
        """Compare two nested file and directory hashsum dicts."""
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
                ret.added[kpath] = cls.compare(None, v)
            return ret

        if isinstance(prev, dict) and (curr is None or isinstance(curr, str)):
            # dir -> file: everything inside "removed"
            for k, v in prev.items():
                kpath = ret.path / k
                ret.removed[kpath] = cls.compare(v, None)
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

        if not ret.added and not ret.removed and not ret.modified:
            return None  # all children same -> directories same
        return ret
