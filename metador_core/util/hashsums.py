from __future__ import annotations

import hashlib
import os
from io import BytesIO
from pathlib import Path
from typing import Any, BinaryIO, Dict, Optional, Union

_hash_alg = {
    # "md5": hashlib.md5,
    # "sha1": hashlib.sha1,
    "sha256": hashlib.sha256,
    "sha512": hashlib.sha512,
}
"""Supported hashsum algorithms."""


def hashsum(data: Union[bytes, BinaryIO], alg: str):
    """Compute hashsum from given binary file stream using selected algorithm."""
    if isinstance(data, bytes):
        data = BytesIO(data)
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


def qualified_hashsum(data: Union[bytes, BinaryIO], alg: str = DEF_HASH_ALG):
    """Like hashsum, but prepends the algorithm to the string."""
    return f"{alg}:{hashsum(data, alg)}"


def file_hashsum(path: Path, alg: str = DEF_HASH_ALG):
    with open(path, "rb") as f:
        return qualified_hashsum(f, alg)


DirHashsums = Dict[str, Any]
"""
Nested dict representing a directory.

str values represent files (checksum) or symlinks (target path),
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


# ----
