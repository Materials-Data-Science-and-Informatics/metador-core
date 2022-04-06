"""Test hashing and diffing functions."""
from pathlib import Path
from typing import Any, Dict

import pytest

from ardiem_container.hashutils import DiffNode, DirDiff, dir_hashsums, file_hashsum


class SymLink(str):
    pass


# data directory contents
tmp1 = {
    "a": {
        "b": {
            "c.csv": """time,position
0,1
1,2.71
2,3.14""",
            "c.csv_meta.yaml": """type: table
title: Movement
columns:
  - title: time
    unit: second
  - title: position
    unit: meter""",
            "d": SymLink("../../d"),
        }
    },
    "d": SymLink("a/b"),
    "e": "will be replaced",
    "f": "will be modified",
    "_meta.yaml": "author: unchanged",
    "z": "",
}

tmp2 = {
    "a": {"b": "hello, world!"},
    "e": {"g": "is added"},
    "f": "is modified",
    "_meta.yaml": "author: unchanged",
    "z": "",
}


def prepare_dir(dir: Path, data: Dict[str, Any]):
    """Given an existing empty directory and a data dict, create structure.

    Will create nested subdirectories, files and symlinks as specified.
    """
    for k, v in data.items():
        path = dir / k
        if isinstance(v, dict):
            path.mkdir()
            prepare_dir(path, v)
        elif isinstance(v, SymLink):
            path.symlink_to(v)
        else:
            with open(path, "wb") as f:
                if isinstance(v, str):
                    v = v.encode("utf-8")
                f.write(v)


def test_hashsum(tmp_path):
    file = tmp_path / "test.txt"
    with open(file, "w") as f:
        f.write("hello world!")

    with pytest.raises(ValueError):
        file_hashsum(file, "invalid")

    hsum = file_hashsum(file, "sha256")
    assert (
        hsum
        == "sha256:7509e5bda0c762d2bac7f90d758b5b2263fa01ccbc542ab5e3df163be08e6ca9"
    )


def test_dir_hashsums_invalid_symlink_fail(tmp_path):
    prepare_dir(tmp_path, {"sym": SymLink("/outside")})
    with pytest.raises(ValueError):
        dir_hashsums(tmp_path, "sha256")


def test_dir_hashsums(tmp_path):
    prepare_dir(tmp_path, tmp1)
    assert dir_hashsums(tmp_path, "sha256") == {
        "_meta.yaml": "sha256:460bc0d9c1c5b1a090d10660b36cacdee627629f58de6abb662c697d3da6a8f4",
        "a": {
            "b": {
                "c.csv_meta.yaml": "sha256:9a95348b68d4abd55fcce0e531028ce71dac8a28753ac22d2c7b4c1a8dc3c6a2",
                "d": "symlink:a/b",
                "c.csv": "sha256:ae69f538d682dbd96528b0e19be350895083de72723f1cdb0f36f418273361c4",
            }
        },
        "e": "sha256:9c56c2e576b1e63c65e2601e182def51891f5d57def434270f6e2c1e36da5e67",
        "d": "symlink:a/b",
        "f": "sha256:4ee3d0c3360e54939cbc219959b2296ccd943826abe60b31e216abc6a17d7aa0",
        "z": "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    }


def test_dirdiff_same_empty(tmp_path):
    prepare_dir(tmp_path, tmp1)
    dhs = dir_hashsums(tmp_path, "sha256")
    diff = DirDiff.compare(dhs, dhs)
    assert diff.is_empty


def test_dirdiff_nontrivial(tmp_path):
    p_tmp1 = tmp_path / "tmp1"
    p_tmp2 = tmp_path / "tmp2"
    p_tmp1.mkdir()
    p_tmp2.mkdir()
    prepare_dir(p_tmp1, tmp1)
    prepare_dir(p_tmp2, tmp2)
    dhs1 = dir_hashsums(p_tmp1, "sha256")
    dhs2 = dir_hashsums(p_tmp2, "sha256")
    diff = DirDiff.compare(dhs1, dhs2)
    assert not diff.is_empty

    # test DirDiff.get()
    with pytest.raises(AssertionError):
        diff.get(Path("/a/b"))
    assert DirDiff.compare(dhs2, dhs2).get("a") is None  # type: ignore
    assert diff.get(Path("a/x")) is None
    ab = diff.get(Path("a/b"))
    assert ab is not None

    assert diff.status(ab) == DiffNode.Status.modified
    assert ab.prev_type == DiffNode.ObjType.directory
    assert ab.curr_type == DiffNode.ObjType.file

    d = diff.get(Path("d"))
    assert d.status() == DiffNode.Status.removed
    assert d.prev_type == DiffNode.ObjType.symlink
    assert d.curr_type is None

    e = diff.get(Path("e"))
    assert e.status() == DiffNode.Status.modified
    assert e.prev_type == DiffNode.ObjType.file
    assert e.curr_type == DiffNode.ObjType.directory

    eg = diff.get(Path("e/g"))
    assert eg.status() == DiffNode.Status.added
    assert eg.prev_type is None
    assert eg.curr_type == DiffNode.ObjType.file

    f = diff.get(Path("f"))
    assert f.status() == DiffNode.Status.modified
    assert f.prev_type == DiffNode.ObjType.file
    assert f.curr_type == DiffNode.ObjType.file

    assert diff.get(Path("_meta.yaml")) is None
    assert diff.status(diff.get(Path("_meta.yaml"))) == DiffNode.Status.unchanged

    # test annotate()

    # changed - check correct order rec(removed, modified, self, added) + unchanged
    lst = ["d", "a/b/c.csv", "a/b/c.csv_meta.yaml", "a/b/d", "a/b", "a"]
    lst += ["e", "e/g", "f", ".", "_meta.yaml", "z"]
    exp_paths = [p_tmp2 / x for x in lst]

    ordered_paths = list(diff.annotate(p_tmp2).keys())
    assert ordered_paths == exp_paths

    # unchanged -> empty annotate
    assert DirDiff.compare(dhs2, dhs2).annotate(p_tmp2) == {}  # type: ignore
