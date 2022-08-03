"""Test dirdiff structure."""
from pathlib import Path

import pytest

from metador_core.hashutils import dir_hashsums
from metador_core.packer.diff import DiffNode, DirDiff

from .conftest import SymLink


def test_dir_hashsums_invalid_symlink_fail(tmp_path, testutils):
    testutils.prepare_dir(tmp_path, {"sym": SymLink("/outside")})
    with pytest.raises(ValueError):
        dir_hashsums(tmp_path, "sha256")


def test_dir_hashsums(tmp_path, testutils):
    testutils.prepare_dir(tmp_path, testutils.data_dir["tmp1"])
    assert dir_hashsums(tmp_path, "sha256") == {
        "example_meta.yaml": "sha256:460bc0d9c1c5b1a090d10660b36cacdee627629f58de6abb662c697d3da6a8f4",
        "a": {
            "b": {
                "d": "symlink:a/b",
                "c.csv": "sha256:d37ad046c2e0c067e59f64a91732873f31df478d51a26e3552e2522cade609b2",
                "c.csv_meta.yaml": "sha256:833c8d30f975d23cd0250900a5b3efa26f04a45d14c0c492a1172f9a41af9146",
            }
        },
        "e": "sha256:9c56c2e576b1e63c65e2601e182def51891f5d57def434270f6e2c1e36da5e67",
        "d": "symlink:a/b",
        "f": "sha256:4ee3d0c3360e54939cbc219959b2296ccd943826abe60b31e216abc6a17d7aa0",
        "z": "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    }


def test_dirdiff_same_empty(tmp_path, testutils):
    testutils.prepare_dir(tmp_path, testutils.data_dir["tmp1"])
    dhs = dir_hashsums(tmp_path, "sha256")
    diff = DirDiff.compare(dhs, dhs)
    assert diff.is_empty


def test_dirdiff_nontrivial(tmp_path, testutils):
    p_tmp1 = tmp_path / "tmp1"
    p_tmp2 = tmp_path / "tmp2"
    p_tmp1.mkdir()
    p_tmp2.mkdir()
    testutils.prepare_dir(p_tmp1, testutils.data_dir["tmp1"])
    testutils.prepare_dir(p_tmp2, testutils.data_dir["tmp2"])
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

    assert diff.get(Path("example_meta.yaml")) is None
    assert diff.status(diff.get(Path("_meta.yaml"))) == DiffNode.Status.unchanged

    # test annotate()

    # changed - check correct order rec(removed, modified, self, added) + unchanged
    lst = ["d", "a/b/c.csv", "a/b/c.csv_meta.yaml", "a/b/d", "a/b", "a"]
    lst += ["e", "e/g", "f", ".", "example_meta.yaml", "z"]
    exp_paths = [p_tmp2 / x for x in lst]

    ordered_paths = list(diff.annotate(p_tmp2).keys())
    assert ordered_paths == exp_paths

    # unchanged -> empty annotate
    assert DirDiff.compare(dhs2, dhs2).annotate(p_tmp2) == {}  # type: ignore
