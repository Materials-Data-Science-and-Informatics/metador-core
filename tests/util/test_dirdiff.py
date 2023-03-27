"""Test dirdiff structure."""
from pathlib import Path

import pytest

from metador_core.util.diff import DiffNode, DirDiff
from metador_core.util.hashsums import dir_hashsums

pytest.skip(reason="invenio installation problem", allow_module_level=True)


def test_dir_hashsums_invalid_symlink_fail(tmp_ds_path):
    """Directories with symlinks pointing out are forbidden."""
    tmp_ds_path.mkdir()
    outside = tmp_ds_path.resolve().parent
    Path(tmp_ds_path / "sym").symlink_to(outside)

    with pytest.raises(ValueError):
        dir_hashsums(tmp_ds_path, "sha256")


def test_dirdiff_same_empty(testinputs):
    """Comparing a directory with itself should produce empty diff."""
    ds = testinputs("dirdiff1")
    dhs = dir_hashsums(ds, "sha256")
    diff = DirDiff.compare(dhs, dhs)
    assert diff.is_empty


def test_dir_hashsums(testinputs):
    ds = testinputs("dirdiff1")
    expected = {
        "_meta.yaml": "sha256:5f5480ca66e7014685c6eff80f4b6535419746517328c0a5d2f839b1a7d58de7",
        "a": {
            "b": {
                "d": "symlink:a/b",
                "c.csv": "sha256:d37ad046c2e0c067e59f64a91732873f31df478d51a26e3552e2522cade609b2",
                "c.csv_meta.yaml": "sha256:833c8d30f975d23cd0250900a5b3efa26f04a45d14c0c492a1172f9a41af9146",
            }
        },
        "e": "sha256:8fa6d6dee361e59354df82c22ba29f9ae0d45f6560a5d906b156f759b25f1b21",
        "d": "symlink:a/b",
        "f": "sha256:a0742cb9bf98c1adec87020f4b3d5a0b8db4c6c48e2bce72aaa4b77604ce948f",
        "z": "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    }
    assert dir_hashsums(ds, "sha256") == expected
    assert dir_hashsums(ds) == expected  # default algorithm is sha256


def test_dirdiff_nontrivial(testinputs):
    ds1, ds2 = testinputs("dirdiff1"), testinputs("dirdiff2")
    dhs1, dhs2 = dir_hashsums(ds1, "sha256"), dir_hashsums(ds2, "sha256")

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
    lst += ["e", "e/g", "f", ".", "h", "_meta.yaml", "z"]
    exp_paths = [ds2 / x for x in lst]

    ordered_paths = list(diff.annotate(ds2).keys())
    assert ordered_paths == exp_paths

    # unchanged -> empty annotate
    assert DirDiff.compare(dhs2, dhs2).annotate(ds2) == {}  # type: ignore
