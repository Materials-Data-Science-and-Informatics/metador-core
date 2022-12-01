"""Test plain IH5 record."""
from pathlib import Path
from uuid import uuid1

import h5py
import numpy as np
import pytest

from metador_core.ih5.container import IH5Record, IH5UserBlock


def test_raw_open_empty_record():
    # list of files must be non-empty
    with pytest.raises(ValueError):
        IH5Record._open([])


def test_open_create_invalid_record_name(tmp_ds_path):
    # record names: only alphanumeric and dashes allowed
    for name in ["invalid.name", "invalid_name", "Юникод", "inva#lid"]:
        assert not IH5Record._is_valid_record_name(name)
        with pytest.raises(ValueError):
            IH5Record(tmp_ds_path / name)
        with pytest.raises(ValueError):
            IH5Record(tmp_ds_path / name, "w")


def test_open_r(tmp_ds_path):
    # not existing yet -> cannot open
    assert IH5Record.find_files(tmp_ds_path) == []

    with pytest.raises(FileNotFoundError):
        IH5Record(tmp_ds_path)
    with IH5Record(tmp_ds_path, "w"):
        pass
    with IH5Record(tmp_ds_path) as ds:
        assert len(ds.ih5_files) == 1
        assert ds.mode == "r"
        with pytest.raises(ValueError):
            ds["test"] = "write"


def test_open_rplus(tmp_ds_path):
    # not existing yet -> cannot open
    assert IH5Record.find_files(tmp_ds_path) == []
    with pytest.raises(FileNotFoundError):
        IH5Record(tmp_ds_path)
    with IH5Record(tmp_ds_path, "w"):
        pass
    with IH5Record(tmp_ds_path, "r+") as ds:
        assert len(ds.ih5_files) == 2
        assert ds.mode == "r+"
        ds["test"] = "write"  # no error


def test_open_a(tmp_ds_path):
    # missing -> should be created
    assert IH5Record.find_files(tmp_ds_path) == []
    with IH5Record(tmp_ds_path, "a") as ds:
        assert ds.mode == "r+"
        ds["test"] = "write"  # writable
    # creates a new patch for existing one
    with IH5Record(tmp_ds_path, "a") as ds:
        assert ds.mode == "r+"
        assert len(ds.ih5_files) == 2
        assert ds["test"][()] == b"write"
        ds["test2"] = "write_more"  # writable


def test_open_w(tmp_ds_path):
    # Open with "w" will kill _all_ connected files if a record exists
    with IH5Record(tmp_ds_path, "w") as ds:
        for _ in range(3):
            ds.commit_patch()
            ds.create_patch()
        old_ds_uuid = ds.ih5_uuid

    with IH5Record(tmp_ds_path, "w") as ds:
        pass

    with IH5Record(tmp_ds_path) as ds:
        assert ds.ih5_uuid != old_ds_uuid
        assert len(ds.ih5_files) == 1


def test_open_x(tmp_ds_path):
    # x/w- should not overwrite existing
    with IH5Record(tmp_ds_path, "w"):
        pass
    with pytest.raises(FileExistsError):
        IH5Record(tmp_ds_path, "x")
    with pytest.raises(FileExistsError):
        IH5Record(tmp_ds_path, "w-")


def test_create_open(tmp_ds_path):
    # create a container, add some stuff, do some sanity checks
    ds = IH5Record(tmp_ds_path, "w")

    # check that the expected container path is created and properties are set
    assert len(ds.ih5_files) == 1
    assert len(IH5Record.find_files(tmp_ds_path)) == 1
    assert str(ds.ih5_files[0]).find(str(tmp_ds_path)) == 0
    assert ds._infer_name(tmp_ds_path) == tmp_ds_path.name
    assert ds.ih5_uuid == ds.ih5_meta[0].record_uuid

    # add some data
    ds["foo"] = "bar"
    del ds["foo"]
    ds["blub"] = "bla"
    ds.create_group("grp")
    ds.attrs["qux"] = 123  # type: ignore
    ds.close()

    with IH5Record(tmp_ds_path) as ds2:
        assert "blub" in ds2
        assert "grp" in ds2
        assert "something" not in ds2
        assert "qux" in ds2.attrs

        assert ds2["blub"][()] == b"bla"
        assert ds2.attrs["qux"] == 123

        assert set(ds2.keys()) == set(["blub", "grp"])
        assert set(iter(ds2)) == set(["blub", "grp"])
        assert set(ds2.values()) == set(map(lambda x: ds2[x], ds2.keys()))
        assert set(map(lambda x: x[1], ds2.items())) == set(ds2.values())

        # we're read-only now, so these should fail:
        with pytest.raises(ValueError):
            ds2["key"] = 123  # setters should not work
        with pytest.raises(ValueError):
            del ds2["blub"]  # deleting should not work
        with pytest.raises(ValueError):
            ds2.commit_patch()
        with pytest.raises(ValueError):
            ds2.discard_patch()


def test_missing_userblock_open_fail(tmp_ds_path):
    # try loading with a missing userblock
    with IH5Record(tmp_ds_path, "w") as ds:
        cfile = ds.ih5_files[0]
    f = h5py.File(cfile, "w")
    f.close()

    with pytest.raises(ValueError):
        IH5Record(tmp_ds_path)


def test_broken_userblock_open_fail(tmp_ds_path):
    # try loading with an invalid userblock
    with IH5Record(tmp_ds_path, "w") as ds:
        cfile = ds.ih5_files[0]
    with open(cfile, "r+b") as f:
        f.write(b"broken")  # destroy the header

    with pytest.raises(ValueError):
        IH5Record(tmp_ds_path)


def test_larger_userblock_open(tmp_ds_path):
    # try loading a user block with bigger than default size
    # (for upward compatibility of the format, if the block needs to grow)
    with IH5Record(tmp_ds_path, "w") as ds:
        cfile = ds.ih5_files[0]
    with h5py.File(cfile, mode="w", userblock_size=2048):
        pass
    ub = IH5UserBlock.create()
    ub._userblock_size = 2048
    ub.save(cfile)

    rub = IH5UserBlock.load(cfile)
    assert rub == ub
    assert rub._userblock_size == 2048


def test_userblock_missing_save_fail(tmp_ds_path):
    # saving userblock should fail in HDF5 files that have no space reserved
    with IH5Record(tmp_ds_path, "w") as ds:
        cfile = ds.ih5_files[0]
    with h5py.File(cfile, mode="w"):
        pass
    ub = IH5UserBlock.create()

    with pytest.raises(ValueError):
        ub.save(cfile)


def test_create_patch_discard(tmp_ds_path):
    # try creating a patch and later discard it
    # also try calling these methods multiple times when it is not valid
    with IH5Record(tmp_ds_path, "w") as ds:
        assert len(ds.ih5_files) == 1
        assert ds._has_writable
        ds["foo"] = 123

        with pytest.raises(ValueError):  # cannot discard base container
            ds.discard_patch()

        with pytest.raises(ValueError):  # cannot create, have writable
            ds.create_patch()

        ds.commit_patch()  # commit base container
        assert not ds._has_writable

        with pytest.raises(ValueError):  # after commit nothing to discard
            ds.discard_patch()

        assert len(ds.ih5_files) == 1

        assert "bar" not in ds
        with pytest.raises(ValueError):  # should not work yet
            ds["bar"] = 456

        ds.create_patch()
        assert ds._has_writable
        with pytest.raises(ValueError):  # should not work twice
            ds.create_patch()
        assert len(ds.ih5_files) == 2  # new container was created

        ds["bar"] = 456
        assert "bar" in ds

        ds.discard_patch()
        assert not ds._has_writable

        assert len(ds.ih5_files) == 1
        assert "bar" not in ds

    with IH5Record(tmp_ds_path) as ds:  # re-open, look again
        assert len(ds.ih5_files) == 1
        assert "foo" in ds
        assert "bar" not in ds

        with pytest.raises(ValueError):  # cannot commit, read-only
            ds.commit_patch()


def test_create_patch_commit_patch(tmp_ds_path):
    with IH5Record(tmp_ds_path, "w") as ds:
        assert len(ds.ih5_files) == 1
        ds["foo"] = 123
        ds["foo"].attrs["qux"] = 321  # type: ignore
        ds.commit_patch()
        ds.create_patch()
        assert len(ds.ih5_files) == 2
        ds["bar"] = 456
        ds["foo"].attrs["qux"] = 789  # type: ignore
        ds.commit_patch()
        ds.create_patch()
        assert len(ds.ih5_files) == 3
        del ds["bar"]
        ds["bar"] = 1337
        ds.commit_patch()

    with IH5Record(tmp_ds_path) as ds:  # re-open, look again
        assert len(ds.ih5_files) == 3
        assert "foo" in ds
        assert "bar" in ds
        assert ds["foo"].attrs["qux"] == 789
        assert ds["bar"][()] == 1337


def test_open_scrambled_filenames(tmp_ds_path):
    # try opening files where the filenames are not systematic and out of order
    with IH5Record(tmp_ds_path, "w") as ds:
        ds.commit_patch()
        for _ in range(3):
            ds.create_patch()
            ds.commit_patch()
        files = ds.ih5_files
        uuids = [ds.ih5_meta[i].patch_uuid for i in range(4)]

    # permutate the files
    order = [3, 0, 2, 1]
    newfiles = [Path(""), Path(""), Path(""), Path("")]
    for i in range(4):
        newfiles[order[i]] = files[i].parent / f"scramble{order[i]}"
        files[i].rename(newfiles[order[i]])

    # check that the files were re-ordered back into the correct order
    with IH5Record._open(newfiles) as ds:
        for i in range(4):
            assert ds.ih5_meta[i].patch_uuid == uuids[i]


def test_check_baseless_fileset_open(tmp_ds_path):
    # try opening an incomplete (patches only) set of files forming a patch chain
    # (not many reasons to do it, but technically there is nothing special about a base)
    with IH5Record(tmp_ds_path, "w") as ds:
        ds.commit_patch()
        for _ in range(3):
            ds.create_patch()
            ds.commit_patch()
        paths = ds.ih5_files  # get container files. we care about the patches only

    with pytest.raises(ValueError):  # with base, but invalid
        IH5Record._open([paths[0], paths[2]])
    with pytest.raises(ValueError):  # baseless, but it is forbidden
        IH5Record._open(paths[1:])
    with pytest.raises(ValueError):  # baseless, but invalid
        IH5Record._open([paths[1], paths[3]], allow_baseless=True)
    with IH5Record._open(paths[1:], allow_baseless=True) as ds:  # baseless, and allowed
        pass


def test_create_patch_then_merge(tmp_ds_path_factory):
    dsname, target = tmp_ds_path_factory(), tmp_ds_path_factory()
    with IH5Record(dsname, "w") as ds:
        ds.attrs["bool_attr"] = True
        ds["foo"] = 123
        ds.commit_patch()
        ds.create_patch()
        ds["bar"] = [1, 2, 3]
        ds["bar"].attrs["str_attr"] = "something"  # type: ignore
        ds.commit_patch()
        ds.create_patch()
        ds.create_group("baz")
        ds["baz"].attrs["int_attr"] = 42
        ds["qux/dat"] = np.void(b"somedata")

        with pytest.raises(ValueError):  # not read-only -> calling merge shall fail
            ds.merge_files(target)

        ds.commit_patch()

        # now merge record and compare contents to original
        merged_file = ds.merge_files(target)
        # merged_file = merged.ih5_files[-1]
        # print(merged_file)
        # merged.close()
        with IH5Record(target) as ds2:
            # check user block
            assert ds2.ih5_uuid == ds.ih5_uuid
            assert len(ds2.ih5_files) == 1
            assert ds2._ublock(0).patch_index == ds._ublock(-1).patch_index
            assert ds2._ublock(0).patch_uuid == ds._ublock(-1).patch_uuid
            assert ds2._ublock(0).prev_patch is None
            assert ds2._ublock(0).hdf5_hashsum.find("sha256:") == 0

            # check data
            orig_nodes, copy_nodes = [], []
            ds.visit(orig_nodes.append)
            ds2.visit(copy_nodes.append)
            assert orig_nodes == copy_nodes
            assert ds.attrs["bool_attr"] == ds2.attrs["bool_attr"]
            assert ds["foo"][()] == ds2["foo"][()]  # type: ignore
            assert np.array_equal(ds["bar"][()], ds2["bar"][()])  # type: ignore
            assert ds["bar"].attrs["str_attr"] == ds2["bar"].attrs["str_attr"]  # type: ignore
            assert ds["baz"].attrs["int_attr"] == ds2["baz"].attrs["int_attr"]
            assert ds["qux/dat"][()].tobytes() == ds2["qux/dat"][()].tobytes()  # type: ignore

    # now create another patch for the original record
    # and then try to open it with the merged container
    with IH5Record(dsname, "r+") as ds:
        ds["qux/new_entry"] = "amazing data"
        assert "qux/new_entry" in ds
        del ds.attrs["bool_attr"]
        del ds["foo"]
        ds["foo"] = 456
        ds.commit_patch()
        new_patch = ds.ih5_files[-1]

    # we should be able to open the merged file with the new patch
    with IH5Record._open([merged_file, new_patch]) as ds:
        assert "bool_attr" not in ds.attrs
        assert ds["foo"][()] == 456
        assert "qux/dat" in ds
        assert "qux/new_entry" in ds
        assert ds["qux/new_entry"][()] == b"amazing data"  # type: ignore


def test_clear_empty(tmp_ds_path):
    # A cleared out multi-patch container is recognized as empty correctly.
    def is_empty(ds):
        return not len(ds) and not len(ds.attrs)

    with IH5Record(tmp_ds_path, "w") as ds:
        assert is_empty(ds)
        ds.attrs["atr"] = "value"
        assert not is_empty(ds)
        ds._clear()  # clear "for real"
        assert is_empty(ds)

        ds.attrs["atr"] = "value"
        ds.commit_patch()
        ds.create_patch()
        ds["foo"] = "bar"
        ds.commit_patch()
        ds.create_patch()
        assert not is_empty(ds)
        ds._clear()  # clear in overlay
        assert is_empty(ds)


def test_delete_record(tmp_ds_path):
    # Deleting closes and removes all files.
    with IH5Record(tmp_ds_path, "w") as ds:
        for _ in range(3):
            ds.commit_patch()
            ds.create_patch()
        files = ds.ih5_files
    assert ds._closed

    IH5Record.delete_files(tmp_ds_path)
    for file in files:
        assert not file.is_file()


def test_get(tmp_ds_path):
    # dict-like get method with default fallback value
    with IH5Record(tmp_ds_path, "w") as ds:
        assert ds.get("foo") is None
        assert ds.get("foo", 123) == 123
        ds["foo"] = 456
        assert ds.get("foo")[()] == 456


def test_create_dataset(tmp_ds_path):
    # relative and absolute paths for create_dataset + pass through arguments
    with IH5Record(tmp_ds_path, "w") as ds:
        foo = ds.create_group("foo")
        foo.create_dataset(
            "bar", data=[1, 2, 3], compression="gzip", compression_opts=9
        )
        foo.create_dataset("/baz", data=1)
        ds.create_dataset("qux/quux", data=2)
        assert "bar" in foo
        assert "foo/bar" in ds
        assert "/baz" in ds
        assert "/qux/quux" in ds

        # try invalid arguments
        with pytest.raises(ValueError) as e:
            ds.create_dataset("newdataset", unknown="arg")
        assert str(e).lower().find("unknown")


# --------
# Test opening containers with various failures
# by manipulating data into invalid states and
# checking that exceptions are correctly triggered


def test_check_ublock_inconsistent_dsuuid_fail(tmp_ds_path):
    # make that there are multiple different record uuids in the files
    with IH5Record(tmp_ds_path, "w") as ds:
        ds.commit_patch()
        ds.create_patch()
        ds._ublock(-1).record_uuid = uuid1()
        ds.commit_patch()

    with pytest.raises(ValueError):
        IH5Record(tmp_ds_path)


def test_check_ublock_inconsistent_checksum_fail(tmp_ds_path):
    # make that checksum for data is incorrect
    with IH5Record(tmp_ds_path, "w") as ds:
        ds.commit_patch()
        ds.create_patch()
        ds.commit_patch()
        ub = ds._ublock(-1)
        ub.hdf5_hashsum = (
            "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        )
        ub.save(ds._files[-1].filename)

    with pytest.raises(ValueError):
        IH5Record(tmp_ds_path)


def test_check_ublock_base_with_prev_patch_fail(tmp_ds_path):
    # make that previous patch uuid does not match
    with IH5Record(tmp_ds_path, "w") as ds:
        ds.commit_patch()
        ds.create_patch()
        ds._ublock(-1).prev_patch = None
        ds.commit_patch()

    with pytest.raises(ValueError):
        IH5Record(tmp_ds_path)


def test_check_ublock_patch_without_prev_patch_fail(tmp_ds_path):
    # make that a patch does not have a declared predecessor
    with IH5Record(tmp_ds_path, "w") as ds:
        ds.commit_patch()
        ds.create_patch()
        ds._ublock(-1).prev_patch = None
        ds.commit_patch()

    with pytest.raises(ValueError):
        IH5Record(tmp_ds_path)


def test_check_ublock_base_patch_idx_order_fail(tmp_ds_path):
    # try loading with files in invalid patch index order
    with IH5Record(tmp_ds_path, "w") as ds:
        ds.commit_patch()
        ds.create_patch()
        ds.commit_patch()
        ub = ds._ublock(0)
        ub.patch_index = 123
        ub.save(ds._files[0].filename)

        with pytest.raises(ValueError):
            ds._check_ublock(ds._files[1].filename, ds._ublock(1), ds._ublock(0))


def test_check_ublock_base_patch_uuid_mismatch_fail(tmp_ds_path):
    # make that the linked predecessor uuid does not match with actual predecessor
    with IH5Record(tmp_ds_path, "w") as ds:
        ds.commit_patch()
        ds.create_patch()
        ds._ublock(1).prev_patch = uuid1()
        ds.commit_patch()

    with pytest.raises(ValueError):
        IH5Record(tmp_ds_path)


def test_check_ublock_multiple_same_patch_uuid_fail(tmp_ds_path):
    # make that there are multiple files with the same patch UUID
    with IH5Record(tmp_ds_path, "w") as ds:
        ds.commit_patch()
        ds.create_patch()
        ds._ublock(1).patch_uuid = ds._ublock(0).patch_uuid
        ds.commit_patch()

    with pytest.raises(ValueError):
        IH5Record(tmp_ds_path)


def test_not_open_fail(tmp_ds_path_factory):
    ds = IH5Record(tmp_ds_path_factory(), "w")
    ds.close()

    def assert_ex(f):
        with pytest.raises(ValueError) as e:
            f()
        assert str(e).lower().find("not open") >= 0

    # check that public methods fail gracefully when record not open
    assert_ex(lambda: ds.create_patch())
    assert_ex(lambda: ds.discard_patch())
    assert_ex(lambda: ds.commit_patch())
    assert_ex(lambda: ds.merge_files(tmp_ds_path_factory()))
