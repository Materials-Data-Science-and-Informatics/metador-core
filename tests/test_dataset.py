from pathlib import Path
from uuid import uuid1

import h5py
import numpy as np
import pytest

from ardiem_container.ih5 import IH5Dataset, IH5UserBlock


def test_open_empty_dataset():
    # list of files must be non-empty
    with pytest.raises(ValueError):
        IH5Dataset([])


def test_open_invalid_dataset_name(ds_dir):
    # dataset names: only alphanumeric and dashes allowed
    for name in ["invalid.name", "invalid_name", "Юникод", "inva#lid"]:
        assert not IH5Dataset._is_valid_dataset_name(name)
        with pytest.raises(ValueError):
            IH5Dataset.open(ds_dir / name)
        with pytest.raises(ValueError):
            IH5Dataset.create(ds_dir / name)


def test_open_nonexisting_dataset_name(ds_dir):
    # not existing yet -> cannot open
    ds_path = ds_dir / "nonexisting"
    assert IH5Dataset.find_containers(ds_path) == []
    with pytest.raises(ValueError):
        IH5Dataset.open(ds_path)


def test_create_open(tmp_ds_path):
    # create a container, add some stuff, do some sanity checks
    ds = IH5Dataset.create(tmp_ds_path)

    # check that the expected container path is created and properties are set
    assert len(ds.containers) == 1
    assert len(IH5Dataset.find_containers(tmp_ds_path)) == 1
    assert str(ds.containers[0]).find(str(tmp_ds_path)) == 0
    assert ds.name == tmp_ds_path.name
    assert ds.uuid == ds._ublock(0).dataset_uuid

    # add some data
    ds["foo"] = "bar"
    del ds["foo"]
    ds["blub"] = "bla"
    ds.create_group("grp")
    ds.attrs["qux"] = 123  # type: ignore
    ds.close()

    with IH5Dataset.open(tmp_ds_path) as ds2:
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
            ds2.commit()
        with pytest.raises(ValueError):
            ds2.discard_patch()


def test_missing_userblock_open_fail(tmp_ds_path):
    # try loading with a missing userblock
    with IH5Dataset.create(tmp_ds_path) as ds:
        cfile = ds.containers[0]
    f = h5py.File(cfile, "w")
    f.close()

    with pytest.raises(ValueError):
        IH5Dataset.open(tmp_ds_path)


def test_broken_userblock_open_fail(tmp_ds_path):
    # try loading with an invalid userblock
    with IH5Dataset.create(tmp_ds_path) as ds:
        cfile = ds.containers[0]
    with open(cfile, "r+b") as f:
        f.write(b"broken")  # destroy the header

    with pytest.raises(ValueError):
        IH5Dataset.open(tmp_ds_path)


def test_larger_userblock_open(tmp_ds_path):
    # try loading a user block with bigger than default size
    # (for upward compatibility of the format, if the block needs to grow)
    with IH5Dataset.create(tmp_ds_path) as ds:
        cfile = ds.containers[0]
    with h5py.File(cfile, mode="w", userblock_size=2048):
        pass
    ub = IH5UserBlock.create()
    ub._userblock_size = 2048
    ub.save(cfile)

    rub = IH5UserBlock.load(cfile)
    assert rub == ub
    assert rub._userblock_size == 2048
    assert rub._filename == cfile


def test_userblock_missing_save_fail(tmp_ds_path):
    # saving userblock should fail in HDF5 files that have no space reserved
    with IH5Dataset.create(tmp_ds_path) as ds:
        cfile = ds.containers[0]
    with h5py.File(cfile, mode="w"):
        pass
    ub = IH5UserBlock.create()

    with pytest.raises(ValueError):
        ub.save(cfile)


def test_create_patch_discard(tmp_ds_path):
    # try creating a patch and later discard it
    # also try calling these methods multiple times when it is not valid
    with IH5Dataset.create(tmp_ds_path) as ds:
        assert len(ds.containers) == 1
        assert not ds.read_only
        ds["foo"] = 123

        with pytest.raises(ValueError):  # cannot discard base container
            ds.discard_patch()

        with pytest.raises(ValueError):  # cannot create, have writable
            ds.create_patch()

        ds.commit()  # commit base container
        assert ds.read_only

        with pytest.raises(ValueError):  # after commit nothing to discard
            ds.discard_patch()

        assert len(ds.containers) == 1

        assert "bar" not in ds
        with pytest.raises(ValueError):  # should not work yet
            ds["bar"] = 456

        ds.create_patch()
        assert not ds.read_only
        with pytest.raises(ValueError):  # should not work twice
            ds.create_patch()
        assert len(ds.containers) == 2  # new container was created

        ds["bar"] = 456
        assert "bar" in ds

        ds.discard_patch()
        assert ds.read_only

        assert len(ds.containers) == 1
        assert "bar" not in ds

    with IH5Dataset.open(tmp_ds_path) as ds:  # re-open, look again
        assert len(ds.containers) == 1
        assert "foo" in ds
        assert "bar" not in ds

        with pytest.raises(ValueError):  # cannot commit, read-only
            ds.commit()


def test_create_patch_commit(tmp_ds_path):
    with IH5Dataset.create(tmp_ds_path) as ds:
        assert len(ds.containers) == 1
        ds["foo"] = 123
        ds["foo"].attrs["qux"] = 321  # type: ignore
        ds.commit()
        ds.create_patch()
        assert len(ds.containers) == 2
        ds["bar"] = 456
        ds["foo"].attrs["qux"] = 789  # type: ignore
        ds.commit()
        ds.create_patch()
        assert len(ds.containers) == 3
        del ds["bar"]
        ds["bar"] = 1337
        ds.commit()

    with IH5Dataset.open(tmp_ds_path) as ds:  # re-open, look again
        assert len(ds.containers) == 3
        assert "foo" in ds
        assert "bar" in ds
        assert ds["foo"].attrs["qux"] == 789
        assert ds["bar"][()] == 1337


def test_open_scrambled_filenames(tmp_ds_path):
    # try opening files where the filenames are not systematic and out of order
    with IH5Dataset.create(tmp_ds_path) as ds:
        ds.commit()
        for _ in range(3):
            ds.create_patch()
            ds.commit()
        files = ds.containers
        uuids = [ds._ublock(i).patch_uuid for i in range(4)]

    # permutate the files
    order = [3, 0, 2, 1]
    newfiles = [Path(""), Path(""), Path(""), Path("")]
    for i in range(4):
        newfiles[order[i]] = files[i].parent / f"scramble{order[i]}"
        files[i].rename(newfiles[order[i]])

    # check that the files were re-ordered back into the correct order
    with IH5Dataset(newfiles) as ds:
        for i in range(4):
            assert ds._ublock(i).patch_uuid == uuids[i]


def test_check_baseless_fileset_open(tmp_ds_path):
    # try opening an incomplete (patches only) set of files
    with IH5Dataset.create(tmp_ds_path) as ds:
        ds.commit()
        for _ in range(3):
            ds.create_patch()
            ds.commit()
        paths = ds.containers  # get container files. we care about the patches only

    with pytest.raises(ValueError):  # with base, but invalid
        IH5Dataset([paths[0], paths[2]])
    with pytest.raises(ValueError):  # baseless, but it is forbidden
        IH5Dataset(paths[1:])
    with pytest.raises(ValueError):  # baseless, but invalid
        IH5Dataset([paths[1], paths[3]], allow_baseless=True)
    with IH5Dataset(paths[1:], allow_baseless=True) as ds:  # baseless, and allowed
        pass


def test_create_patch_then_merge(tmp_ds_path_factory):
    dsname, target = tmp_ds_path_factory(), tmp_ds_path_factory()
    with IH5Dataset.create(dsname) as ds:
        ds.attrs["bool_attr"] = True
        ds["foo"] = 123
        ds.commit()
        ds.create_patch()
        ds["bar"] = [1, 2, 3]
        ds["bar"].attrs["str_attr"] = "something"  # type: ignore
        ds.commit()
        ds.create_patch()
        ds.create_group("baz")
        ds["baz"].attrs["int_attr"] = 42
        ds["qux/dat"] = np.void(b"somedata")

        with pytest.raises(ValueError):  # not read-only -> calling merge shall fail
            ds.merge(target)
        ds.commit()

        # now merge dataset and compare contents to original
        merged_file = ds.merge(target)
        with IH5Dataset.open(target) as ds2:
            # check user block
            assert ds2.uuid == ds.uuid
            assert len(ds2.containers) == 1
            assert ds2._ublock(0).patch_index == ds._ublock(-1).patch_index
            assert ds2._ublock(0).patch_uuid == ds._ublock(-1).patch_uuid
            assert ds2._ublock(0).prev_patch is None
            assert ds2._ublock(0).data_hashsum.find("sha256:") == 0

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

    # now create another patch for the original dataset
    # and then try to open it with the merged container
    with IH5Dataset.open(dsname) as ds:
        ds.create_patch()
        ds["qux/new_entry"] = "amazing data"
        assert "qux/new_entry" in ds
        del ds.attrs["bool_attr"]
        del ds["foo"]
        ds["foo"] = 456
        ds.commit()
        new_patch = ds.containers[-1]

    # we should be able to open the merged file with the new patch
    with IH5Dataset([merged_file, new_patch]) as ds:
        assert "bool_attr" not in ds.attrs
        assert ds["foo"][()] == 456
        assert "qux/dat" in ds
        assert "qux/new_entry" in ds
        assert ds["qux/new_entry"][()] == b"amazing data"  # type: ignore


# --------
# check that exceptions are correctly triggered by manipulating data into invalid states


def test_check_ublock_inconsistent_dsuuid_fail(tmp_ds_path):
    # make that there are multiple different dataset uuids in the files
    with IH5Dataset.create(tmp_ds_path) as ds:
        ds.commit()
        ds.create_patch()
        ds._ublock(-1).dataset_uuid = uuid1()
        ds.commit()

    with pytest.raises(ValueError):
        IH5Dataset.open(tmp_ds_path)


def test_check_ublock_inconsistent_checksum_fail(tmp_ds_path):
    # make that checksum for data is incorrect
    with IH5Dataset.create(tmp_ds_path) as ds:
        ds.commit()
        ds.create_patch()
        ds.commit()
        ub = ds._ublock(-1)
        ub.data_hashsum = (
            "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        )
        ub.save(ub._filename)

    with pytest.raises(ValueError):
        IH5Dataset.open(tmp_ds_path)


def test_check_ublock_base_with_prev_patch_fail(tmp_ds_path):
    # make that previous patch uuid does not match
    with IH5Dataset.create(tmp_ds_path) as ds:
        ds.commit()
        ds.create_patch()
        ds._ublock(-1).prev_patch = None
        ds.commit()

    with pytest.raises(ValueError):
        IH5Dataset.open(tmp_ds_path)


def test_check_ublock_patch_without_prev_patch_fail(tmp_ds_path):
    # make that a patch does not have a declared predecessor
    with IH5Dataset.create(tmp_ds_path) as ds:
        ds.commit()
        ds.create_patch()
        ds._ublock(-1).prev_patch = None
        ds.commit()

    with pytest.raises(ValueError):
        IH5Dataset.open(tmp_ds_path)


def test_check_ublock_base_patch_idx_order_fail(tmp_ds_path):
    # try loading with files in invalid patch index order
    with IH5Dataset.create(tmp_ds_path) as ds:
        ds.commit()
        ds.create_patch()
        ds.commit()
        ub = ds._ublock(0)
        ub.patch_index = 123
        ub.save(ub._filename)

        with pytest.raises(ValueError):
            ds._check_ublock(ds._ublock(1), ds._ublock(0))


def test_check_ublock_base_patch_uuid_mismatch_fail(tmp_ds_path):
    # make that the linked predecessor uuid does not match with actual predecessor
    with IH5Dataset.create(tmp_ds_path) as ds:
        ds.commit()
        ds.create_patch()
        ds._ublock(1).prev_patch = uuid1()
        ds.commit()

    with pytest.raises(ValueError):
        IH5Dataset.open(tmp_ds_path)


def test_check_ublock_multiple_same_patch_uuid_fail(tmp_ds_path):
    # make that there are multiple files with the same patch UUID
    with IH5Dataset.create(tmp_ds_path) as ds:
        ds.commit()
        ds.create_patch()
        ds._ublock(1).patch_uuid = ds._ublock(0).patch_uuid
        ds.commit()

    with pytest.raises(ValueError):
        IH5Dataset.open(tmp_ds_path)
