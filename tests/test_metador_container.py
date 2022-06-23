"""Tests for MetadorContainer."""
from pathlib import Path

import h5py
import pytest

from metador_core.container import MetadorContainer
from metador_core.hashutils import HASH_ALG, dir_hashsums
from metador_core.ih5 import IH5Record
from metador_core.packer.interface import DirDiff, Packer
from metador_core.packer.util import MetadorValidationErrors


class DummyPacker(Packer):
    """The dummy packer."""

    fail_check_directory = False
    fail_check_record = False
    fail_check_record_after_pack = False

    ex_check_directory = False
    ex_check_record = False
    ex_pack_directory = False

    @classmethod
    def check_directory(cls, data_dir: Path) -> MetadorValidationErrors:
        if cls.ex_check_directory:
            raise Exception("check_directory failed.")

        if cls.fail_check_directory:
            return MetadorValidationErrors({"error": ["check_directory"]})
        return MetadorValidationErrors()

    @classmethod
    def check_record(cls, record: IH5Record) -> MetadorValidationErrors:
        if cls.ex_check_record:
            raise Exception("check_record failed.")

        if cls.fail_check_record:
            return MetadorValidationErrors({"error": ["check_record"]})
        if cls.fail_check_record_after_pack and "fail" in record:
            return MetadorValidationErrors({"error": ["check_record_after_pack"]})
        return MetadorValidationErrors()

    @classmethod
    def pack_directory(
        cls, data_dir: Path, diff: DirDiff, record: IH5Record, fresh: bool
    ):
        if cls.ex_pack_directory:
            raise ValueError("pack_directory failed.")

        # just count number of patches (base container is 0)
        if "updates" not in record.attrs:
            record.attrs["updates"] = 0
        else:
            if not isinstance(record.attrs["updates"], h5py.Empty):
                record.attrs["updates"] = record.attrs["updates"] + 1

        if cls.fail_check_record_after_pack and "fail" not in record:
            record["fail"] = True


def test_create_fail(tmp_ds_path, tmp_path):
    data_dir = tmp_path
    ds_path = tmp_ds_path

    # not a directory
    with pytest.raises(ValueError) as e:
        MetadorContainer.create(ds_path, Path(tmp_path / "invalid_dir"), DummyPacker)
    assert str(e).lower().find("invalid") >= 0

    # directory is empty
    with pytest.raises(ValueError) as e:
        MetadorContainer.create(ds_path, data_dir, DummyPacker)
    assert str(e).lower().find("empty") >= 0

    (data_dir / "dummy_file").touch()

    with MetadorContainer.create(ds_path, data_dir, DummyPacker) as ds:
        ds_uuid = ds.record.uuid

    # record exists
    with pytest.raises(FileExistsError) as e:
        MetadorContainer.create(ds_path, data_dir, DummyPacker)
    assert str(e).lower().find("exists") >= 0

    # with overwrite, should work
    with MetadorContainer.create(ds_path, data_dir, DummyPacker, overwrite=True) as ds:
        assert ds.record.uuid != ds_uuid  # overwritten -> also changed uuid


def test_create_update_record_minimal(tmp_ds_path, tmp_path):
    ds_path = tmp_ds_path
    data_dir = tmp_path
    (data_dir / "dummy_file").touch()

    with MetadorContainer.create(ds_path, data_dir, DummyPacker) as ds:
        assert ds.name == ds_path.name
        assert ds.record.read_only
        assert len(ds.record.containers) == 1
        assert ds.manifest.record_uuid == ds.record.ih5_meta[-1].record_uuid
        assert ds.manifest.patch_uuid == ds.record.ih5_meta[-1].patch_uuid
        assert ds.manifest.patch_index == ds.record.ih5_meta[-1].patch_index
        assert ds.manifest.hashsums == dir_hashsums(data_dir, HASH_ALG)
        assert ds.record.attrs["updates"] == 0

    with MetadorContainer.open(ds_path) as ds:
        # unchanged dir -> refuse update
        with pytest.raises(ValueError):
            ds.update(data_dir, DummyPacker)
        assert len(ds.record.containers) == 1

        (data_dir / "dummy_file").unlink()
        (data_dir / "dummy_file2").touch()

        # update, check manifest also is updated and record patch succeeded
        ds.update(data_dir, DummyPacker)
        assert ds.manifest.patch_index == ds.record.ih5_meta[-1].patch_index
        assert ds.manifest.hashsums == dir_hashsums(data_dir, HASH_ALG)
        assert ds.record.attrs["updates"] == 1
        assert len(ds.record.containers) == 2

        # update with allow_unchanged=True
        ds.update(data_dir, DummyPacker, allow_unchanged=True)
        assert ds.record.attrs["updates"] == 2
        assert len(ds.record.containers) == 3


def test_open_kwargs_fail(tmp_ds_path, tmp_path):
    (tmp_path / "dummy_file").touch()
    with MetadorContainer.create(tmp_ds_path, tmp_path, DummyPacker):
        pass

    # unknown kwarg
    with pytest.raises(ValueError) as e:
        MetadorContainer.open(tmp_ds_path, something=True)
    assert str(e).lower().find("unknown") >= 0

    # these arguments are mutually exclusive. should be catched
    with pytest.raises(ValueError) as e:
        MetadorContainer.open(tmp_ds_path, missing_manifest=True, only_manifest=True)
    assert str(e).lower().find("exclusive") >= 0


def test_open_manifest_mismatch_fail(tmp_ds_path, tmp_path):
    (tmp_path / "dummy_file").touch()
    with MetadorContainer.create(tmp_ds_path, tmp_path, DummyPacker) as ds:
        # rename old manifest
        old_manifest = ds._manifest_filepath(ds._path)
        new_mf = Path(str(old_manifest) + ".bak")
        old_manifest.rename(new_mf)

        (tmp_path / "dummy_file2").touch()
        ds.update(tmp_path, DummyPacker)

        # overwrite new manifest with old
        new_mf.rename(old_manifest)

    # expect manifest mismatch
    with pytest.raises(ValueError) as e:
        MetadorContainer.open(tmp_ds_path)
    assert str(e).lower().find("match") >= 0


def test_open_missing_manifest(tmp_ds_path, tmp_path):
    ds_path = tmp_ds_path
    data_dir = tmp_path
    (data_dir / "dummy_file").touch()

    # create record, keep just manifest
    with MetadorContainer.create(ds_path, data_dir, DummyPacker) as ds:
        mfile = ds._manifest_filepath(ds._path)
    mfile.unlink()

    with pytest.raises(FileNotFoundError) as e:
        MetadorContainer.open(ds_path)
    assert str(e).lower().find("not exist") >= 0

    # now it should work
    with MetadorContainer.open(ds_path, missing_manifest=True) as ds:
        assert ds.record is not None
        assert ds.manifest is None


def test_open_only_manifest(tmp_ds_path, tmp_path):
    ds_path = tmp_ds_path
    data_dir = tmp_path
    (data_dir / "dummy_file").touch()

    # create record, keep just manifest
    with MetadorContainer.create(ds_path, data_dir, DummyPacker) as ds:
        cfile = ds.record.containers[0]
    cfile.unlink()

    # no container found
    with pytest.raises(FileNotFoundError) as e:
        MetadorContainer.open(ds_path)
    assert str(e).lower().find("no containers found") >= 0

    # now it should work
    with MetadorContainer.open(ds_path, only_manifest=True) as ds:
        assert ds.record is None
        assert ds.manifest is not None


def test_update_stub_minimal(tmp_ds_path, tmp_path):
    ds_path = tmp_ds_path
    data_dir = tmp_path
    (data_dir / "dummy_file").touch()

    # create record, keep just manifest
    with MetadorContainer.create(ds_path, data_dir, DummyPacker) as ds:
        cfiles = ds.record.containers
    cfiles[0].unlink()

    (data_dir / "dummy_file2").touch()

    # update -> should create a stub based on manifest and patch that without issues
    with MetadorContainer.open(ds_path, only_manifest=True) as ds:
        ds.update(data_dir, DummyPacker)
        assert ds.record.ih5_meta[0].is_stub
        assert not ds.record.ih5_meta[1].is_stub


def test_packer_fail_check_directory(tmp_ds_path, tmp_path):
    ds_path = tmp_ds_path
    data_dir = tmp_path
    (data_dir / "dummy_file").touch()

    DummyPacker.fail_check_directory = True
    with pytest.raises(MetadorValidationErrors) as e:
        MetadorContainer.create(ds_path, data_dir, DummyPacker)
    DummyPacker.fail_check_directory = False

    # got an error
    assert bool(e.value.errors)
    # no container created
    assert len(IH5Record.find_containers(ds_path)) == 0


def test_update_fail_check_record(tmp_ds_path, tmp_path):
    ds_path = tmp_ds_path
    data_dir = tmp_path
    (data_dir / "dummy_file").touch()

    # create record
    with MetadorContainer.create(ds_path, data_dir, DummyPacker) as ds:
        cfiles = ds.record.containers
        manifest = ds.manifest

    (data_dir / "dummy_file2").touch()

    DummyPacker.fail_check_record = True
    with MetadorContainer.open(ds_path) as ds:
        with pytest.raises(MetadorValidationErrors):
            ds.update(data_dir, DummyPacker)
    DummyPacker.fail_check_record = False

    # patch was not created, old manifest
    with MetadorContainer.open(ds_path) as ds:
        assert ds.record.containers == cfiles
        assert ds.manifest == manifest


def test_update_fail_check_record_after_pack(tmp_ds_path, tmp_path):
    ds_path = tmp_ds_path
    data_dir = tmp_path
    (data_dir / "dummy_file").touch()

    # create record
    with MetadorContainer.create(ds_path, data_dir, DummyPacker) as ds:
        cfiles = ds.record.containers
        manifest = ds.manifest

    (data_dir / "dummy_file2").touch()

    DummyPacker.fail_check_record_after_pack = True
    with MetadorContainer.open(ds_path) as ds:
        with pytest.raises(MetadorValidationErrors):
            ds.update(data_dir, DummyPacker)
    DummyPacker.fail_check_record_after_pack = False

    # patch was not created, old manifest
    with MetadorContainer.open(ds_path) as ds:
        assert ds.record.containers == cfiles
        assert ds.manifest == manifest


def test_errors_add_append():
    errs = MetadorValidationErrors()
    assert not errs

    errs.add("a", "entry1")
    errs.add("a", "entry2")
    assert errs
    assert errs.errors["a"] == ["entry1", "entry2"]

    errs2 = MetadorValidationErrors()
    errs2.add("a", "entry3")
    errs2.add("a", "entry4")
    errs2.add("b", "entry5")
    assert errs2.errors["a"] == ["entry3", "entry4"]
    assert errs2.errors["b"] == ["entry5"]

    errs3 = MetadorValidationErrors()
    errs3.add("b", "entry6")

    errs.append(errs2, errs3)
    assert errs.errors["a"] == ["entry1", "entry2", "entry3", "entry4"]
    assert errs.errors["b"] == ["entry5", "entry6"]
