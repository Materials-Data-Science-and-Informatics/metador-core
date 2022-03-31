"""Tests for ArdiemRecord."""
import inspect
from pathlib import Path

import h5py
import pytest

from ardiem_container.hashutils import HASH_ALG, dir_hashsums
from ardiem_container.ih5 import IH5Record
from ardiem_container.packer import ArdiemPacker, DirDiff, ValidationErrors
from ardiem_container.record import ArdiemRecord, ValidationError


class DummyPacker(ArdiemPacker):
    """The dummy packer."""

    fail_check_directory = False
    fail_check_record = False

    ex_check_directory = False
    ex_check_record = False
    ex_pack_directory = False

    @classmethod
    def check_directory(cls, data_dir: Path) -> ValidationErrors:
        print(f"called: {inspect.currentframe().f_code.co_name}")  # type: ignore
        if not cls.fail_check_directory:
            return {}
        return {"error": ["value"]}

    @classmethod
    def check_record(cls, record: IH5Record) -> ValidationErrors:
        print(f"called: {inspect.currentframe().f_code.co_name}")  # type: ignore
        if not cls.fail_check_record:
            return {}
        return {"error": ["value"]}

    @classmethod
    def pack_directory(
        cls, data_dir: Path, diff: DirDiff, record: IH5Record, fresh: bool
    ):
        if cls.ex_pack_directory:
            raise ValueError("Packing failed.")

        print(f"called: {inspect.currentframe().f_code.co_name}")  # type: ignore
        print(f"from {data_dir} to {record._files[-1].filename} (fresh={fresh})")
        print(diff)
        # just count number of patches (base container is 0)
        if "updates" not in record.attrs:
            record.attrs["updates"] = 0
        else:
            if not isinstance(record.attrs["updates"], h5py.Empty):
                record.attrs["updates"] = record.attrs["updates"] + 1


def test_create_fail(tmp_ds_path, tmp_path):
    data_dir = tmp_path
    ds_path = tmp_ds_path

    # not a directory
    with pytest.raises(ValueError) as e:
        ArdiemRecord.create(ds_path, Path(tmp_path / "invalid_dir"), DummyPacker)
    assert str(e).lower().find("invalid") >= 0

    # directory is empty
    with pytest.raises(ValueError) as e:
        ArdiemRecord.create(ds_path, data_dir, DummyPacker)
    assert str(e).lower().find("empty") >= 0

    (data_dir / "dummy_file").touch()

    with ArdiemRecord.create(ds_path, data_dir, DummyPacker) as ds:
        ds_uuid = ds.record.uuid

    # record exists
    with pytest.raises(FileExistsError) as e:
        ArdiemRecord.create(ds_path, data_dir, DummyPacker)
    assert str(e).lower().find("exists") >= 0

    # with overwrite, should work
    with ArdiemRecord.create(ds_path, data_dir, DummyPacker, overwrite=True) as ds:
        assert ds.record.uuid != ds_uuid  # overwritten -> also changed uuid


def test_create_update_record_minimal(tmp_ds_path, tmp_path):
    ds_path = tmp_ds_path
    data_dir = tmp_path
    (data_dir / "dummy_file").touch()

    with ArdiemRecord.create(ds_path, data_dir, DummyPacker) as ds:
        assert ds.name == ds_path.name
        assert ds.record.read_only
        assert len(ds.record.containers) == 1
        assert ds.manifest.record_uuid == ds.record.ih5meta[-1].record_uuid
        assert ds.manifest.patch_uuid == ds.record.ih5meta[-1].patch_uuid
        assert ds.manifest.patch_index == ds.record.ih5meta[-1].patch_index
        assert ds.manifest.hashsums == dir_hashsums(data_dir, HASH_ALG)
        assert ds.record.attrs["updates"] == 0

    with ArdiemRecord.open(ds_path) as ds:
        # unchanged dir -> refuse update
        with pytest.raises(ValueError):
            ds.update(data_dir, DummyPacker)
        assert len(ds.record.containers) == 1

        (data_dir / "dummy_file").unlink()
        (data_dir / "dummy_file2").touch()

        # update, check manifest also is updated and record patch succeeded
        ds.update(data_dir, DummyPacker)
        assert ds.manifest.patch_index == ds.record.ih5meta[-1].patch_index
        assert ds.manifest.hashsums == dir_hashsums(data_dir, HASH_ALG)
        assert ds.record.attrs["updates"] == 1
        assert len(ds.record.containers) == 2

        # update with allow_unchanged=True
        ds.update(data_dir, DummyPacker, allow_unchanged=True)
        assert ds.record.attrs["updates"] == 2
        assert len(ds.record.containers) == 3


def test_open_kwargs_fail(tmp_ds_path, tmp_path):
    (tmp_path / "dummy_file").touch()
    with ArdiemRecord.create(tmp_ds_path, tmp_path, DummyPacker):
        pass

    # unknown kwarg
    with pytest.raises(ValueError) as e:
        ArdiemRecord.open(tmp_ds_path, something=True)
    assert str(e).lower().find("unknown") >= 0

    # these arguments are mutually exclusive. should be catched
    with pytest.raises(ValueError) as e:
        ArdiemRecord.open(tmp_ds_path, missing_manifest=True, only_manifest=True)
    assert str(e).lower().find("exclusive") >= 0


def test_open_manifest_mismatch_fail(tmp_ds_path, tmp_path):
    (tmp_path / "dummy_file").touch()
    with ArdiemRecord.create(tmp_ds_path, tmp_path, DummyPacker) as ds:
        # rename old manifest
        old_manifest = ds._manifest_filepath(ds._path)
        new_mf = Path(str(old_manifest) + ".bak")
        old_manifest.rename(new_mf)

        (tmp_path / "dummy_file2").touch()
        ds.update(tmp_path, DummyPacker)

        # overwrite new manifest with old
        new_mf.rename(old_manifest)

    # expect manifest mismatch
    with pytest.raises(ValidationError) as e:
        ArdiemRecord.open(tmp_ds_path)
    assert str(e).lower().find("match") >= 0


def test_open_missing_manifest(tmp_ds_path, tmp_path):
    ds_path = tmp_ds_path
    data_dir = tmp_path
    (data_dir / "dummy_file").touch()

    # create record, keep just manifest
    with ArdiemRecord.create(ds_path, data_dir, DummyPacker) as ds:
        mfile = ds._manifest_filepath(ds._path)
    mfile.unlink()

    with pytest.raises(FileNotFoundError) as e:
        ArdiemRecord.open(ds_path)
    assert str(e).lower().find("not exist") >= 0

    # now it should work
    with ArdiemRecord.open(ds_path, missing_manifest=True) as ds:
        assert ds.record is not None
        assert ds.manifest is None


def test_open_only_manifest(tmp_ds_path, tmp_path):
    ds_path = tmp_ds_path
    data_dir = tmp_path
    (data_dir / "dummy_file").touch()

    # create record, keep just manifest
    with ArdiemRecord.create(ds_path, data_dir, DummyPacker) as ds:
        cfile = ds.record.containers[0]
    cfile.unlink()

    # no container found
    with pytest.raises(FileNotFoundError) as e:
        ArdiemRecord.open(ds_path)
    assert str(e).lower().find("no containers found") >= 0

    # now it should work
    with ArdiemRecord.open(ds_path, only_manifest=True) as ds:
        assert ds.record is None
        assert ds.manifest is not None


def test_update_stub_minimal(tmp_ds_path, tmp_path):
    ds_path = tmp_ds_path
    data_dir = tmp_path
    (data_dir / "dummy_file").touch()

    # create record, keep just manifest
    with ArdiemRecord.create(ds_path, data_dir, DummyPacker) as ds:
        cfiles = ds.record.containers
    cfiles[0].unlink()

    (data_dir / "dummy_file2").touch()

    # update -> should create a stub based on manifest and patch that without issues
    with ArdiemRecord.open(ds_path, only_manifest=True) as ds:
        ds.update(data_dir, DummyPacker)
        assert ds.record.ih5meta[0].is_stub
        assert not ds.record.ih5meta[1].is_stub


def test_packer_fail_check_directory(tmp_ds_path, tmp_path):
    ds_path = tmp_ds_path
    data_dir = tmp_path
    (data_dir / "dummy_file").touch()

    DummyPacker.fail_check_directory = True
    with pytest.raises(ValidationError) as e:
        ArdiemRecord.create(ds_path, data_dir, DummyPacker)
    DummyPacker.fail_check_directory = False

    # got expected error
    assert "error" in e.value.args[0]
    # no container created
    assert len(IH5Record.find_containers(ds_path)) == 0
