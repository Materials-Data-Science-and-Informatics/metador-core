"""Test IH5MF record with manifest."""
from pathlib import Path
from uuid import uuid1

import pytest

from metador_core.ih5.manifest import IH5Manifest, IH5MFRecord, IH5UBExtManifest
from metador_core.ih5.record import IH5Record, IH5UserBlock


def latest_manifest_filepath(ds):
    return ds._manifest_filepath(ds._files[-1].filename)


def test_ubext_manifest():
    ub = IH5UserBlock.create()
    assert IH5UBExtManifest.get(ub) is None
    ubext = IH5UBExtManifest(
        is_stub_container=True, manifest_uuid=uuid1(), manifest_hashsum="sha256:0"
    )
    ubext.update(ub)
    assert IH5UBExtManifest.get(ub) == ubext


def test_fresh_record_no_manifest_exception(tmp_ds_path):
    with IH5MFRecord(tmp_ds_path, "w") as ds:
        with pytest.raises(ValueError):
            ds.manifest  # fresh uncommitted record has no manifest
        ds.commit_patch()
        ds.manifest  # no exception now


def test_commit_with_exts(tmp_ds_path):
    with IH5MFRecord(tmp_ds_path, "w") as ds:
        ds.commit_patch(manifest_exts={"test_ext": "yeah!"})
    # desired extra metadata section is stored successfully
    with IH5MFRecord(tmp_ds_path) as ds:
        assert ds.manifest.manifest_exts["test_ext"] == "yeah!"


def test_load_ih5mf_as_ih5(tmp_ds_path):
    # IH5MF containers (with patches and manifests) open cleanly as IH5 (ignore ext.)
    mf_files = []
    with IH5MFRecord(tmp_ds_path, "w") as ds:
        ds["foo/bar"] = "hello"
        ds.commit_patch()
        mf_files.append(latest_manifest_filepath(ds))

        ds.create_patch()
        ds["foo/baz"] = "world"
        ds.commit_patch()
        mf_files.append(latest_manifest_filepath(ds))

        assert len(ds.ih5_meta[-1].ub_exts.keys()) > 0  # userblock ext present

    # Manifest files exist for each patch
    assert all(map(lambda x: Path(x).is_file(), mf_files))

    with IH5Record(tmp_ds_path) as ds:
        # should make no problems, even though there are manifest files
        # and extras in the user block
        assert ds["foo/bar"][()] == b"hello"
        assert ds["foo/baz"][()] == b"world"


def test_commit_no_patch_fail(tmp_ds_path):
    # opening ih5mf with latest container lacking correct ub ext should fail
    with IH5MFRecord(tmp_ds_path, "w") as ds:
        ds.commit_patch()
        with pytest.raises(ValueError):  # not writable
            ds.commit_patch()


def test_missing_ub_ext_fail(tmp_ds_path):
    # opening ih5mf with latest container lacking correct ub ext should fail
    with IH5MFRecord(tmp_ds_path, "w") as ds:
        pass
    with IH5Record(tmp_ds_path, "r+") as ds:
        pass  # commits patch without manifest!
    with IH5MFRecord(tmp_ds_path, "r+") as ds:
        with pytest.raises(ValueError):  # no manifest
            ds.manifest
        ds.commit_patch()
        ds.manifest  # now exists


def test_missing_latest_manifest_fail(tmp_ds_path):
    # opening ih5mf with latest container lacking manifest should fail
    ds_mf = ""
    with IH5MFRecord(tmp_ds_path, "w") as ds:
        ds.commit_patch()
        ds.create_patch()
        ds["foo"] = 0
        ds.commit_patch()
        ds_mf = latest_manifest_filepath(ds)

    ds_mf.unlink()  # kill latest manifest
    with pytest.raises(ValueError):
        IH5MFRecord(tmp_ds_path)


def test_modified_latest_manifest_fail(tmp_ds_path):
    # opening ih5mf with latest container lacking manifest should fail
    ds_mf = ""
    with IH5MFRecord(tmp_ds_path, "w") as ds:
        ds.commit_patch()
        ds.create_patch()
        ds["foo"] = 0
        ds.commit_patch()
        ds_mf = latest_manifest_filepath(ds)

    with open(ds_mf, "w") as f:
        f.write("{}")  # overwrite manifest... changes hashsum

    with pytest.raises(ValueError):
        IH5MFRecord(tmp_ds_path)


def test_create_stub_from_mf_and_patch(tmp_ds_path_factory):
    # Skeleton from manifest can be used to create a valid stub and patch it
    ds1 = tmp_ds_path_factory()
    mf = ""
    files = []
    with IH5MFRecord(ds1, "w") as ds:
        ds["foo/bar"] = "hello"
        ds.commit_patch()

        ds.create_patch()
        ds["foo/baz"] = "world"
        ds.commit_patch()
        files = ds.ih5_files
        mf = latest_manifest_filepath(ds)

    # create a stub and add a patch
    ds2 = tmp_ds_path_factory()
    mf2 = ""
    with IH5MFRecord.create_stub(ds2, mf) as stub:
        stub.create_patch()
        stub["qux"] = "patch"
        stub.commit_patch()
        mf2 = latest_manifest_filepath(stub)
        files.append(stub.ih5_files[-1])

    # apply the patch to the orig dataset
    with IH5MFRecord._open(files, manifest_file=mf2) as ds:
        assert ds["qux"][()] == b"patch"


def test_merge_stub_fail(tmp_ds_path_factory):
    # Merging with a stub container fails
    ds_path = tmp_ds_path_factory()
    mf = ""
    with IH5MFRecord(ds_path, "w") as ds:
        ds["foo/bar"] = "hello"
        ds.commit_patch()
        mf = latest_manifest_filepath(ds)

    stub_path = tmp_ds_path_factory()
    merged_path = tmp_ds_path_factory()
    with IH5MFRecord.create_stub(stub_path, mf) as stub:
        assert IH5UBExtManifest.get(stub.ih5_meta[-1]).is_stub_container

        stub.create_patch()
        stub["qux"] = "patch"
        stub.commit_patch()

        with pytest.raises(ValueError) as e:
            stub.merge_files(merged_path)
        assert str(e).find("stub") >= 0


def test_merge_correct(tmp_ds_path_factory):
    # Merging should preserve latest manifest
    ds1_path = tmp_ds_path_factory()
    ds2_path = tmp_ds_path_factory()
    mf1_path = ""
    files = []
    # manifest extension names
    ext1 = "test_mfext"
    ext2 = "fresh_mfext"
    with IH5MFRecord(ds1_path, "w") as ds:
        ds["foo/bar"] = "hello"
        ds.commit_patch(manifest_exts={ext1: "hello"})
        mf_v1 = ds.manifest

        ds.create_patch()
        ds["foo/baz"] = "world"
        ds.manifest.manifest_exts[ext2] = "world"
        ds.commit_patch()
        mf_v2 = ds.manifest

        # new manifest should be fresh but inherit attached data
        assert mf_v1.manifest_uuid != mf_v2.manifest_uuid
        assert mf_v1.manifest_exts.get(ext1) == mf_v2.manifest_exts.get(ext1)
        assert mf_v2.manifest_exts.get(ext2) == "world"

        files = ds.ih5_files
        mf1_path = latest_manifest_filepath(ds)

        ds.merge_files(ds2_path)

    with IH5MFRecord(ds2_path, "r") as ds:
        # we should open this successfully... now check that the manifest agrees
        mf2_path = latest_manifest_filepath(ds)
        mf1 = IH5Manifest.parse_file(mf1_path)
        mf2 = IH5Manifest.parse_file(mf2_path)
        assert mf1 == mf2
        assert ext1 in mf2.manifest_exts  # also has the extensions
        assert ext2 in mf2.manifest_exts

    with IH5MFRecord(ds2_path, "r+") as ds:
        # create a new patch on top of merged container
        ds["qux"] = 123
        files.append(ds.ih5_files[-1])

    # open patch done on top of merged container, now with the original containers.
    # it should infer manifest file of most recent patch and work fine.
    with IH5MFRecord._open(files) as ds:
        assert ds["qux"][()] == 123
        assert "test_mfext" in ds.manifest.manifest_exts
