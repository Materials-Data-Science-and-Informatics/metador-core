"""Test IH5MF record with manifest."""
from pathlib import Path
from uuid import uuid1
from metador_core.ih5.record import IH5Record, IH5UserBlock
from metador_core.ih5.manifest import IH5UBExtManifest, IH5MFRecord, IH5ManifestFile

import pytest

def latest_manifest_filepath(ds):
    return ds._manifest_filepath(ds._files[-1].filename)

def test_ubext_manifest():
    ub = IH5UserBlock.create()
    assert IH5UBExtManifest.get(ub) is None
    ubext = IH5UBExtManifest(is_stub_container=True, manifest_uuid=uuid1(),
                             manifest_hashsum="sha256:0")
    ubext.update(ub)
    assert IH5UBExtManifest.get(ub) == ubext

def test_fresh_record_no_manifest_exception(tmp_ds_path):
    with IH5MFRecord.create(tmp_ds_path) as ds:
        with pytest.raises(ValueError):
            ds.manifest # fresh uncommitted record has no manifest
        ds.commit()
        ds.manifest # no exception now

def test_commit_with_exts(tmp_ds_path):
    with IH5MFRecord.create(tmp_ds_path) as ds:
        ds.commit(manifest_exts={"test_ext": "yeah!"})
    # desired extra metadata section is stored successfully
    with IH5MFRecord.open(tmp_ds_path) as ds:
        assert ds.manifest.manifest_exts["test_ext"] == "yeah!"


def test_load_ih5mf_as_ih5(tmp_ds_path):
    # IH5MF containers (with patches and manifests) open cleanly as IH5 (ignore ext.)
    mf_files = []
    with IH5MFRecord.create(tmp_ds_path) as ds:
        ds["foo/bar"] = "hello"
        ds.commit()
        mf_files.append(latest_manifest_filepath(ds))

        ds.create_patch()
        ds["foo/baz"] = "world"
        ds.commit()
        mf_files.append(latest_manifest_filepath(ds))

        assert len(ds.ih5_meta[-1].ub_exts.keys()) > 0 # userblock ext present

    # Manifest files exist for each patch
    assert all(map(lambda x: Path(x).is_file(), mf_files))

    with IH5Record.open(tmp_ds_path) as ds:
        # should make no problems, even though there are manifest files
        # and extras in the user block
        assert ds["foo/bar"][()] == b"hello"
        assert ds["foo/baz"][()] == b"world"

def test_commit_no_patch_fail(tmp_ds_path):
    # opening ih5mf with latest container lacking correct ub ext should fail
    with IH5MFRecord.create(tmp_ds_path) as ds:
        ds.commit()
        with pytest.raises(ValueError): # not writable
            ds.commit()

def test_missing_ub_ext_fail(tmp_ds_path):
    # opening ih5mf with latest container lacking correct ub ext should fail
    with IH5MFRecord.create(tmp_ds_path) as ds:
        ds.commit()
    with IH5Record.open(tmp_ds_path) as ds:
        ds.create_patch()
        ds.commit() # patch without manifest!
    with IH5MFRecord.open(tmp_ds_path) as ds:
        with pytest.raises(ValueError):  # no manifest
            ds.manifest
        ds.create_patch() # create a patch just to add the manifest on commit
        ds.commit()
        ds.manifest # now exists

def test_missing_latest_manifest_fail(tmp_ds_path):
    # opening ih5mf with latest container lacking manifest should fail
    ds_mf = ""
    with IH5MFRecord.create(tmp_ds_path) as ds:
        ds.commit()
        ds.create_patch()
        ds["foo"] = 0
        ds.commit()
        ds_mf = latest_manifest_filepath(ds)

    ds_mf.unlink() # kill latest manifest
    with pytest.raises(ValueError):
        IH5MFRecord.open(tmp_ds_path)

def test_modified_latest_manifest_fail(tmp_ds_path):
    # opening ih5mf with latest container lacking manifest should fail
    ds_mf = ""
    with IH5MFRecord.create(tmp_ds_path) as ds:
        ds.commit()
        ds.create_patch()
        ds["foo"] = 0
        ds.commit()
        ds_mf = latest_manifest_filepath(ds)

    with open(ds_mf, "w") as f:
        f.write("{}") # overwrite manifest... changes hashsum

    with pytest.raises(ValueError):
        IH5MFRecord.open(tmp_ds_path)

def test_create_stub_from_mf_and_patch(tmp_ds_path_factory):
    # Skeleton from manifest can be used to create a valid stub and patch it
    ds1 = tmp_ds_path_factory()
    mf = ""
    files = []
    with IH5MFRecord.create(ds1) as ds:
        ds["foo/bar"] = "hello"
        ds.commit()

        ds.create_patch()
        ds["foo/baz"] = "world"
        ds.commit()
        files = ds.containers
        mf = latest_manifest_filepath(ds)

    # create a stub and add a patch
    ds2 = tmp_ds_path_factory()
    mf2 = ""
    with IH5MFRecord.create_stub(ds2, mf) as stub:
        stub.create_patch()
        stub["qux"] = "patch"
        stub.commit()
        mf2 = latest_manifest_filepath(stub)
        files.append(stub.containers[-1])

    # apply the patch to the orig dataset
    with IH5MFRecord(files, manifest_file=mf2) as ds:
        assert ds["qux"][()] == b"patch"

def test_merge_stub_fail(tmp_ds_path_factory):
    # Merging with a stub container fails
    ds_path = tmp_ds_path_factory()
    mf = ""
    with IH5MFRecord.create(ds_path) as ds:
        ds["foo/bar"] = "hello"
        ds.commit()
        mf = latest_manifest_filepath(ds)

    stub_path = tmp_ds_path_factory()
    merged_path = tmp_ds_path_factory()
    with IH5MFRecord.create_stub(stub_path, mf) as stub:
        assert IH5UBExtManifest.get(stub.ih5_meta[-1]).is_stub_container

        stub.create_patch()
        stub["qux"] = "patch"
        stub.commit()

        with pytest.raises(ValueError) as e:
            stub.merge(merged_path)
        assert str(e).find("stub") >= 0

def test_merge_correct(tmp_ds_path_factory):
    # Merging should preserve latest manifest
    ds1_path = tmp_ds_path_factory()
    ds2_path = tmp_ds_path_factory()
    mf1_path = ""
    files = []
    with IH5MFRecord.create(ds1_path) as ds:
        ds["foo/bar"] = "hello"
        ds.commit(manifest_exts={"test_mfext": "hello"})
        mf_v1 = ds.manifest

        ds.create_patch()
        ds["foo/baz"] = "world"
        ds.commit()  
        mf_v2 = ds.manifest

        # new manifest should be fresh but inherit attached data
        assert mf_v1.manifest_uuid != mf_v2.manifest_uuid
        assert mf_v1.manifest_exts == mf_v2.manifest_exts

        files = ds.containers
        mf1_path = latest_manifest_filepath(ds)

        ds.merge(ds2_path)


    with IH5MFRecord.open(ds2_path) as ds:
        #we should open this successfully...

        # now check that the manifest agrees
        mf2_path = latest_manifest_filepath(ds)
        mf1 = IH5ManifestFile.parse_file(mf1_path)
        mf2 = IH5ManifestFile.parse_file(mf2_path)
        assert mf1 == mf2
        assert "test_mfext" in mf2.manifest_exts  # also has the extension

        # create a new patch on top of merged container
        ds.create_patch()
        ds["qux"] = 123
        ds.commit()
        files.append(ds.containers[-1])

    # open patch done on top of merged container, now with the original containers.
    # it should infer manifest file of most recent patch and work fine.
    with IH5MFRecord(files) as ds:
        assert ds["qux"][()] == 123
        assert "test_mfext" in ds.manifest.manifest_exts
