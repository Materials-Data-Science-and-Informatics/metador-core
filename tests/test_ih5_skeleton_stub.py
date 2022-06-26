"""Test skeleton and stub creation (decoupled from manifest file)."""
import pytest

from metador_core.ih5.containers import IH5Record
from metador_core.ih5.skeleton import ih5_skeleton, init_stub_skeleton, init_stub_base

def test_ih5_skeleton(tmp_ds_path_factory):
    with IH5Record.create(tmp_ds_path_factory()) as ds:
        ds["foo/bar"] = "hello"
        ds.attrs["root_attr"] = 1
        ds["foo"].attrs["group_attr"] = 2
        ds["foo/bar"].attrs["dataset_attr"] = 3

        assert ih5_skeleton(ds) == {
            "@root_attr": "attribute",
            "foo@group_attr": "attribute",
            "foo/bar@dataset_attr": "attribute",
            "foo/bar": "dataset",
            "foo": "group",
        }

def test_init_stub_skeleton(tmp_ds_path_factory):
    with IH5Record.create(tmp_ds_path_factory()) as ds:
        with pytest.raises(ValueError):
            skel = { # dict in key creation order. Key order should not matter!
                "foo@atr": "attribute",  # first creates the attribute...
                "foo": "dataset",  # ...then explicitly value
                "qux/bar": "dataset",  # ... first implicitly create group
                "qux": "group",  # ...then explicitly
            }
            # in this order it still should work fine
            init_stub_skeleton(ds, skel)

    # now test failures
    with IH5Record.create(tmp_ds_path_factory()) as ds:
        with pytest.raises(ValueError):
            init_stub_skeleton(ds, {"foo": "invalid"})  # invalid skeleton

    with IH5Record.create(tmp_ds_path_factory()) as ds:
        ds["bar"] = "not empty anymore" # not empty, but no collision
        with pytest.raises(ValueError):
            init_stub_skeleton(ds, {"foo": "dataset"})  # not empty target

def test_patch_on_stub_works_with_real(tmp_ds_path_factory):
    # Test that patching over a stub works correctly and can be used with original.

    # create a little normal record with multiple patches
    dsname = tmp_ds_path_factory()
    stubname = tmp_ds_path_factory()
    with IH5Record.create(dsname) as ds:
        ds["foo/bar"] = [1, 2, 3]
        ds.commit()
        ds.create_patch()
        ds["foo/bar"].attrs["qux"] = 42  # type: ignore
        ds["data"] = "interesting data"
        ds.commit()
        ds.create_patch()
        ds["data"].attrs["key"] = "value"  # type: ignore
        ds["foo/muh"] = 1337
        ds["foo/tokill"] = "this will be deleted"
        ds.commit()

        ds_files = ds.containers
        ds_ub = ds.ih5_meta[-1]
        ds_sk = ih5_skeleton(ds)

        # create the stub faking this record
        with IH5Record.create(stubname) as stub:
            init_stub_base(stub, ds_ub, ds_sk)
            assert stub.read_only

            # should agree on relevant user block infos
            assert ds_ub.record_uuid == stub.ih5_meta[0].record_uuid
            assert ds_ub.patch_uuid == stub.ih5_meta[0].patch_uuid
            assert ds_ub.patch_index == stub.ih5_meta[0].patch_index
            # and of course on the skeleton
            assert ih5_skeleton(stub) == ds_sk

            # create new patch on top of stub
            stub.create_patch()
            del stub["foo/tokill"]
            del stub["foo/bar"]
            stub["/foo/bar/blub"] = True
            stub["data"].attrs["key2"] = "othervalue"
            stub["foo/bar"].attrs["qax"] = 987  # type: ignore
            stub.commit()

            assert len(stub.containers) == 2
            stub_files = stub.containers
            stub_skel = ih5_skeleton(stub)
            assert stub_skel == {
                "data": "dataset",
                "data@key": "attribute",
                "data@key2": "attribute",
                "foo": "group",
                "foo/bar": "group",
                "foo/bar@qax": "attribute",
                "foo/bar/blub": "dataset",
                "foo/muh": "dataset",
            }

    # open real record with new patch in stub
    with IH5Record([*ds_files, stub_files[-1]]) as ds:
        # first success is that it even opens without complaining.
        # now check that it's the same skeleton with the real data:
        assert ih5_skeleton(ds) == stub_skel
        # check the attributes are merged and values look as expeted
        assert set(ds["data"].attrs.keys()) == set(["key", "key2"])
        assert set(ds["foo/bar"].attrs.keys()) == set(["qax"])
        assert "foo/muh" in ds
        assert "foo/bar/blub" in ds


