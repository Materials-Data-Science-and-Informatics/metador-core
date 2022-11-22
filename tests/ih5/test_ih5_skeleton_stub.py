"""Test skeleton and stub creation (decoupled from manifest file)."""
import pytest

from metador_core.ih5.container import IH5Record
from metador_core.ih5.skeleton import (
    H5Type,
    IH5Skeleton,
    SkeletonNodeInfo,
    init_stub_base,
    init_stub_skeleton,
)


def test_ih5_skeleton(tmp_ds_path_factory):
    with IH5Record(tmp_ds_path_factory(), "w") as ds:
        ds["foo/bar"] = "hello"
        ds.attrs["root_attr"] = 1
        ds["foo"].attrs["group_attr"] = 2
        ds.commit_patch()
        ds.create_patch()
        ds["foo"].attrs["group_attr2"] = 3
        ds["foo/bar"].attrs["dataset_attr"] = 4  # type: ignore
        ds["foo/baz"] = "world"
        ds["foo/baz"].attrs["dataset_attr2"] = 5  # type: ignore

        assert IH5Skeleton.for_record(ds) == IH5Skeleton(
            __root__={
                "/": SkeletonNodeInfo(
                    node_type=H5Type.group, patch_index=0, attrs={"root_attr": 0}
                ),
                "/foo": SkeletonNodeInfo(
                    node_type=H5Type.group,
                    patch_index=0,
                    attrs={"group_attr": 0, "group_attr2": 1},
                ),
                "/foo/bar": SkeletonNodeInfo(
                    node_type=H5Type.dataset, patch_index=0, attrs={"dataset_attr": 1}
                ),
                "/foo/baz": SkeletonNodeInfo(
                    node_type=H5Type.dataset, patch_index=1, attrs={"dataset_attr2": 1}
                ),
            }
        )


def test_init_stub_skeleton(tmp_ds_path_factory):
    with IH5Record(tmp_ds_path_factory(), "w") as ds:
        skel = {  # dict in key creation order. Key order should not matter!
            "foo": {"node_type": "dataset", "patch_index": 0, "attrs": {"atr": 0}},
            "qux": {
                "node_type": "group",
                "patch_index": 0,
                "attrs": {},
            },
            "qux/bar": {
                "node_type": "dataset",
                "patch_index": 0,
                "attrs": {},
            },
        }
        # in this order it still should work fine
        init_stub_skeleton(ds, IH5Skeleton.parse_obj(skel))

    # now test failures
    with IH5Record(tmp_ds_path_factory(), "w") as ds:
        # an invalid skeleton
        with pytest.raises(ValueError):
            init_stub_skeleton(ds, IH5Skeleton.parse_obj({"foo": "invalid"}))

    with IH5Record(tmp_ds_path_factory(), "w") as ds:
        # not empty, but no collision
        ds["bar"] = "not empty anymore"
        with pytest.raises(ValueError):
            init_stub_skeleton(ds, IH5Skeleton.parse_obj({"foo": "dataset"}))


def test_patch_on_stub_works_with_real(tmp_ds_path_factory):
    # Test that patching over a stub works correctly and can be used with original.

    # create a little normal record with multiple patches
    dsname = tmp_ds_path_factory()
    stubname = tmp_ds_path_factory()
    with IH5Record(dsname, "w") as ds:
        ds["foo/bar"] = [1, 2, 3]
        ds.commit_patch()
        ds.create_patch()
        ds["foo/bar"].attrs["qux"] = 42  # type: ignore
        ds["data"] = "interesting data"
        ds.commit_patch()
        ds.create_patch()
        ds["data"].attrs["key"] = "value"  # type: ignore
        ds["foo/muh"] = 1337
        ds["foo/tokill"] = "this will be deleted"
        ds.commit_patch()

        ds_files = ds.ih5_files
        ds_ub = ds.ih5_meta[-1]
        ds_sk = IH5Skeleton.for_record(ds)

        # create the stub faking this record
        with IH5Record(stubname, "w") as stub:
            init_stub_base(stub, ds_ub, ds_sk)
            stub.commit_patch()

            # should agree on relevant user block infos
            assert ds_ub.record_uuid == stub.ih5_meta[0].record_uuid
            assert ds_ub.patch_uuid == stub.ih5_meta[0].patch_uuid
            assert ds_ub.patch_index == stub.ih5_meta[0].patch_index
            # and of course on the skeleton
            latest_pidx = stub.ih5_meta[-1].patch_index
            assert IH5Skeleton.for_record(stub) == ds_sk.with_patch_index(latest_pidx)

            # create new patch on top of stub
            stub.create_patch()
            del stub["foo/tokill"]
            del stub["foo/bar"]
            stub["/foo/bar/blub"] = True
            stub["data"].attrs["key2"] = "othervalue"
            stub["foo/bar"].attrs["qax"] = 987  # type: ignore
            stub.commit_patch()

            assert len(stub.ih5_files) == 2
            stub_files = stub.ih5_files
            stub_skel = IH5Skeleton.for_record(stub)
            assert stub_skel == IH5Skeleton.parse_obj(
                {
                    "/": {"node_type": "group", "patch_index": 2, "attrs": {}},
                    "/data": {
                        "node_type": "dataset",
                        "patch_index": 2,
                        "attrs": {"key": 2, "key2": 3},
                    },
                    "/foo": {"node_type": "group", "patch_index": 2, "attrs": {}},
                    "/foo/bar": {
                        "node_type": "group",
                        "patch_index": 3,
                        "attrs": {"qax": 3},
                    },
                    "/foo/bar/blub": {
                        "node_type": "dataset",
                        "patch_index": 3,
                        "attrs": {},
                    },
                    "/foo/muh": {"node_type": "dataset", "patch_index": 2, "attrs": {}},
                }
            )

    # open real record with new patch in stub
    with IH5Record._open([*ds_files, stub_files[-1]]) as ds:
        # first success is that it even opens without complaining.
        # now check that it's the same skeleton with the real data:
        # (ignoring patch indices, which are different)
        ds_skel = IH5Skeleton.for_record(ds)
        assert ds_skel.with_patch_index(0) == stub_skel.with_patch_index(0)
        # check the attributes are merged and values look as expeted
        assert set(ds["data"].attrs.keys()) == set(["key", "key2"])
        assert set(ds["foo/bar"].attrs.keys()) == set(["qax"])
        assert "foo/muh" in ds
        assert "foo/bar/blub" in ds
