import h5py

from metador_core.ih5.protocols import H5FileLike, H5GroupLike, H5DatasetLike


def test_with_h5(tmp_ds_path):
    with h5py.File(tmp_ds_path, "w") as f:
        g = f.create_group("group")
        g["dataset"] = 123
        d = f["group/dataset"]

        for c in [H5FileLike, H5GroupLike, H5DatasetLike]:
            assert not isinstance(d.attrs, c)

        assert isinstance(f, H5FileLike)
        # can't do these because file passes through to root group:
        # assert not isinstance(f, H5DatasetLike)
        # assert not isinstance(f, H5GroupLike)

        assert not isinstance(g, H5FileLike)
        assert isinstance(g, H5GroupLike)
        assert not isinstance(g, H5DatasetLike)

        assert not isinstance(d, H5FileLike)
        assert not isinstance(d, H5GroupLike)
        assert isinstance(d, H5DatasetLike)
