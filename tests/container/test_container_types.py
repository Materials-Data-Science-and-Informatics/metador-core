import h5py

from metador_core.container.types import (
    H5DatasetLike,
    H5FileLike,
    H5GroupLike,
    H5NodeLike,
)


def test_instance_checks(tmp_ds_path):
    with h5py.File(tmp_ds_path / "blub.h5", "w") as f:
        f["group/dataset"] = 123

        assert isinstance(f, H5NodeLike)
        assert isinstance(f, H5FileLike)
        assert isinstance(f, H5GroupLike)
        assert not isinstance(f, H5DatasetLike)

        g = f["group"]
        assert isinstance(f, H5NodeLike)
        assert not isinstance(g, H5FileLike)
        assert isinstance(g, H5GroupLike)
        assert not isinstance(g, H5DatasetLike)

        d = g["dataset"]
        assert isinstance(f, H5NodeLike)
        assert not isinstance(d, H5FileLike)
        assert not isinstance(d, H5GroupLike)
        assert isinstance(d, H5DatasetLike)
