import pytest

from metador_core.container.drivers import (
    METADOR_DRIVER_CLASSES,
    MetadorDriverEnum,
    get_driver_type,
    get_source,
    to_h5filelike,
)
from metador_core.container.protocols import (
    H5DatasetLike,
    H5FileLike,
    H5GroupLike,
    H5NodeLike,
)


@pytest.mark.parametrize("driver", METADOR_DRIVER_CLASSES)
def test_instance_checks(driver, tmp_ds_path):
    """Check that all driver classes work correctly with the protocol types.

    Expected relationships (< = base_class_of):
       H5DatasetLike > H5NodeLike < H5GroupLike < H5FileLike
    """
    tmp_ds_path.mkdir()
    with driver(tmp_ds_path / "testcontainer", "w") as f:
        # create group and dataset node
        f["group/dataset"] = 123

        # check that behavior is correct:

        # file-like:
        assert isinstance(f, H5NodeLike)
        assert isinstance(f, H5FileLike)
        assert isinstance(f, H5GroupLike)
        assert not isinstance(f, H5DatasetLike)

        # group-like:
        g = f["group"]
        assert isinstance(f, H5NodeLike)
        assert not isinstance(g, H5FileLike)
        assert isinstance(g, H5GroupLike)
        assert not isinstance(g, H5DatasetLike)

        # dataset-like:
        d = g["dataset"]
        assert isinstance(f, H5NodeLike)
        assert not isinstance(d, H5FileLike)
        assert not isinstance(d, H5GroupLike)
        assert isinstance(d, H5DatasetLike)


@pytest.mark.parametrize("driver_type", tuple(iter(MetadorDriverEnum)))
def test_driver_type_detection(driver_type, tmp_ds_path):
    d_cls = driver_type.value
    tmp_ds_path.mkdir()
    filename = "testcontainer"

    with d_cls(tmp_ds_path / filename, "w") as f:
        get_driver_type(f) == driver_type

    class sub_d_cls(d_cls):
        ...  # try with subclass

    with sub_d_cls(tmp_ds_path / filename, "w") as f:
        get_driver_type(f) == driver_type

    with pytest.raises(ValueError):
        get_driver_type(123)


@pytest.mark.parametrize("driver_type", tuple(iter(MetadorDriverEnum)))
def test_driver_source_detection(driver_type, tmp_ds_path):
    d_cls = driver_type.value
    tmp_ds_path.mkdir()
    filename = "testcontainer"

    with d_cls(tmp_ds_path / filename, "w") as f:
        # extract data source object
        src = get_source(f)

    with d_cls(src, "r") as f:
        # can open it again using returned data source object
        pass

    with pytest.raises(ValueError):
        get_source(123)


def test_to_h5filelike(tmp_ds_path):
    tmp_ds_path.mkdir()

    with pytest.raises(ValueError):
        # unknown driver class
        to_h5filelike(tmp_ds_path / "some_file", driver=object)

    with MetadorDriverEnum.HDF5.value(tmp_ds_path / "hdf5_file", "w") as f:
        # returns back same object
        assert to_h5filelike(f) is f

    # opens object with default driver
    assert isinstance(
        to_h5filelike(tmp_ds_path / "hdf5_file"), MetadorDriverEnum.HDF5.value
    )
