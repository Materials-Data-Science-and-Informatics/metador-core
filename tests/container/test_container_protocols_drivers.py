import numpy as np
import pytest

from metador_core.container.drivers import (
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


def check_protocols_for(f):
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


def test_instance_checks(fresh_mc):
    # Check that all driver classes work correctly with the protocol types.
    #
    # Expected relationships (< = base_class_of):
    #  H5DatasetLike > H5NodeLike < H5GroupLike < H5FileLike
    check_protocols_for(fresh_mc.__wrapped__)


def test_instance_checks_wrapped(fresh_mc):
    # same, but wrapped with MetadorContainer
    check_protocols_for(fresh_mc)


# ----


def test_driver_type_detection(tmp_mc_path, mc_driver):
    with mc_driver.value(tmp_mc_path, "w") as f:
        # check to get expected driver type
        assert get_driver_type(f) == mc_driver

    class sub_d_cls(mc_driver.value):
        ...

    with sub_d_cls(tmp_mc_path, "r") as f:
        # a subclass should be still same "driver type"
        assert get_driver_type(f) == mc_driver

    with pytest.raises(ValueError):
        # invalid argument type
        get_driver_type(123)


def test_driver_source_detection(tmp_mc_path, mc_driver):
    with mc_driver.value(tmp_mc_path, "w") as f:
        # extract data source object
        src = get_source(f, driver=mc_driver)

    with mc_driver.value(src, "r") as f:
        # can open it again using returned data source object
        pass

    with pytest.raises(ValueError):
        # invalid argument type
        get_source(123)


def test_to_h5filelike(tmp_ds_path):
    def_drv_cls = MetadorDriverEnum.HDF5.value

    tmp_ds_path.mkdir()

    with pytest.raises(ValueError):
        # unknown driver class
        to_h5filelike(tmp_ds_path / "some_file", driver=object)

    with def_drv_cls(tmp_ds_path / "hdf5_file", "w") as f:
        # returns back same object
        assert to_h5filelike(f) is f

    # opens object with default driver
    assert isinstance(to_h5filelike(tmp_ds_path / "hdf5_file"), def_drv_cls)


# ----
# test driver implementation, i.e. the methods in the protocols
# actually work as expected and needed for the Metador interface.
# rigorous testing should be done for each driver separately.


def test_h5filelike(tmp_mc_path, mc_driver):
    # 'with' notation should work (__enter__, __exit__)
    with mc_driver.value(tmp_mc_path, "w") as f:
        # new file is empty and writable
        assert len(f) == 0
        assert f.mode == "r+"
        # check that we can write a value as expected
        f["a"] = b"b"

    with mc_driver.value(tmp_mc_path, "r") as f:
        # existing file is readable and contains what it should
        assert len(f) == 1
        assert f.mode == "r"
        assert list(f.keys()) == ["a"]
        assert f["a"][()] == b"b"

    f = mc_driver.value(tmp_mc_path, "r")
    f.close()
    with pytest.raises(ValueError):
        f["a"]  # access to a closed file should not work


def test_h5datasetlike(fresh_raw_managed):
    # test simple datatypes that are important
    c = fresh_raw_managed

    i = c.create_dataset("int", data=123)
    assert i.ndim == 0
    assert i[()] == 123  # __getattr__

    s = c.create_dataset("str", data="hello")
    assert s.ndim == 0
    assert s[()] == b"hello"

    b = c.create_dataset("bytes", data=b"world")
    assert b.ndim == 0
    assert b[()] == b"world"

    v = c.create_dataset("void", data=np.void(b"wrapped"))
    assert v.ndim == 0
    assert v[()].tolist() == b"wrapped"

    lst = c.create_dataset("mat", data=[[1, 2], [3, 4]])
    assert lst.ndim == 2
    assert lst[1, 1] == 4
    lst[1, 1] = 5  # __setattr__, nontrivial indexing
    assert lst[1, 1] == 5


def test_h5nodelike(fresh_raw_managed):
    f = fresh_raw_managed
    f["a/b"] = 123
    g = f["a"]
    d = g["b"]

    assert f.name == "/"
    assert g.name == "/a"
    assert d.name == "/a/b"

    assert d.parent == g
    assert g.parent == f

    assert g.file == f
    assert d.file == f

    # TODO: should it be checked more than that attrs exist?
    # we don't use them but packers might.
    # depends on whether packers should be driver agnostic
    assert f.attrs is not None
    assert d.attrs is not None


def test_h5grouplike(fresh_raw_managed):
    f = fresh_raw_managed

    assert "a" not in f
    assert "a/b" not in f
    assert f.get("a/b/c") is None

    f["a/b"] = 123  # __setattr__
    g = f["a"]  # __getattr__
    d = g.get("b")
    assert d[()] == 123
    g2 = g.create_group("d")
    g2.create_dataset("/a/c", data=456)
    assert len(g2) == 0

    tmp_d = g2.require_dataset("e", shape=(10, 10), dtype="float64")
    assert isinstance(tmp_d, H5DatasetLike)
    tmp_g = g2.require_group("f")
    assert isinstance(tmp_g, H5GroupLike)
    with pytest.raises(TypeError):
        g2.require_group("e")
    with pytest.raises(TypeError):
        g2.require_dataset("f", shape=(10, 10), dtype="float64")

    assert len(g2) == 2
    del g2["e"]  # __delitem__
    del g["/a/d/f"]
    assert len(g2) == 0

    assert list(f.values()) == [g]
    assert dict(f.items()) == {"a": g}
    assert list(f.keys()) == ["a"]
    assert list(iter(f)) == ["a"]
    assert list(g.keys()) == ["b", "c", "d"]

    assert "/a" in f
    assert "a/b" in f
    assert "b" not in f
    assert "b" in g
    assert "/a" in g
    assert "/b" not in g
    assert "/a/b" in g

    assert f["a/b"] == f["/a/b"]
    assert f["a/b"] == g["b"]
    assert f["/"] == g["/"]

    g.move("/a", "/g")
    f.move("g/b", "b")
    f["g"].move("c", "/c")
    f.move("/g", "a")
    assert list(f.keys()) == ["a", "b", "c"]
    assert list(f["a"].keys()) == ["d"]

    f.copy("b", f["a/d"])
    f.copy("c", "/a/d/c")
    f["a"].copy(f["a/d"], f)
    assert list(f.keys()) == ["a", "b", "c", "d"]
    assert list(f["d"].keys()) == ["b", "c"]
    assert f["d/b"][()] == 123

    collected = []

    def collect(name):
        collected.append(name)

    f["a"].visit(collect)
    assert collected == ["d", "d/b", "d/c"]

    collected = []

    def collect(name, node):
        assert node.name == f"/a/{name}"
        collected.append(name)

    f["a"].visititems(collect)
    assert collected == ["d", "d/b", "d/c"]
