import h5py
import numpy as np
import pytest

from ardiem_container.ih5 import IH5Dataset, IH5Group, IH5Record
from ardiem_container.ih5.overlay import DEL_VALUE, SUBST_KEY, IH5InnerNode, IH5Node
from ardiem_container.ih5.record import ih5_type_skeleton


def create_entries(node: IH5InnerNode):
    node["array"] = [0, 0, 0]
    node["bool"] = False
    node["int"] = 0
    node["string"] = "string"
    node["raw"] = np.void(b"raw")


def fill_dummy_ds(ds: IH5Record, flat: bool):
    def next_patch():
        if not flat:
            ds.commit()
            ds.create_patch()

    create_entries(ds["/"])
    create_entries(ds.attrs)
    create_entries(ds["int"].attrs)
    next_patch()
    for name in ["a", "b"]:
        grp = ds.create_group(name)
        create_entries(grp)
        create_entries(grp.attrs)
        create_entries(grp["array"].attrs)
        next_patch()
        for subname in ["a", "b"]:
            sgrp = grp.create_group(subname)
            create_entries(sgrp)
            create_entries(sgrp["array"].attrs)
            next_patch()


@pytest.fixture
def dummy_ds_factory(tmp_ds_path_factory):
    """Return record with base container still in writable mode."""
    records = []

    def get_ds(flat: bool, commit: bool):
        ds = IH5Record.create(tmp_ds_path_factory())
        fill_dummy_ds(ds, flat)
        if commit:
            ds.commit()
        records.append(ds)
        return ds

    yield get_ds
    for ds in records:
        ds.close()


# --------


def test_node_instance_checks():
    # test that instance creation expectedly fails due to sanity checks
    with pytest.raises(ValueError):
        IH5Node([], "bla", 0)
    with pytest.raises(ValueError):
        IH5Node([None], "", 0)  # type: ignore
    with pytest.raises(ValueError):
        IH5Node([None], "bla", -1)  # type: ignore
    with pytest.raises(ValueError):
        IH5Node([None], "bla", 1)  # type: ignore
    IH5Node([None], "bla", 0)  # type: ignore


def test_ih5node_compare(tmp_ds_path_factory):
    # nodes from same open record with same path and creation index should be equal
    # everything else should not.
    ds1name, ds2name = tmp_ds_path_factory(), tmp_ds_path_factory()
    with IH5Record.create(ds1name) as ds1:
        ds1["/foo"] = "bar"
        foo = ds1["/foo"]

        assert ds1["/"] == ds1["/"]
        assert ds1["/foo"] == foo
        assert ds1["/"] != foo  # different path

        ds1.commit()
        ds1.create_patch()
        assert ds1["/foo"] == foo  # still same patch
        del ds1["/foo"]
        ds1["/foo"] = "blub"
        assert ds1["/foo"] != foo  # now different patch

        with IH5Record.create(ds2name) as ds2:
            ds2["/foo"] = "bar"
            assert ds2["/"] == ds2["/"]
            assert ds2["/foo"] != foo  # different record


def test_abs_and_rel_path(tmp_ds_path):
    with IH5Record.create(tmp_ds_path) as ds:
        ds["grp/foo/bar"] = 123

        assert ds["grp"]._abs_path("foo") == "/grp/foo"
        assert ds["grp"]._abs_path("/foo") == "/foo"
        assert ds["grp"]._rel_path("/grp/foo/bar") == "foo/bar"
        assert ds["grp"]._rel_path("foo") == "foo"
        with pytest.raises(RuntimeError):
            ds["grp"]._rel_path("/invalid")


def test_check_key(tmp_ds_path):
    # attributes cannot have slashes, paths cannot be empty
    with IH5Record.create(tmp_ds_path) as ds:
        with pytest.raises(ValueError):
            ds[""] = 1
        with pytest.raises(ValueError):
            ds["invalid@path"] = 1
        with pytest.raises(ValueError):
            ds["hello world"] = 1
        with pytest.raises(ValueError):
            ds["абвгд"] = 1
        with pytest.raises(ValueError):
            ds.attrs["invalid@attr"] = 1
        with pytest.raises(ValueError):
            ds.attrs[""] = 1
        with pytest.raises(ValueError):
            ds.attrs["/invalidattr"] = 1
        with pytest.raises(ValueError):
            ds.attrs["invalid/attr"] = 1


def test_latest_container_idx(tmp_ds_path):
    # test finding most recent entity at a path
    with IH5Record.create(tmp_ds_path) as ds:
        ds["foo/bar"] = 123
        ds.commit()
        assert IH5Group._latest_container_idx(ds._files, "/foo/bar") == 0
        ds.create_patch()
        del ds["foo/bar"]
        ds.commit()
        assert IH5Group._latest_container_idx(ds._files, "/foo/bar") is None
        ds.create_patch()
        ds["foo/bar"] = 456
        ds.commit()
        assert IH5Group._latest_container_idx(ds._files, "/foo/bar") == 2


def test_visit(tmp_ds_path):
    with IH5Record.create(tmp_ds_path) as ds:
        ds["grp/foo/bar"] = 123
        ds.attrs["rootattr"] = "yay"  # type: ignore
        ds["grp/foo/bar"].attrs["someattr"] = "value"  # type: ignore
        ds.create_group("grp/qux")

        lst = []
        # check expected return value
        ret = ds["/grp"].visit(lambda x: lst.append(x) if not lst else lst[0])
        assert ret == lst[0]

        lst.clear()
        # collect all inside group 'grp', return None -> no early return
        ds["/grp"].visit(lst.append)
        # should be relative paths in alphabetical order
        assert lst == ["foo", "foo/bar", "qux"]

        # same again, from root node
        lst.clear()
        ds.visit(lst.append)
        assert lst == ["grp", "grp/foo", "grp/foo/bar", "grp/qux"]
        assert ih5_type_skeleton(ds) == {
            "@rootattr": (None, str),
            "grp": (IH5Group, None),
            "grp/foo": (IH5Group, None),
            "grp/foo/bar": (IH5Dataset, np.int64),
            "grp/foo/bar@someattr": (None, str),
            "grp/qux": (IH5Group, None),
        }


def test_set_inside_value(tmp_ds_path):
    with IH5Record.create(tmp_ds_path) as ds:
        ds["a"] = [1, 2, 3]
        a = ds["a"]
        ds.commit()

        # cannot set, no current patch
        with pytest.raises(ValueError) as e:
            a[2] = 5
        assert str(e).lower().find("patch") >= 0

        with pytest.raises(ValueError) as e:
            ds["a"].copy_into_patch()  # type: ignore
        assert str(e).lower().find("patch") >= 0

        ds.create_patch()
        # cannot set using this node (node from previous patch)
        with pytest.raises(ValueError) as e:
            a[2] = 5
        assert str(e).lower().find("latest") >= 0

        # still cannot write (no value there)
        with pytest.raises(ValueError) as e:
            ds["a"][2] = 5
        assert str(e).lower().find("latest") >= 0

        # explicitly copy value into new patch
        a.copy_into_patch()  # type: ignore
        with pytest.raises(ValueError) as e:
            ds["a"].copy_into_patch()  # type: ignore
        assert str(e).lower().find("latest") >= 0

        ds["a"][2] = 5  # now we can modify it
        assert np.array_equal(ds["a"][()], np.array([1, 2, 5]))  # type: ignore
        ds["a"][0:2] = [3, 4]
        assert np.array_equal(ds["a"][()], np.array([3, 4, 5]))  # type: ignore


def test_create_virtual_fail(tmp_ds_path):
    # _create_virtual should fail when the path already exists
    with IH5Record.create(tmp_ds_path) as ds:
        ds["a/b/c"] = 123
        assert not ds["a"]._create_virtual("b")  # should not work, is a group
        assert not ds["a"]._create_virtual("b/c")  # should not work, is a value

        ds.commit()
        ds.create_patch()
        del ds["a/b/c"]
        assert "a/b/c" not in ds
        assert ds["a"]._create_virtual("b/c")  # should work, ignoring deletion mark


def test_forbidden_entities_fail(tmp_ds_path):
    with IH5Record.create(tmp_ds_path) as ds:
        # we should not be able to set a DEL value or a SUB attr key
        with pytest.raises(ValueError) as e:
            ds["a/b/c"] = DEL_VALUE
        assert str(e).lower().find("forbidden") >= 0
        with pytest.raises(ValueError) as e:
            ds.attrs[SUBST_KEY] = 123
        assert str(e).lower().find("invalid") >= 0

        # hard links to values and groups
        grp = ds.create_group("grp")
        grp["val"] = 123
        with pytest.raises(ValueError) as e:
            ds["a/b/c"] = grp
        assert str(e).lower().find("hard links") >= 0
        with pytest.raises(ValueError) as e:
            ds["a/b/c"] = grp["val"]
        assert str(e).lower().find("hard links") >= 0

        # symbolic and external links
        with pytest.raises(ValueError) as e:
            ds["a/b/c"] = h5py.SoftLink("/grp")
        assert str(e).lower().find("symlink") >= 0
        with pytest.raises(ValueError) as e:
            ds["a/b/c"] = h5py.ExternalLink("file.h5", "/grp")
        assert str(e).lower().find("externallink") >= 0


def test_modify_readonly_fail(dummy_ds_factory):
    # modifying record values or attributes should not work while not creating patch
    def expected_msg(e):
        return str(e).lower().find("patch") >= 0

    ds1 = dummy_ds_factory(flat=True, commit=True)
    ds2 = dummy_ds_factory(flat=False, commit=True)
    for ds in [ds1, ds2]:
        a = ds["a"]
        assert isinstance(a, IH5Group)

        # set a value
        with pytest.raises(ValueError) as e:
            a["newkey"] = "value"
        assert expected_msg(e)
        with pytest.raises(ValueError) as e:
            a.attrs["newkey"] = "value"
        assert expected_msg(e)

        # create a group
        with pytest.raises(ValueError) as e:
            ds.create_group("c")
        assert expected_msg(e)

        # try deleting
        with pytest.raises(ValueError) as e:
            del a["a"]
        with pytest.raises(ValueError) as e:
            del a["bool"]
        assert expected_msg(e)
        with pytest.raises(ValueError) as e:
            del a.attrs["newkey"]
        assert expected_msg(e)


def test_create_over_existing_fail(dummy_ds_factory):
    # overwriting records or groups should not work without deleting first (like h5py)
    def expected_msg(e):
        return str(e).lower().find("exist") >= 0

    ds1 = dummy_ds_factory(flat=True, commit=False)
    ds2 = dummy_ds_factory(flat=False, commit=False)
    for ds in [ds1, ds2]:
        with pytest.raises(ValueError) as e:
            ds["a"] = True  # value over group
        expected_msg(e)
        with pytest.raises(ValueError) as e:
            ds["/bool"] = True  # value over value
        expected_msg(e)
        with pytest.raises(ValueError) as e:
            ds.create_group("/a")  # group over group
        expected_msg(e)
        with pytest.raises(ValueError) as e:
            ds.create_group("bool")  # group over value
        expected_msg(e)


def test_delete_nonexisting_fail(dummy_ds_factory):
    # deleting non-existing entities should fail
    def expected_msg(e):
        return str(e).lower().find("exist") >= 0

    ds1 = dummy_ds_factory(flat=True, commit=False)
    ds2 = dummy_ds_factory(flat=False, commit=False)
    for ds in [ds1, ds2]:
        with pytest.raises(KeyError) as e:
            del ds["missing"]
        expected_msg(e)
        with pytest.raises(KeyError) as e:
            del ds["/missing"]
        expected_msg(e)

        # delete real thing, but twice
        del ds["a"]
        with pytest.raises(KeyError) as e:
            del ds["a"]
        del ds["bool"]
        with pytest.raises(KeyError) as e:
            del ds["bool"]
        del ds.attrs["bool"]
        with pytest.raises(KeyError) as e:
            del ds.attrs["bool"]


def test_create_access_relative_absolute(tmp_ds_path):
    # create datasets/groups using relative and absolute paths
    with IH5Record.create(tmp_ds_path) as ds:
        nested = ds.create_group("nested")
        nested.create_group("deep")
        nested.create_group("/toplevel")
        nested["data"] = 123
        nested["data"].attrs["key"] = "value"  # type: ignore
        nested["/moredata"] = 456
        ds.commit()

        assert ih5_type_skeleton(ds) == {
            "moredata": (IH5Dataset, np.int64),
            "nested": (IH5Group, None),
            "nested/data": (IH5Dataset, np.int64),
            "nested/data@key": (None, str),
            "nested/deep": (IH5Group, None),
            "toplevel": (IH5Group, None),
        }
        # access from inside using absolute or relative path
        assert ds["nested"]["data"]._gpath == "/nested/data"
        assert ds["nested"]["/moredata"]._gpath == "/moredata"
        # cannot access into dataset
        with pytest.raises(ValueError) as e:
            ds["nested/data/something"]
        assert str(e).lower().find("value") >= 0
        # cannot access, no such entity
        with pytest.raises(KeyError) as e:
            ds["nested/missing"]
        assert str(e).lower().find("exist") >= 0


def test_fresh_patch_overlay(dummy_ds_factory):
    # create a new patch with various changes to the dummy data
    ds = dummy_ds_factory(flat=False, commit=True)
    ds.create_patch()

    atrs = ds["a"].attrs
    del atrs["bool"]  # delete an attribute (will implicitly create virtual node)
    atrs["int"] = "surprise"  # overwrite an attribute
    atrs["key"] = True  # add an attribute

    del ds["b"]  # delete a group
    del ds["bool"]  # delete a value

    del ds["a/a"]  # overwrite a group with a value
    ds["a/a"] = 123

    del ds["a/b"]  # overwrite a group with a fresh group
    ds.create_group("a/b")

    del ds["a/bool"]  # overwrite a value with a new value
    ds["/a/bool"] = "new value"

    del ds["a/array"]  # overwrite a value with a fresh group with contents
    assert "a/array" not in ds
    ds["a/array/data"] = 456  # implicit group creation!
    assert "a/array" in ds
    assert "a/array/data" in ds

    ds["a"].create_group("c")  # create new group
    ds["a/c/d"] = 789  # create new value

    # set an attribute
    ds["a/c/d"].attrs["key"] = 1337  # type: ignore

    ds.commit()

    # check expected values
    assert "b" not in ds
    assert "bool" not in ds
    assert "a/a" in ds and ds["a"]["a"][()] == 123
    assert isinstance(ds["a/b"], IH5Group) and list(ds["a/b"].attrs.keys()) == []
    assert ds["a/bool"][()] == b"new value"
    assert isinstance(ds["a/array"], IH5Group)
    assert isinstance(ds["a/array/data"], IH5Dataset)
    assert isinstance(ds["/a/c"], IH5Group)
    assert isinstance(ds["/a/c/d"], IH5Dataset) and ds["a"]["c/d"][()] == 789
    assert ds["a/c/d"].attrs["key"] == 1337  # type: ignore
    assert "bool" not in ds["a"].attrs
    assert ds["a"].attrs["int"] == "surprise"
    assert ds["a"].attrs["key"] == True


def test_clear_all_override(dummy_ds_factory):
    # get multi-patch record, delete EVERYTHING
    ds = dummy_ds_factory(flat=False, commit=True)

    def clear_and_verify():
        for k in ds.keys():
            del ds[k]
        for k in ds.attrs.keys():
            del ds.attrs[k]

        # nothing should be there
        assert list(ds.attrs.keys()) == []
        assert list(ds.keys()) == []

    def create_and_verify():
        ds.create_group("a")
        ds["b/a"] = 123
        ds["/b"]["a"].attrs["atr"] = "string"  # type: ignore
        ds.create_group("b/b")

        # only the new stuff should be there
        assert ih5_type_skeleton(ds) == {
            "a": (IH5Group, None),
            "b": (IH5Group, None),
            "b/a": (IH5Dataset, np.int64),
            "b/a@atr": (None, str),
            "b/b": (IH5Group, None),
        }

    # fully clear and refill within the same patch
    ds.create_patch()
    clear_and_verify()
    create_and_verify()
    ds.commit()
    # clear in one patch, overwrite in another
    ds.create_patch()
    clear_and_verify()
    ds.commit()
    ds.create_patch()
    create_and_verify()


def test_create_value_patch_delete_value_delete_parent_group(tmp_ds_path):
    # create value inside a group, in next patch remove it,
    # then try removing the corresponding parent group
    # based on actual bug (group removal failed, but should succeed)
    with IH5Record.create(tmp_ds_path) as ds:
        ds["a/b/c"] = 123
        ds.commit()
        ds.create_patch()

        assert ds["a/b/c"][()] == 123  # type: ignore
        del ds["a/b/c"]
        assert "a/b/c" not in ds

        assert "a/b" in ds
        del ds["a/b"]
        assert "a/b" not in ds

        ds["a/b"] = 456
        assert ds["a/b"][()] == 456  # type: ignore
        assert "a/b" in ds


def test_not_open_fail(dummy_ds_factory):
    ds = dummy_ds_factory(flat=False, commit=False)
    a = ds["a"]
    ds.close()

    def assert_ex(f):
        with pytest.raises(ValueError) as e:
            f()
        assert str(e).lower().find("not open") >= 0

    # check that public methods fail gracefully when record not open
    assert_ex(lambda: a.__delitem__("b"))
    assert_ex(lambda: a.__setitem__("x/y", "z"))
    assert_ex(lambda: a.get("b"))
    assert_ex(lambda: a["b"])
    assert_ex(lambda: a.attrs)
    assert_ex(lambda: a.keys())
    assert_ex(lambda: a.values())
    assert_ex(lambda: a.items())
    assert_ex(lambda: a.create_group("some_group"))
    assert_ex(lambda: a.visit(print))
    assert_ex(lambda: "b" in a)
    assert_ex(lambda: iter(a))
