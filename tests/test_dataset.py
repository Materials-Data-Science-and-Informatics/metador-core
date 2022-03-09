import h5py
import pytest

from ardiem_container.dataset import ArdiemDataset
from ardiem_container.overlay import ArdiemAttributeManager, ArdiemGroup, ArdiemValue


@pytest.fixture(scope="session")
def ds_dir(tmpdir_factory):
    return tmpdir_factory.mktemp("test_datasets")


DS_NAME = "Test-ds1"  # dataset used throughout the tests


def test_open_create_write(ds_dir):
    # dataset names: only alphanumeric and dashes allowed
    for name in ["invalid.name", "invalid_name", "Юникод", "inva#lid"]:
        assert not ArdiemDataset._is_valid_dataset_name(name)
        with pytest.raises(ValueError):
            ArdiemDataset.open(ds_dir / name)
        with pytest.raises(ValueError):
            ArdiemDataset.create(ds_dir / name)

    # not existing yet -> cannot open
    ds_path = ds_dir / DS_NAME
    assert ArdiemDataset.find_containers(ds_path) == []
    with pytest.raises(ValueError):
        ArdiemDataset.open(ds_path)

    # now lets create a dataset -> one initial HDF5 file
    ds = ArdiemDataset.create(ds_path)
    assert len(ArdiemDataset.find_containers(ds_path)) == 1
    assert len(ds.containers) == 1
    assert ds.name == DS_NAME
    assert str(ds.uuid) == ds._files[0].attrs[ArdiemDataset.DATASET_UUID]
    assert str(ds.containers[0]).find(str(ds_path)) == 0

    # write data to the new dataset (simple scalar)
    assert "number" not in ds
    with pytest.raises(KeyError):
        ds["/number"]
    val = 42
    ds["number"] = val
    # the writing action was passed through?
    assert ds._files[-1]["number"][()] == val  # type: ignore
    # access through wrapper, relative...
    assert isinstance(ds["number"], ArdiemValue)
    assert ds["number"][()] == val  # type: ignore
    # ... and absolute
    assert ds["/number"][()] == val  # type: ignore
    assert "number" in ds
    assert "/number" in ds

    # create a group
    assert "nested" not in ds
    ds.create_group("/nested")
    assert "nested" in ds
    assert len(ds._files[-1]["/nested"].attrs.keys()) == 0  # not marked as "overwrite"
    # cannot create same group again, until we remove it
    with pytest.raises(ValueError):
        ds.create_group("/nested")
    # delete... try deleting again... and recreate group again
    del ds["nested"]
    assert "nested" not in ds
    with pytest.raises(KeyError):
        del ds["nested"]
    ds.create_group("/nested")
    assert "nested" in ds

    # add another scalar value in a nested group (should create subgroup automatically)
    with pytest.raises(KeyError):
        ds["/nested/group/string"]
    val = b"hello world"
    ds["/nested/group/string"] = val
    # the writing action was passed through?
    assert ds._files[-1]["/nested/group/string"][()] == val  # type: ignore
    # access through wrapper, relative...
    assert ds["nested/group/string"][()] == val  # type: ignore
    # ... and absolute
    assert ds["/nested/group/string"][()] == val  # type: ignore

    assert isinstance(ds["/nested"], ArdiemGroup)
    assert isinstance(ds["/nested/group"], ArdiemGroup)
    assert isinstance(ds["/nested/group/string"], ArdiemValue)

    # go into a non-root group, try setting stuff from there
    nested = ds["nested"]
    assert isinstance(nested, ArdiemGroup)
    nested["/bool"] = False  # absolute -> relative to root
    nested["bool"] = True  # relative to current group
    assert ds["/bool"][()] == False  # type: ignore
    assert ds["/nested/bool"][()] == True  # type: ignore

    # test some attributes
    assert isinstance(ds.attrs, ArdiemAttributeManager)
    assert isinstance(nested.attrs, ArdiemAttributeManager)

    # accessed keys must be nonempty
    with pytest.raises(ValueError):
        ds[""]
    with pytest.raises(ValueError):
        ds.attrs[""]
    with pytest.raises(ValueError):
        ds[""] = 123
    with pytest.raises(ValueError):
        ds.attrs[""] = 123

    # attributes cannot have slashes
    with pytest.raises(ValueError):
        ds.attrs["/invalidattr"] = "value"
    with pytest.raises(ValueError):
        nested.attrs["/invalidattr"] = "value"
    with pytest.raises(ValueError):
        nested.attrs["invalid/attr"] = "value"

    # check that attributes do not exist
    assert "rootattr" not in ds.attrs
    assert "myattr" not in nested.attrs

    # attach attribute to root
    ds.attrs["rootattr"] = 123
    assert "rootattr" in ds.attrs
    assert ds.attrs["rootattr"] == 123
    assert "rootattr" in ds["/"].attrs
    assert ds["/"].attrs["rootattr"] == 123

    # attach attribute to a nested group
    nested.attrs["myattr"] = 321
    assert "myattr" in nested.attrs
    assert nested.attrs["myattr"] == 321
    assert ds["/nested"].attrs["myattr"] == 321

    # attach attribute and remove it -> should go away without a trace
    assert "addremove" not in nested.attrs
    nested.attrs["addremove"] = 213
    assert "addremove" in nested.attrs
    assert nested.attrs["addremove"] == 213
    assert ds._files[-1]["/nested"].attrs["addremove"] == 213
    del nested.attrs["addremove"]
    assert "addremove" not in nested.attrs
    assert "addremove" not in ds._files[-1]["/nested"].attrs

    # add a dataset...
    assert "addremove" not in nested
    nested["addremove"] = 213
    assert "addremove" in nested
    assert nested["addremove"][()] == 213  # type: ignore
    assert ds._files[-1]["/nested/addremove"][()] == 213  # type: ignore
    # ... and remove (relative path) -> should go away without a trace
    del nested["addremove"]  # <- relative path
    assert "addremove" not in nested
    assert "addremove" not in ds._files[-1]["/nested"]  # type: ignore
    # ... and the same for absolute addressing:
    nested["addremove"] = 213
    assert "addremove" in nested
    assert ds._files[-1]["/nested/addremove"][()] == 213  # type: ignore
    del nested["/nested/addremove"]  # <- absolute path, starting from nested group
    assert "addremove" not in nested
    assert "addremove" not in ds._files[-1]["/nested"]  # type: ignore
    # removing already non-existing datasets should also fail:
    with pytest.raises(KeyError):
        del nested["addremove"]
    with pytest.raises(KeyError):
        del nested["/nested/addremove"]

    # check keys / values / items function
    assert set(ds.keys()) == {"number", "bool", "nested"}
    assert len(ds.values()) == len(ds.items()) == 3

    assert set(nested.attrs.keys()) == {"myattr"}
    assert set(nested.attrs.values()) == {321}
    assert dict(nested.attrs.items()) == {"myattr": 321}

    ds.close()


def test_patch_create_discard(ds_dir):
    with ArdiemDataset.open(ds_dir / DS_NAME) as ds:
        assert len(ds.containers) == 1

        # try writing without starting a patch -> (custom) error
        with pytest.raises(ValueError) as e:
            ds["/nested/int"] = 1706
        assert str(e).find("patch") >= 0

        with pytest.raises(ValueError) as e:
            ds["/nested"].attrs["newattr"] = 1993
        assert str(e).find("patch") >= 0

        # create patch
        ds.create_patch()
        assert len(ds.containers) == 2

        # add a group
        ds["nested"].create_group("nestmore")
        with pytest.raises(ValueError):
            ds["nested"].create_group("nestmore")
        del ds["nested/nestmore"]
        with pytest.raises(KeyError):
            del ds["nested/nestmore"]
        ds["nested"].create_group("nestmore")

        assert isinstance(ds._files[-1]["/nested/nestmore"], h5py.Group)
        assert isinstance(ds["/nested/nestmore"], ArdiemGroup)

        # add a dataset and an attribute
        ds["/nested/int"] = 1706
        assert ds["/nested/int"][()] == 1706  # type: ignore

        ds["/nested"].attrs["newattr"] = 1993
        assert ds["/nested"].attrs["newattr"] == 1993

        # discard patch, assume all changes are gone again
        ds.discard_patch()
        assert len(ds.containers) == 1

        with pytest.raises(KeyError):
            ds["/nested/int"]
        with pytest.raises(KeyError):
            ds["/nested/nestmore"]
        with pytest.raises(KeyError):
            ds["/nested"].attrs["newattr"]


# base container layout:
# ---------------
# /@rootattr = 123
# /number = 42
# /bool = False
# /nested@myattr = 321
# /nested/bool = True
# /nested/group/string = "hello"


def test_patch1(ds_dir):
    with ArdiemDataset.open(ds_dir / DS_NAME) as ds:
        assert len(ds.containers) == 1
        ds.create_patch()

        # attribute deletion, substitution and updating.
        # should create virtual groups for patching on the fly

        assert "attribute" not in ds["/number"].attrs
        ds["/number"].attrs["attribute"] = "value"  # add (to dataset)
        assert ds["number"].attrs["attribute"] == "value"

        assert "newattr" not in ds["nested"].attrs
        ds["nested"].attrs["newattr"] = 432  # add (to group)
        assert "newattr" in ds["nested"].attrs
        assert ds["nested"].attrs["newattr"] == 432

        assert "rootattr" in ds.attrs
        assert ds.attrs["rootattr"] == 123  # get old value
        ds.attrs["rootattr"] = 234  # change
        assert ds.attrs["rootattr"] == 234  # get new value

        assert "myattr" in ds["/nested"].attrs
        del ds["/nested"].attrs["myattr"]  # remove
        assert "myattr" not in ds["/nested"].attrs
        with pytest.raises(KeyError):
            del ds["/nested"].attrs["myattr"]  # remove again (should fail)

        # try setting/changing attribute of non-existing
        with pytest.raises(KeyError):
            ds["/non-existing"].attrs["someattr"] = "something"

        # similarly for datasets and groups - create, substitute and remove
        # TODO

        ds.commit()
        assert len(ds.containers) == 2

        # check again after it was reopened as read-only:
        assert set(ds.keys()) == set(["number", "bool", "nested"])
        assert set(ds["/nested"].keys()) == set(["bool", "group"])
        assert set(ds["/nested/group"].keys()) == set(["string"])
        assert ds.attrs["rootattr"] == 234  # get new value
        assert ds["number"].attrs["attribute"] == "value"
        assert ds["nested"].attrs["newattr"] == 432
        assert ds["/number"][()] == 42
        assert ds["/bool"][()] == False
        assert ds["/nested/bool"][()] == True
        assert ds["/nested/group/string"][()] == b"hello world"

        # just check for correct gpath (the / and combining paths should work right)
        assert ds["/nested/group"]._gpath == "/nested/group"
        assert ds["nested"]["group"]._gpath == "/nested/group"
        assert ds["nested/group"]["/nested/bool"]._gpath == "/nested/bool"


# TODO: replace dataset with directory
# replace directory with dataset
# add new ones, delete old ones, substitute with same kind of thing
# replace root


# container + patch 1 layout:
# ---------------
# /@rootattr = 234
# /number = 42
# /number@attribute = "value"
# /bool = False
# /nested@newattr = 432
# /nested/bool = True
# /nested/group/string = "hello"
