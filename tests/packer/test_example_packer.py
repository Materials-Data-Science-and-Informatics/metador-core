"""Example GenericPacker tests."""

import h5py
import pytest

from metador_core.packer.utils import DirValidationErrors
from metador_core.plugins import packers

pytest.skip(reason="FIXME when API complete", allow_module_level=True)


def test_example_packer(tmp_ds_path, testinputs):
    ds1 = testinputs("dirdiff1")
    ds2 = testinputs("dirdiff1")

    generic = packers["generic"]

    # prepare directory and check that it is packer-compatible (just to make sure)
    assert not generic.check_dir(ds1)

    # rename a file to make it fail
    (ds1 / "_meta.yaml").rename(ds1 / "renamed")
    errs = generic.check_dir(ds1)
    assert errs
    print(errs)

    with pytest.raises(DirValidationErrors):
        packers.pack("generic", ds1, tmp_ds_path, h5py.File)

    # fix error with directory
    (ds1 / "renamed").rename(ds1 / "_meta.yaml")

    # do some "changes"
    with open(ds2 / "example_meta.yaml", "w") as f:
        f.write("author: changed")

    # now update
    packers.update("generic", ds2, tmp_ds_path, h5py.File)
