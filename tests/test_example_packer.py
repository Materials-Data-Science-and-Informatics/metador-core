"""Example GenericPacker tests."""

import h5py
import pytest

from metador_core.packer import PGPacker
from metador_core.packer.util import DirValidationErrors
from metador_core.plugins import installed

pytest.skip(reason="FIXME when API complete", allow_module_level=True)


def test_example_packer(tmp_path_factory, tmp_ds_path, testutils):
    tmp1 = tmp_path_factory.mktemp("tmp1")
    tmp2 = tmp_path_factory.mktemp("tmp2")

    PACKERS = installed.group("packer", PGPacker)
    generic = PACKERS["generic"]

    # prepare directory and check that it is packer-compatible (just to make sure)
    testutils.prepare_dir(tmp1, testutils.data_dir["tmp1"])
    assert not generic.check_dir(tmp1)

    # rename a file to make it fail
    (tmp1 / "_meta.yaml").rename(tmp1 / "renamed")
    errs = generic.check_dir(tmp1)
    assert errs
    print(errs)

    with pytest.raises(DirValidationErrors):
        PACKERS.pack("generic", tmp1, tmp_ds_path, h5py.File)

    # fix error with directory
    (tmp1 / "renamed").rename(tmp1 / "_meta.yaml")

    # do some "changes"
    testutils.prepare_dir(tmp2, testutils.data_dir["tmp2"])
    with open(tmp2 / "example_meta.yaml", "w") as f:
        f.write("author: changed")

    # now update
    PACKERS.update("generic", tmp2, tmp_ds_path, h5py.File)
