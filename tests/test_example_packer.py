"""ExamplePacker tests."""

import pytest

from metador_core.container import MetadorContainer
from metador_core.packer.util import DirValidationError
from metador_core.plugins import installed


@pytest.mark.skip(reason="FIXME")
def test_example_packer_create(tmp_path_factory, tmp_ds_path, testutils):
    tmp1 = tmp_path_factory.mktemp("tmp1")
    tmp2 = tmp_path_factory.mktemp("tmp2")

    # get the packer from registered entry-point
    epacker = installed["packer"]["example"]

    # prepare directory and check that it is packer-compatible (just to make sure)
    testutils.prepare_dir(tmp1, testutils.data_dir["tmp1"])
    assert not epacker.check_directory(tmp1)

    # rename a file to make it fail
    (tmp1 / "example_meta.yaml").rename(tmp1 / "renamed")
    errs = epacker.check_directory(tmp1)
    assert errs
    print(errs)

    with pytest.raises(DirValidationError):
        MetadorContainer.create(tmp_ds_path, tmp1, epacker)

    # fix error with directory
    (tmp1 / "renamed").rename(tmp1 / "example_meta.yaml")

    with MetadorContainer.create(tmp_ds_path, tmp1, epacker) as ds:
        # the freshly produced record should have no issues according to packer
        assert not ds.check_record(epacker)

        # try making the record invalid so check_record fails
        ds.record.create_patch()

        del ds.record["head/packer"]
        assert ds.check_record(epacker)

        ds.record.discard_patch()

        # do some "changes"
        testutils.prepare_dir(tmp2, testutils.data_dir["tmp2"])
        with open(tmp2 / "example_meta.yaml", "w") as f:
            f.write("author: changed")

        # now update
        ds.update(tmp2, epacker)
