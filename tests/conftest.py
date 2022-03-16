import secrets
from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def ds_dir(tmpdir_factory):
    """Create a fresh temporary directory for datasets created in the tests."""
    return tmpdir_factory.mktemp("test_datasets")


@pytest.fixture
def tmp_ds_path_factory(ds_dir):
    """Return a dataset name generator to be used for creating datasets.

    All containers of all datasets will be cleaned up after completing the test.
    """
    names = []

    def fresh_name():
        name = secrets.token_hex(4)
        names.append(name)
        return Path(ds_dir / name)

    yield fresh_name

    # clean up
    for name in names:
        for file in Path(ds_dir).glob(f"{name}*"):
            file.unlink()


@pytest.fixture
def tmp_ds_path(tmp_ds_path_factory):
    """Generate a dataset name to be used for creating datasets.

    All containers will be cleaned up after completing the test.
    """
    return tmp_ds_path_factory()
