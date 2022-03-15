import secrets
from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def ds_dir(tmpdir_factory):
    """Create a fresh temporary directory for datasets created in the tests."""
    return tmpdir_factory.mktemp("test_datasets")


@pytest.fixture
def tmp_ds_path(ds_dir):
    """Generate a dataset name to be used for creating datasets.

    All containers will be cleaned up after completing the test.
    """
    name = secrets.token_hex(4)
    yield Path(ds_dir / name)
    for file in Path(ds_dir).glob("name*"):
        file.unlink()
