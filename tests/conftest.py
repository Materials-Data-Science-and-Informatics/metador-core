import secrets
import shutil
from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def ds_dir(tmpdir_factory):
    """Create a fresh temporary directory for records created in the tests."""
    return tmpdir_factory.mktemp("test_records")


@pytest.fixture
def tmp_ds_path_factory(ds_dir):
    """Return a record name generator to be used for creating records.

    All containers of all records will be cleaned up after completing the test.
    """
    names = []

    def fresh_name():
        name = secrets.token_hex(4)
        names.append(name)
        return Path(ds_dir / name)

    yield fresh_name

    # clean up
    for name in names:
        for path in Path(ds_dir).glob(f"{name}*"):
            if path.is_file() or path.is_symlink():
                path.unlink()
            elif path.is_dir():
                shutil.rmtree(path)


@pytest.fixture
def tmp_ds_path(tmp_ds_path_factory):
    """Generate a record name to be used for creating records.

    All containers will be cleaned up after completing the test.
    """
    return tmp_ds_path_factory()
