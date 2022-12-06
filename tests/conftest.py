import secrets
import shutil
from pathlib import Path

import pytest


@pytest.fixture(scope="module")
def plugingroups_test():
    """Access to plugingroups in a test, but will reset afterwards."""
    from metador_core.plugins import plugingroups

    yield plugingroups

    plugingroups.__reset__()


# ----


@pytest.fixture(scope="session")
def ds_dir(tmpdir_factory):
    """Create a fresh temporary directory for records created in the tests."""
    return tmpdir_factory.mktemp("metador_tests")


@pytest.fixture
def tmp_ds_path_factory(ds_dir):
    """Return a record name generator to be used for creating records.

    All containers of all records will be cleaned up after completing the test.
    """
    names = []

    def fresh_name() -> Path:
        name = secrets.token_hex(4)
        names.append(name)
        path = Path(ds_dir / name)
        return path

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


TEST_DATA_DIR = Path(__file__).resolve().parent / "data"
"""Location of the test input data."""


@pytest.fixture
def testinputs(tmp_ds_path_factory):
    """Create temporary file or directory based on a known test input."""

    def copy_from(name):
        dst = tmp_ds_path_factory()
        assert not dst.exists()

        src = TEST_DATA_DIR / name
        print(src)
        if src.is_file():
            shutil.copy2(src, dst)
        elif src.is_dir():
            shutil.copytree(src, dst, symlinks=True)
        else:
            raise RuntimeError(f"No such test input: {name}")

        assert dst.exists()
        return dst

    return copy_from
