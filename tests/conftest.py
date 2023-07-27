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


@pytest.fixture(scope="module")
def schemas():
    """Access available schemas in a test without needing to import.

    Could be more robust in case the plugin system breaks.
    """
    from metador_core.plugins import schemas

    return schemas


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


@pytest.fixture
def testinput_maker(tmp_ds_path_factory):
    def wrapped(root_dir: Path):
        def copy_from(name: str):
            dst_dir = tmp_ds_path_factory()
            dst_dir.mkdir()
            dst = dst_dir / name
            assert not dst.exists()

            src = root_dir / name
            if src.is_file():
                shutil.copy2(src, dst)
            elif src.is_dir():
                shutil.copytree(src, dst, symlinks=True)
            else:
                raise RuntimeError(f"No such test input in {root_dir}: {name}")

            assert dst.exists()
            return dst

        return copy_from

    return wrapped


TEST_DIR = Path(__file__).resolve().parent

TEST_DATA_DIR = TEST_DIR / "data"
"""Location of the test input data."""

TUTORIAL_FILES_DIR = TEST_DIR.parent / "docs" / "notebooks" / "files"
"""Location of the tutorial notebook input files."""


@pytest.fixture
def testinputs(testinput_maker):
    """Create temporary file or directory based on a known test input."""
    return testinput_maker(TEST_DATA_DIR)


@pytest.fixture
def tutorialfiles(testinput_maker):
    """Create temporary file or directory based on a known tutorial input."""
    return testinput_maker(TUTORIAL_FILES_DIR)
