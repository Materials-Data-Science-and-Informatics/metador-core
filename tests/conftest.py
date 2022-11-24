import secrets
import shutil
from pathlib import Path
from typing import Any, Dict

import pytest


@pytest.fixture
def plugingroups_test():
    """Access to plugingroups in a test, but will reset afterwards."""
    from metador_core.plugins import plugingroups

    yield plugingroups

    plugingroups.__reset__()


# ----


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
        path = Path(ds_dir / name)
        path.mkdir()
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


class SymLink(str):
    pass


class UtilFuncs:
    """Helpers used in tests."""

    @staticmethod
    def random_hex(length: int) -> str:
        """Return hex string of given length."""
        return secrets.token_hex(int(length / 2))

    # data directory contents
    data_dir = {
        "tmp1": {
            "a": {
                "b": {
                    "c.csv": """time,position
0,1
1,2.71
2,3.14
""",
                    "c.csv_meta.yaml": """type: table
title: Movement
columns:
  - title: time
    unit: second
  - title: position
    unit: meter
""",
                    "d": SymLink("../../d"),
                }
            },
            "d": SymLink("a/b"),
            "e": "will be replaced",
            "f": "will be modified",
            "_meta.yaml": "author: unchanged",
            "z": "",
        },
        "tmp2": {
            "a": {"b": "hello, world!"},
            "e": {"g": "is added"},
            "f": "is modified",
            "_meta.yaml": "author: unchanged",
            "z": "",
        },
    }

    @classmethod
    def prepare_dir(cls, dir: Path, data: Dict[str, Any]):
        """Given an existing empty directory and a data dict, create structure.

        Will create nested subdirectories, files and symlinks as specified.
        """
        for k, v in data.items():
            path = dir / k
            if isinstance(v, dict):
                path.mkdir()
                cls.prepare_dir(path, v)
            elif isinstance(v, SymLink):
                path.symlink_to(v)
            else:
                with open(path, "wb") as f:
                    if isinstance(v, str):
                        v = v.encode("utf-8")
                    f.write(v)


@pytest.fixture(scope="session")
def testutils():
    """Fixture giving access to helper functions anywhere in test suite."""
    return UtilFuncs
