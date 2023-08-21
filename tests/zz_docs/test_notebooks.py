import os
from pathlib import Path

import pytest
from testbook import testbook


@pytest.fixture(scope="function")
def nb_cwd():
    """Fixture to change CWD to location of notebooks."""
    old = os.getcwd()
    nb_dir = Path("./docs/notebooks")
    assert nb_dir.is_dir()
    os.chdir(nb_dir)
    yield None
    os.chdir(old)


def test_notebooks_execute(nb_cwd):
    """Test that all tutorial notebooks run through without exceptions."""
    for nb_file in Path(".").glob("*.ipynb"):
        with testbook(nb_file, execute=True):
            # just check it runs fine
            print(f"Notebook {nb_file} run complete.")
