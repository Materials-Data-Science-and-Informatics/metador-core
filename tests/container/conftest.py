import pytest

from metador_core.container import MetadorContainer
from metador_core.container.drivers import MetadorDriverEnum


@pytest.fixture(scope="function", params=list(iter(MetadorDriverEnum)))
def mc_driver(request):
    """Provide different metador container driver enum values."""
    return request.param


@pytest.fixture(scope="function")
def tmp_mc_path(tmp_ds_path):
    """Return a temporary path suitable for MetadorContainer creation."""
    tmp_ds_path.mkdir()
    return tmp_ds_path / "container"


@pytest.fixture
def fresh_raw(tmp_mc_path, mc_driver):
    # will not autoclose
    return mc_driver.value(tmp_mc_path, "w")


@pytest.fixture
def fresh_raw_managed(fresh_raw):
    # will auto-close
    with fresh_raw:
        yield fresh_raw


@pytest.fixture(scope="function")
def fresh_mc(fresh_raw):
    """Return fresh writable MetadorContainer instance.

    The dataset will be closed and destroyed afterwards.
    """
    with MetadorContainer(fresh_raw) as m:
        yield m
