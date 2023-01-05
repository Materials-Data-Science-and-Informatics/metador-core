import pytest

from metador_core.container import MetadorContainer
from metador_core.container.provider import SimpleContainerProvider


def test_simple_container_provider(tmp_ds_path):
    scp = SimpleContainerProvider()

    assert scp.get("test") is None
    assert "test" not in scp
    with pytest.raises(KeyError):
        scp["test"]

    tmp_ds_path.mkdir()
    filepath = tmp_ds_path / "container.h5"
    with MetadorContainer(filepath, "w") as m:
        m["hello"] = b"world"

        # add to provider
        scp["test"] = m

    # access
    assert "test" in scp
    assert "test" in scp.keys()
    with scp["test"] as m:
        assert m["hello"][()] == b"world"

    # remove
    del scp["test"]
    assert "test" not in scp
