import pytest

from metador_core.container import MetadorContainer
from metador_core.container.drivers import METADOR_DRIVERS
from metador_core.container.utils import METADOR_VERSION_PATH


@pytest.mark.parametrize("drv", iter(METADOR_DRIVERS.items()))
def test_container_init_fail(tmp_ds_path, drv):
    _, drv_cls = drv

    with drv_cls(tmp_ds_path, "w") as r:
        # create empty container file and close it
        ...

    with pytest.raises(ValueError):
        # read-only and non-metador -> can't init
        with drv_cls(tmp_ds_path, "r") as r:
            MetadorContainer(r)

    with MetadorContainer(drv_cls(tmp_ds_path, "w")) as m:
        # init container, once writable, once only readable
        assert m.metador.container_uuid
        assert m.metador.spec_version[0] == 1

    with drv_cls(tmp_ds_path, "w") as r:
        # mark with invalid version
        r[METADOR_VERSION_PATH] = "2.0"

    with pytest.raises(ValueError):
        # invalid spec version is assigned
        MetadorContainer(drv_cls(tmp_ds_path, "r"))


@pytest.mark.parametrize("drv", iter(METADOR_DRIVERS.items()))
def test_container_init(tmp_ds_path, drv):
    drv_type, drv_cls = drv

    with MetadorContainer(tmp_ds_path, "w", driver=drv_cls) as m:
        c_uuid = m.metador.container_uuid
        c_src = m.metador.source

        # as expected
        assert m["/"]._self_container is m
        assert m.metador.spec_version[0] == 1
        assert m.metador.driver == drv_cls
        assert m.metador.driver_type == drv_type
        assert m.mode == "r+"

    # can open based on retrieved "data source object"
    with MetadorContainer(c_src, "r", driver=drv_cls) as m:
        assert m.metador.container_uuid == c_uuid  # UUID did not change
        assert m.mode == "r"

    # can also just wrap a raw container that is already opened
    with drv_cls(c_src, "r") as r:
        assert MetadorContainer(r).__wrapped__ is r  # unpacked container object
