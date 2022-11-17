from enum import Enum, auto
from typing import Any, Optional, Type, Union, cast

import h5py

from ..ih5.container import IH5Record
from .types import H5FileLike, OpenMode


class MetadorDriverEnum(Enum):
    HDF5 = auto()
    IH5 = auto()


METADOR_DRIVERS = {MetadorDriverEnum.HDF5: h5py.File, MetadorDriverEnum.IH5: IH5Record}
METADOR_DRIVER_CLASSES = tuple(METADOR_DRIVERS.values())
MetadorDriver = Union[h5py.File, IH5Record]


def get_driver_type(raw_cont: MetadorDriver) -> MetadorDriverEnum:
    for val, cls in METADOR_DRIVERS.items():
        if isinstance(raw_cont, cls):
            return val
    raise RuntimeError(f"Object not of known container driver type: {raw_cont}")


def get_source(raw_cont: MetadorDriver, driver: MetadorDriverEnum) -> Any:
    c = cast(Any, raw_cont)
    if driver == MetadorDriverEnum.HDF5:
        return c.filename
    elif driver == MetadorDriverEnum.IH5:
        return c.ih5_files
    raise RuntimeError(f"Object not of known container driver type: {raw_cont}")


def to_h5filelike(
    name_or_obj: Union[MetadorDriver, Any],
    mode: OpenMode = "r",
    *,
    # NOTE: driver takes class instead of enum to also allow subclasses
    driver: Optional[Type[MetadorDriver]] = None,
) -> H5FileLike:
    if isinstance(name_or_obj, METADOR_DRIVER_CLASSES):
        # user h5file-like object already
        return cast(H5FileLike, name_or_obj)
    else:
        # user passed arguments to instantiate a container object
        driver = driver or h5py.File  # default: use h5py.File
        if not issubclass(driver, METADOR_DRIVER_CLASSES):
            msg = f"Passed driver class not supported: {driver}"
            raise ValueError(msg)
        return cast(H5FileLike, driver(cast(Any, name_or_obj), mode))
