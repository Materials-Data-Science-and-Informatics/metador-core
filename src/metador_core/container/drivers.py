"""Metador driver abstraction in order to enable different underlying implementations."""
from enum import Enum
from typing import Any, Optional, Type, Union, cast

import h5py

from ..ih5.container import IH5Record
from .protocols import H5FileLike, OpenMode


class MetadorDriverEnum(Enum):
    """Supported classes that work with MetadorContainer.

    Note that they must be unrelated (i.e. not subclasses of each other).
    """

    HDF5 = h5py.File
    IH5 = IH5Record

    @classmethod
    def to_dict(cls):
        return {x: x.value for x in iter(cls)}


# NOTE: must be duplicated or static checkers can't pick it up
MetadorDriver = Union[h5py.File, IH5Record]
"""Union of all supported classes (for static type check)."""

METADOR_DRIVERS = MetadorDriverEnum.to_dict()
"""Dict representation of MetadorDriverEnum."""

METADOR_DRIVER_CLASSES = tuple(METADOR_DRIVERS.values())
"""Tuple of all supported classes (for instance check)."""

# ----


def get_driver_type(raw_cont: MetadorDriver) -> MetadorDriverEnum:
    """Return the driver type of container (if it is a suitable (sub)class)."""
    for val, cls in METADOR_DRIVERS.items():
        if isinstance(raw_cont, cls):
            return val
    raise ValueError(f"Object not of known container driver type: {raw_cont}")


def get_source(raw_cont: MetadorDriver, driver: MetadorDriverEnum = None) -> Any:
    """Return an object (i.e. input resource(s)) needed to re-open the given container."""
    c = cast(Any, raw_cont)
    driver = driver or get_driver_type(raw_cont)
    if driver == MetadorDriverEnum.HDF5:
        return c.filename
    elif driver == MetadorDriverEnum.IH5:
        return c.ih5_files


def to_h5filelike(
    name_or_obj: Union[MetadorDriver, Any],
    mode: OpenMode = "r",
    *,
    # NOTE: driver takes actual class instead of enum, to also allow subclasses
    driver: Optional[Type[MetadorDriver]] = None,
) -> H5FileLike:
    """Given a container or a resource with a driver, try to return a H5FileLike.

    If first argument is instance of a known driver is returned unchanged.
    Otherwise, will try to open it using the driver (h5py.File by default).

    Returns a H5FileLike compatible with MetadorContainer, or raises ValueError.
    """
    if isinstance(name_or_obj, METADOR_DRIVER_CLASSES):
        # user has passed a h5file-like object already
        return cast(H5FileLike, name_or_obj)
    else:
        # user passed arguments to try instantiating a container object
        driver = driver or h5py.File
        if not issubclass(driver, METADOR_DRIVER_CLASSES):
            msg = f"Passed driver class not supported: {driver}"
            raise ValueError(msg)
        return cast(H5FileLike, driver(cast(Any, name_or_obj), mode))
