"""Abstract Metador container provider interface."""
from typing import Any, Dict, Generic, Optional, Protocol, Tuple, Type, TypeVar

from . import MetadorContainer
from .interface import MetadorDriver

T = TypeVar("T")
ContainerArgs = Tuple[Type[MetadorDriver], Any]


class MetadorContainerProvider(Protocol[T]):
    """Dict-like abstract interface for Metador container providers.

    This interface acts like a proxy to container access.
    It abstracts over different ways to store and organize containers and serves
    as the implementation target for generic service components such as
    container-centric Flask blueprints, so they can be easier reused in
    different backends and services.
    """

    def get(self, key: T) -> Optional[MetadorContainer]:
        ...

    def __contains__(self, key: T) -> bool:
        return self.get(key) is None

    def __getitem__(self, key: T) -> T:
        if ret := self.get(key):
            return ret
        raise KeyError(key)


class SimpleContainerProvider(Generic[T], MetadorContainerProvider[T]):
    """Dict-backed simple index for containers."""

    _known: Dict[T, ContainerArgs]
    """Mapping from Metador Container name to collection of files it consists of."""

    def __init__(self):
        self._known = {}

    def get(self, key: T) -> Optional[MetadorContainer]:
        """Get an open container file to access data and metadata, if it exists."""
        if key not in self._known:
            return None
        driver, source = self._known[key]
        return MetadorContainer(driver(source))

    def __delitem__(self, key: T):
        del self._known[key]

    def __setitem__(self, key: T, value: ContainerArgs):
        self._known[key] = value

    def keys(self):
        return self._known.keys()
