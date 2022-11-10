"""Abstract Metador container provider interface."""
from typing import Any, Dict, Generic, Optional, Protocol, Tuple, Type, TypeVar

from .wrappers import MetadorContainer, MetadorDriver

T = TypeVar("T")


class ContainerProxy(Protocol[T]):
    """Abstract interface for Metador container providers.

    This interface acts like a proxy to access containers by some identifier.

    The identifier type parameter T is in the simplest case the Metador
    container UUID. In more complex cases, it could be a different unique
    identifier with a non-trivial relationship to Metador container UUIDs
    (many-to-many). Therefore, T is implementation-specific.

    There are many ways to store and organize containers, this interface serves
    as the implementation target for generic service components such as
    container-centric Flask blueprints, so they can be easier reused in
    different backends and services.

    Note that only containment and retrieval are possible - on purpose.
    Knowing and iterating over all containers in a system is not always possible.
    """

    def get(self, key: T) -> Optional[MetadorContainer]:
        ...

    def __contains__(self, key: T) -> bool:
        return self.get(key) is None

    def __getitem__(self, key: T) -> T:
        if ret := self.get(key):
            return ret
        raise KeyError(key)


ContainerArgs = Tuple[Type[MetadorDriver], Any]
"""Pair of (driver class, suitable driver arguments).

Must be such that `MetadorContainer(driver(source))` yields a container.
"""


class SimpleContainerProvider(Generic[T], ContainerProxy[T]):
    """Dict-backed container proxy.

    It is a minimal reasonable implementation for the interface that can be
    used in small apps and does not depend on the container driver,
    thus can support all container interface implementations.
    """

    _known: Dict[T, ContainerArgs]
    """Mapping from container identifier to MetadorContainer constructor args."""

    def __init__(self):
        self._known = {}

    def get(self, key: T) -> Optional[MetadorContainer]:
        """Get an open container file to access data and metadata, if it exists."""
        if key not in self._known:
            return None
        driver, source = self._known[key]
        return MetadorContainer(driver(source))

    # ----

    def __delitem__(self, key: T):
        del self._known[key]

    def __setitem__(self, key: T, value: ContainerArgs):
        self._known[key] = value

    def keys(self):
        return self._known.keys()
