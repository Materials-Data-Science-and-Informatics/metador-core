"""
Protocol roughly formalizing the overlap of h5py.File and IH5Record API.

We build the MetadorContainer interface assuming only these methods.
"""
from __future__ import annotations

from typing import (
    Any,
    Callable,
    ItemsView,
    Iterable,
    KeysView,
    MutableMapping,
    Optional,
    Protocol,
    TypeVar,
    Union,
    ValuesView,
    runtime_checkable,
)

from typing_extensions import Literal

from ..ih5.record import OpenMode


@runtime_checkable
class H5NodeLike(Protocol):
    """HDF5 Files, Groups and Datasets are all Nodes."""

    @property
    def name(self) -> str:
        """Absolute path of the node."""

    @property
    def attrs(self) -> MutableMapping:
        """Attached HDF5 attributes."""

    @property
    def parent(self) -> H5GroupLike:
        """Parent group."""

    @property
    def file(self) -> H5FileLike:
        """Original file-like object this node belongs to."""


@runtime_checkable
class H5DatasetLike(H5NodeLike, Protocol):
    """Datasets provide numpy-style indexing into data.

    Metador containers use it for storing bytes,
    and for getting bytes out again using [()].
    """

    def __getitem__(self, key: Any) -> Any:
        ...

    def __setitem__(self, key: Any, value) -> None:
        ...

    # needed to distinguish from other types:
    @property
    def ndim(self) -> int:
        """Numpy-style dimensionality."""


CallbackResult = TypeVar("CallbackResult")
VisititemsCallback = Callable[[str, H5NodeLike], Optional[CallbackResult]]
VisitCallback = Callable[[str], Optional[CallbackResult]]


@runtime_checkable
class H5GroupLike(H5NodeLike, Protocol):

    # MutableMapping-like

    def __getitem__(self, name: str) -> Union[H5GroupLike, H5DatasetLike]:
        ...

    def __setitem__(self, name: str, obj) -> None:
        ...

    def __delitem__(self, name: str) -> None:
        ...

    def __iter__(self) -> Iterable[str]:
        ...

    def __len__(self) -> int:
        ...

    # Container-like

    def __contains__(self, name: str) -> bool:
        ...

    # dict-like extras

    def keys(self) -> KeysView[str]:
        ...

    def values(self) -> ValuesView[Union[H5GroupLike, H5DatasetLike]]:
        ...

    def items(self) -> ItemsView[str, Union[H5GroupLike, H5DatasetLike]]:
        ...

    def get(self, name: str, *args, **kwargs) -> Union[H5GroupLike, H5DatasetLike]:
        ...

    # h5py specific

    def visititems(self, func: VisititemsCallback) -> CallbackResult:
        # returns value returned from callback
        ...

    def visit(self, func: VisitCallback) -> CallbackResult:
        # returns value returned from callback
        ...

    def create_dataset(self, path, *args, **kwargs) -> H5DatasetLike:
        # returns original passed-in data
        ...

    def require_dataset(self, path, *args, **kwargs) -> H5DatasetLike:
        ...

    def create_group(self, path: str) -> H5GroupLike:
        # returns new group
        ...

    def require_group(self, path: str) -> H5GroupLike:
        ...

    def move(self, source: str, dest: str) -> None:
        ...

    def copy(self, source, dest, **kwargs) -> None:
        ...


@runtime_checkable
class H5FileLike(H5GroupLike, Protocol):
    """A HDF5 File acts like the root group and has some extra features."""

    @property
    def mode(self) -> Literal["r", "r+"]:
        """Return 'r' if container is immutable, otherwise 'r+'."""

    def close(self) -> None:
        ...

    # context manager (`with` notation)

    def __enter__(self) -> H5FileLike:
        ...

    def __exit__(self, ex_type, ex_value, ex_traceback) -> None:
        ...


__all__ = ["H5FileLike", "H5GroupLike", "H5DatasetLike", "H5NodeLike", "OpenMode"]
