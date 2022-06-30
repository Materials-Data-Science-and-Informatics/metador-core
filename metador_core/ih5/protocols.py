"""
Protocol formalizing the overlapping API of h5py.File and the IH5Record.

Mostly defined for documentation purposes.
"""
from __future__ import annotations

from typing import (
    Optional,
    Any,
    Callable,
    Protocol,
    runtime_checkable,
    Iterable,
    MutableMapping,
    KeysView,
    ValuesView,
    ItemsView,
)
from typing_extensions import Literal

# NOTE: might also checkout zope.interfaces and see if it makes more sense


@runtime_checkable
class H5NodeLike(Protocol):
    @property
    def name(self) -> str:
        # absolute path of the node
        ...

    @property
    def attrs(self) -> MutableMapping:
        # attached HDF5 attributes
        ...

    @property
    def parent(self) -> H5GroupLike:
        # parent group
        ...

    @property
    def file(self) -> H5FileLike:
        # opened file this node belongs to
        ...


@runtime_checkable
class H5DatasetLike(H5NodeLike, Protocol):
    # thing providing indexing into data.
    # we'll mostly use it for getting bytes out using [()] to access it

    # numpy-style access
    def __getitem__(self, key):
        ...

    # numpy-style access
    def __setitem__(self, key, value):
        ...

    # check for this attribute to distinguish from other things matching this protocol

    @property
    def ndim(self) -> int:
        ...


VisititemsCallback = Callable[[str, H5NodeLike], Optional[Any]]
VisitCallback = Callable[[str], Optional[Any]]


@runtime_checkable
class H5GroupLike(H5NodeLike, Protocol):

    # MutableMapping-like

    def __getitem__(self, key: str) -> H5NodeLike:
        ...

    def __setitem__(self, key: str, value):
        ...

    def __delitem__(self, key: str):
        ...

    def __iter__(self) -> Iterable[str]:
        ...

    def __len__(self) -> int:
        ...

    # Container-like

    def __contains__(self, key: str) -> bool:
        ...

    # dict-like extras

    def keys(self) -> KeysView[str]:
        ...

    def values(self) -> ValuesView[H5NodeLike]:
        ...

    def items(self) -> ItemsView[str, H5NodeLike]:
        ...

    def get(self, key: str, *args, **kwargs) -> Any:
        ...

    # h5py specific

    def visititems(self, func: VisititemsCallback) -> Any:
        # returns value returned from callback
        ...

    def visit(self, func: VisitCallback) -> Any:
        # returns value returned from callback
        ...

    def create_dataset(self, path, *args, **kwargs) -> Any:
        # returns original passed-in data
        ...

    def create_group(self, path: str) -> H5GroupLike:
        # returns new group
        ...


@runtime_checkable
class H5FileLike(H5GroupLike, Protocol):
    # inherits interface because of pass-through to root group

    @property
    def mode(self) -> Literal["r", "r+"]:
        """Return 'r' if container is read-only, otherwise 'r+'."""
        ...

    # context manager

    def __enter__(self):
        ...

    def __exit__(self, ex_type, ex_value, ex_traceback):
        ...
