"""Utilities to make widget serving work."""
from pathlib import Path
from typing import Dict, KeysView, List, Optional
from uuid import UUID

import h5py

from ...container.utils import METADOR_UUID_PATH
from ...ih5.container import IH5Record
from ..container import MetadorContainer


class ContainerIndex:
    """Management of containers that are known to the application."""

    _files: Dict[UUID, List[Path]]
    """Mapping from Metador Container name to collection of files it consists of."""

    _opened: Dict[UUID, MetadorContainer]
    """Mapping from container names to possibly already open container."""

    @staticmethod
    def metador_uuid(files: List[Path]) -> Optional[UUID]:
        """Try extracting first metador UUID from file list, return it on success."""
        for file in files:
            try:
                with h5py.File(file, "r") as f:
                    val = f.get(METADOR_UUID_PATH)
                    if isinstance(val, h5py.Dataset):
                        dat = val[()]
                        if isinstance(dat, bytes):
                            try:
                                return UUID(dat.decode("utf-8"))
                            except ValueError:
                                continue
            except (BlockingIOError, OSError):
                continue
        return None

    def __init__(self):
        self._files = {}
        self._opened = {}

    def keys(self) -> KeysView:
        return self._files.keys()

    def get(self, uuid: UUID) -> Optional[MetadorContainer]:
        """Get an open container file to access data and metadata, if it exists.

        Will reuse an instance if it exists or create a new one.
        """
        if uuid not in self._files:
            return None
        fs = self._files[uuid]

        if uuid not in self._opened:
            # first access -> open container
            if len(fs) > 1:
                obj = IH5Record._open(fs)
            else:
                obj = h5py.File(fs[0], "r")
            self._opened[uuid] = MetadorContainer(obj)

        return self._opened[uuid]

    def update(self, uuid: UUID, container_files: List[Path]):
        """(Un)register or update a metador container file list.

        If the list is empty, will remove the container from the cache.

        If the list is updated, will close + remove the open container (if it exists).
        That way on next access a fresh container object is returned.
        """
        if container_files:
            # new / updated set of files making up a container
            self._files[uuid] = container_files
        elif not container_files and uuid in self._files:
            # empty files list -> remove if it exists
            del self._files[uuid]

        if uuid in self._opened:
            # already existed, something changed... close existing instance
            self._opened[uuid].close()
            del self._opened[uuid]
