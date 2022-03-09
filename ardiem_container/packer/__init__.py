from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict

import h5py

from ..container import ArdiemContainer


class ArdiemPacker(ABC):
    """Interface to be implemented by Ardiem packer plugins.

    These plugins is how support for wildly different domain-specific
    use-cases can be added to `ardiem-container` in a opt-in and
    loosely-coupled way.

    Users can install only the packer plugins they need for their use-cases,
    and such plugins can be easily developed independently from the rest
    of the Ardiem tooling, as long as this interface is honored.

    Carefully read the documentation for the required methods
    and implement them for your use-case in a subclass.
    See `ardiem_container.packer.example` for an example plugin.
    """

    @staticmethod
    @abstractmethod
    def check_directory(dir: Path) -> Dict[str, str]:
        """Check whether the given directory is suitable for packing with this plugin.

        This method will be called before `pack_directory`.

        Args:
            dir: Directory containing all the data to be packed

        Returns:
            {} if there are no problems and the directory can be packed.

            Otherwise, a dict mapping file paths (relative to `dir`) to error
            messages.

            The error message must be either a string (containing a
            human-readable summary of all problems with that file), or another
            dict with more granular error messages, in case that the file is a
            JSON-compatible file subject to validation with JSON Schemas.
        """

    @staticmethod
    @abstractmethod
    def pack_directory(dir: Path, container: h5py.File):
        """Pack a directory into an Ardiem HDF5 container.

        It is assumed that `dir` is suitable (according to `check_directory`).
        The container file will be overwritten, if it already exists.

        No files in `dir` will be created or modified.

        Args:
            dir: Directory containing all the data to be packed
            container: Filepath to be used for the resulting container
        """

    # TODO: what if packing fails? maybe need ArdiemPackerException
    # and must ensure clean-up of partial container file

    # TODO: maybe useful to provide a "build directory" for caching something
    # to speedup container creation. Where does it live? how is it managed?
    # For now, we'll support no caching for simplicity's sake.

    @staticmethod
    @abstractmethod
    def check_container(container: ArdiemContainer) -> Dict[str, str]:
        """Check a container assembled by this plugin whether it is in order.

        This clearly must be the case immediately after `pack_directory` and
        can be used to verify the internal container structure in case that
        e.g. the user manually modified it.

        Args:
            container: Filepath to the existing packed container

        Returns:
            {} if there are no problems with the container.

            Otherwise, a dict mapping container group paths to error messages.

            The error message must be either a string (containing a
            human-readable summary of all problems with that file), or another
            dict with more granular error messages, in case that the file is a
            JSON-compatible file subject to validation with JSON Schemas.
        """
