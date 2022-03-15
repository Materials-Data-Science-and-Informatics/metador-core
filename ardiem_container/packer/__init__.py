from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict

from ..dataset import IH5Dataset


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
    def check_directory(data_dir: Path) -> Dict[str, str]:
        """Check whether the given directory is suitable for packing with this plugin.

        This method will be called before `pack_directory`.

        Args:
            data_dir: Directory containing all the data to be packed

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
    def pack_directory(data_dir: Path, dataset: IH5Dataset, update: bool):
        """Pack a directory into an Ardiem IH5 dataset or update it.

        It is assumed that `data_dir` is suitable (according to `check_directory`).
        The container file will be overwritten, if it already exists.

        No files in `dir` will be created or modified.

        Args:
            data_dir: Directory containing all the data to be packed
            container: IH5 dataset to pack the data into
            update: if true, this is a non-empty dataset to be updated
        """

    # TODO: what if packing fails? maybe need ArdiemPackerException
    # and must ensure clean-up of partial container file

    # TODO: maybe useful to provide a "build directory" for caching something
    # to speedup container creation. Where does it live? how is it managed?
    # For now, we'll support no caching for simplicity's sake.

    @staticmethod
    @abstractmethod
    def check_container(dataset: IH5Dataset) -> Dict[str, str]:
        """Check a dataset assembled by this plugin whether it is in order.

        This clearly must be the case immediately after `pack_directory` and
        can be used to verify the internal container structure in case that
        e.g. the user manually modified it.

        Args:
            dataset: The dataset to be verified

        Returns:
            {} if there are no problems with the container.

            Otherwise, a dict mapping dataset paths to error messages.

            The error message must be either a string (containing a
            human-readable summary of all problems with that file), or another
            dict with more granular error messages, e.g. in case that the file is a
            JSON-compatible file subject to validation with JSON Schemas.
        """
