from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional

from ..ih5.dataset import IH5Dataset
from ..util import DirDiff, ValidationErrors


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
    def check_directory(data_dir: Path) -> ValidationErrors:
        """Check whether the given directory is suitable for packing with this plugin.

        This method will be called before `pack_directory` and MUST detect
        all problems (such as missing or invalid data or metadata) that can be
        expected to be fixed by the user in preparation for the packing.

        Args:
            data_dir: Directory containing all the data to be packed

        Returns:
            Empty dict if there are no problems and the directory can be packed.

            Otherwise, a dict mapping file paths (relative to `dir`) to error
            messages.

            The error message must be either a string (containing a
            human-readable summary of all problems with that file), or another
            dict with more granular error messages, in case that the file is a
            JSON-compatible file subject to validation with JSON Schemas.
        """

    @staticmethod
    @abstractmethod
    def pack_directory(data_dir: Path, dataset: IH5Dataset, diff: Optional[DirDiff]):
        """Pack a directory into an Ardiem IH5 dataset or update it.

        It is assumed that `data_dir` is suitable (according to `check_directory`).

        The passed in dataset is already in writable mode.

        Files in `data_dir` MUST NOT be created, deleted or modified.
        The `dataset.commit()` MUST NOT be called by the packer.

        IH5Values that are already pre-existing in the passed dataset
        MUST NOT be read or be relied on for generating a patch,
        as they could be dummy stubs.

        If the dataset to be created is fresh, `diff` will be `None`.
        Otherwise, it will contain information about changed paths
        and a patch MUST be generated creating only structures in the dataset
        that are relevant to the changed files and directories.

        In order to abort packing, and exception must be raised.

        Args:
            data_dir: Directory containing all the data to be packed
            container: Ardiem IH5 dataset to pack the data into or update
            diff: if true, this is a non-empty dataset to be updated
        """

    @staticmethod
    @abstractmethod
    def check_dataset(dataset: IH5Dataset) -> ValidationErrors:
        """Check a dataset assembled by this plugin whether it is in order.

        This method MUST succeed on a dataset that was created or updated using
        `pack_directory` and it will be used to verify the internal container
        structure to check whether a possibly unknown dataset is compatible
        with this packer, e.g. before creating a patch.

        Args:
            dataset: The dataset to be verified

        Returns:
            Empty dict if there are no problems with the container.

            Otherwise, a dict mapping dataset paths to error messages.

            The error message must be either a string (containing a
            human-readable summary of all problems with that file), or another
            dict with more granular error messages, e.g. in case that the file is a
            JSON-compatible file subject to validation with JSON Schemas.
        """
