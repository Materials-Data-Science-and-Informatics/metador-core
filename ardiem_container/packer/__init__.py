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

        More specifically, it MUST cover all metadata that is to be provided directly by
        the user (i.e. is not inferred or extracted from generated data) for the purpose
        of packing and SHOULD try to cover as many problems with data and metadata as
        possible to avoid failure during the actual packing process.

        Files or directories inside of `data_dir` MUST NOT be created, deleted or modified
        by this method.

        Args:
            data_dir: Directory containing all the data to be packed.

        Returns:
            Empty dict if there are no problems and the directory looks like it
            can be packed, assuming it stays in the state it is currently in.

            Otherwise, returns a dict mapping file paths (relative to `dir`)
            to lists of detected errors.

            The errors must be either a string (containing a human-readable summary of all
            problems with that file), or another dict with more granular error messages,
            in case that the file is e.g. a JSON-compatible file subject to validation
            with JSON Schemas.
        """

    @staticmethod
    @abstractmethod
    def check_dataset(dataset: IH5Dataset) -> ValidationErrors:
        """Check whether a dataset is compatible with and valid according to this packer.

        This method MUST succeed on a dataset that was created or updated using
        `pack_directory` and it will be used to verify the internal container
        structure to check whether a possibly unknown dataset can be updated using
        this packer before creating a patch for the dataset.

        Args:
            dataset: The dataset to be verified.

        Returns:
            Empty dict if there are no problems detected in the container.

            Otherwise, a dict mapping dataset paths to errors.

            The errors must be either a string (containing a human-readable summary of all
            problems with that file), or another dict with more granular error messages,
            e.g. in case that the file is a JSON-compatible file subject to validation
            with JSON Schemas.
        """

    @staticmethod
    @abstractmethod
    def pack_directory(
        data_dir: Path, dataset: IH5Dataset, fresh: bool, diff: Optional[DirDiff]
    ):
        """Pack a directory into an Ardiem IH5 dataset or update it.

        The `data_dir` is assumed to be suitable (according to `check_directory`).

        The `dataset` is assumed to be already in writable mode.

        The flag `fresh` indicates whether this is a new dataset.

        The structure `diff` contains information about changed paths.

        If `fresh=True`, the `diff` tree will have all files and directories
        inside `data_dir` listed as 'added' entities and `dataset` will be empty.
        Otherwise, `dataset` will be valid for this packer according to `check_dataset`
        and `diff` will have a non-trivial structure (in case of changes in `data_dir`).

        Files or directories inside of `data_dir` MUST NOT be created, deleted or modified
        by this method.

        The `dataset.commit()` MUST NOT be called by the packer, as the caller of this
        method might perform additional postprocessing after the packing.

        `IH5Values` that are already pre-existing in the passed dataset
        MUST NOT be read or be relied on for generating a patch,
        as they could be dummy stubs. One MAY rely on existence or absence of
        `IH5Group`s, `IH5Value`s and attributes in the container.

        The packer MUST be able to perform the packing both in case that `dataset` is
        fresh and if it is a pre-existing dataset packed by this method earlier.

        If `fresh=True` the packer MUST do a full packing of `data_dir` into `dataset`.

        If `fresh=False`, the packer SHOULD modify only structures in the dataset that are
        affected by the added/changed/removed files and directories according to `diff`.

        The behaviour in both cases (fresh complete packing or update patch) MUST lead to
        the same observable result.
        In other words, patching an existing dataset MUST lead to the same overlay view as
        creating the dataset from scratch, so the packer is responsible for using the
        `diff` tree for correctly cleaning up and adapting the existing dataset into a
        form that is observationally equivalent to a freshly packed dataset. **In case of
        doubt, ensuring this rule is more important than "small" patches.**

        In case that packing must be aborted, and exception MUST be raised and contain
        an error dict like in the other methods above helping to find and fix the problem.

        Args:
            data_dir: Directory containing all the data to be packed
            dataset: Ardiem IH5 dataset to pack the data into or update
            fresh: `True` if this is to be treated like a new dataset
            diff: Diff tree of dirs and files in data_dir compared to a previous state
        """
