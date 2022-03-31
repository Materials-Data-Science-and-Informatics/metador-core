from abc import ABC, abstractmethod
from pathlib import Path
from typing import List

from ..hashutils import DiffObjType  # noqa: F401
from ..hashutils import PathStatus  # noqa: F401
from ..hashutils import DirDiff, ValidationErrors
from ..ih5.record import IH5Record


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

    @classmethod
    @abstractmethod
    def packer_id(cls) -> str:
        """Return the unique packer ID string.

        This MUST be equal to the declared entry-point under which this class is
        registered in the Python package it is contained in.
        """

    @classmethod
    @abstractmethod
    def packer_version(cls) -> List[int]:
        """Return the current version the packer.

        The returned list corresponds to a version string, i.e. [1,2,4] means "1.2.4".

        A packer MUST be able to update records with the same MAJOR version
        and the current or earlier MINOR versions.

        More formally, the version list [MAJOR, MINOR, REVISION]
        MUST adhere to the following contract:

        1. increasing MAJOR means a break in backwards-compatibility
        for older datasets (i.e. new packer cannot work with old records),

        2. increasing MINOR means a break in forward-compatibility
        for newer datasets (i.e. older packers will not work with newer records),

        3. increasing REVISION does not affect compatibility
        for datasets with the same MAJOR and MINOR version.

        When this packer is updated, the version MUST increase in a suitable way.

        This means, the REVISION version should increase for e.g. bugfixes that do
        not change the structure or metadata stored in the dataset,
        MINOR should increase whenever from now on older versions would not be able
        to produce a valid update for a dataset created with this version,
        but upgrading old datasets with this version still works.
        Finally, MAJOR version is increased when all compatibility guarantees are off.
        """

    @classmethod
    @abstractmethod
    def check_directory(cls, data_dir: Path) -> ValidationErrors:
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

    @classmethod
    @abstractmethod
    def check_record(cls, record: IH5Record) -> ValidationErrors:
        """Check whether a record is compatible with and valid according to this packer.

        This method MUST succeed on a record that was created or updated using
        `pack_directory` and it will be used to verify the internal container
        structure to check whether a possibly unknown record can be updated using
        this packer before creating a patch for the record.

        Args:
            record: The record to be verified.

        Returns:
            Empty dict if there are no problems detected in the container.

            Otherwise, a dict mapping record paths to errors.

            The errors must be either a string (containing a human-readable summary of all
            problems with that file), or another dict with more granular error messages,
            e.g. in case that the file is a JSON-compatible file subject to validation
            with JSON Schemas.
        """

    @classmethod
    @abstractmethod
    def pack_directory(
        cls, data_dir: Path, diff: DirDiff, record: IH5Record, fresh: bool
    ):
        """Pack a directory into an Ardiem IH5 record or update it.

        The `data_dir` is assumed to be suitable (according to `check_directory`).

        The structure `diff` contains information about changed paths.

        The `record` is assumed to be already in writable mode.

        The flag `fresh` indicates whether this is a new record.

        If `fresh=True`, the `diff` tree will have all files and directories
        inside `data_dir` listed as 'added' and `record` will be empty.
        Otherwise, `record` will be valid for this packer according to `check_record`
        and `diff` will have a non-trivial structure (in case of changes in `data_dir`).

        Requirements for well-behaved packers:

        1. Directory invariance:
        Files or directories inside of `data_dir` MUST NOT be created, deleted or
        modified by this method.

        2. No manual patch management:
        The `record.commit()` MUST NOT be called by the packer, as the caller of this
        method might perform additional postprocessing after the packing.
        Similarly, `record.create_patch()` or `record.discard_patch()` must not be
        used. The packer gets a writable record and is only responsible for perform
        the neccessary additions, deletions and modifications.

        3. Data-independence of updates:
        `IH5Record`s that are already pre-existing in the passed record
        MUST NOT be read or be relied on for generating a patch,
        as they could be dummy stubs. One MAY rely on existence or absence of
        `IH5Group`s, `IH5Records`s and attributes in the container.

        4. Fresh and update packing:
        If `fresh=True` the packer MUST do a full packing of `data_dir` into `record`.
        If `fresh=False`, the packer SHOULD modify only structures in the record that are
        affected by the added/changed/removed files and directories according to `diff`.
        The packer MUST be able to produce a correct update on top of a record that is
        suitable according to `check_record`.

        5. Semantic correctness:
        The behaviour in both cases (fresh record or update patch onto a previous
        version) MUST lead to the same observable result, unless documented otherwise.

        In other words, it is expected from packers that patching an existing record
        MUST lead to the same overlay view as creating the record from scratch, so the
        packer is responsible for using the `diff` tree for correctly cleaning up and
        adapting the existing record into a form that is observationally equivalent to a
        freshly packed record.

        Deviations from this rule are possible, but MUST be documented for the users of
        the packer. For example, the packer documentation may clarify that when updating,
        it will only take care of certain files and ignore other changes. If this
        restriction is made known to users, then it is acceptable.

        So if a implementing a shallow update in full generality is not feasible,
        there are two possibilities:

        1. The packer can document what entities can be updated and which can not.
            Then the updates are only semantically correct for the documented
            updatable entities, but will be much smaller in size than a full packing.

        2. The packer can clear the record and proceed in the same way
            as in the case it was fresh. While semantically correct, this negates
            the advantage of having patchable records in the first place, so the
            previous option is preferrable in practice.

        6. Exceptional termination:
        In case that packing must be aborted, and exception MUST be raised and contain
        an error dict like in the other methods above helping to find and fix the problem.

        Args:
            data_dir: Directory containing all the data to be packed
            record: Ardiem IH5 record to pack the data into or update
            fresh: `True` if this is to be treated like a new record
            diff: Diff tree of dirs and files in data_dir compared to a previous state
        """
