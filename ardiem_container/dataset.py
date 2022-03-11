"""
Ardiem dataset management.

An Ardiem dataset consists of a base container and a number of patch containers.
This allows a dataset to work in settings where files are immutable, but still
provide a structured way of updating data stored inside.

Both base containers and patches are HDF5 files that are linked together
by some special attributes in the container root.
`ArdiemDataset` is a class that wraps such a set of files. It features
* support for dataset creation and updating
* automatic handling of the patch mechanism (i.e., creating/finding corresponding files)
* transparent access to data in the dataset (possibly spanning multiple files)
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import List, Optional, Union
from uuid import UUID, uuid1

import h5py

from .overlay import ArdiemGroup
from .util import hashsum

# TODO: assemble() to merge containers into a new base dataset
# Question:
# should it be an independent new base dataset?
# simplest solution, but makes patches not compatible to both split and assembled
#
# alternatively, patches would not do integrity checking and trust container UUID
# then the assembled one could have the container id of the highest patch
# and the patch index could start higher than 0

# TODO: do we need write/delete protection for the base/patch special attributes
# with the UUIDs etc, also including prevention of manual setting of
# the substitution-marker attribute and deletion-marker dataset value?

# TODO: create patch in a temp file, move/rename to actual name after commit

# TODO: hashsum directory tree helper function (to help creating updates from directory)

# TODO: explain the "patching algebra" rules, pseudo Haskell?
# maybe use that formalization for property-based tests with hypothesis?


class ArdiemDataset:
    """
    Class representing a dataset, which consists of a collection of files.

    One file is a base container (with patch index 0 and no linked predecessor)
    the remaining files are a linear sequence of patch containers.

    Runtime invariants:
        * all files of an instance are open for reading (until `close()` is called)
        * all files in `_files` are in patch index order
        * at most one file is open in writable mode (if any, it is the last one)
        * a `writable` file is available after `create` or `create_patch` was called
          and until `commit` or `discard` was called, and at no other time
        * the `writable` file, if present, is either the base container
          or the most recent patch

    Only creation of and access to containers is supported.
    Renaming or deleting a container collection is not supported.
    For this, use `ArdiemDataset.find_containers` and apply standard tools.
    """

    # Characters that may appear in a dataset name.
    # (to be put into regex [..] symbol braces)
    ALLOWED_NAME_CHARS = r"A-Za-z0-9\-"

    # filenames for a dataset named NAME are of the shape:
    # NAME[<PATCH_INFIX>.*]?<FILE_EXT>
    # NOTE: the first symbol of these must be one NOT in ALLOWED_NAME_CHARS!
    # This constraint is needed for correctly filtering filenames
    PATCH_INFIX = ".p"
    FILE_EXT = ".rdm.h5"

    # the magic string we use to identify a valid container
    FORMAT_MAGIC_STR = "ardiem_v1"
    # algorithm to use and prepend to a hashsum
    HASH_ALG = "sha256"

    # attribute names as constants (to avoid typos)
    FORMAT = "format"
    DATASET_UUID = "dataset_uuid"
    CONTAINER_UUID = "container_uuid"
    PATCH_INDEX = "patch_index"
    PREV_PATCH = "prev_patch"
    PREV_HASHSUM = "prev_hashsum"

    @classmethod
    def _is_valid_dataset_name(cls, name: str) -> bool:
        """Return whether a dataset name is valid."""
        return re.match(f"^[{cls.ALLOWED_NAME_CHARS}]+$", name) is not None

    def _next_patch_filepath(self) -> Path:
        """Compute filepath for the next patch based on the previous one."""
        path = Path(self._files[0].filename).parent
        patch_index = self._patch_index(self._files[-1]) + 1
        res = f"{path}/{self.name}{self.PATCH_INFIX}{patch_index}{self.FILE_EXT}"
        return Path(res)

    @classmethod
    def _patch_index(cls, f: h5py.File) -> int:
        """Return patch index of a container file (or throw exception)."""
        try:
            return f.attrs[cls.PATCH_INDEX].item()  # type: ignore
        except KeyError:
            raise ValueError(f"{f.filename}: attribute missing: '{cls.PATCH_INDEX}'")

    @classmethod
    def _get_uuid(cls, f: h5py.File, atr: str) -> UUID:
        """Return a UUID root attr value in container file (or throw exception)."""
        try:
            return UUID(f.attrs.get(atr, ""))
        except ValueError:
            raise ValueError(f"{f.filename}: attribute missing or invalid: '{atr}'")

    @classmethod
    def _init_container(cls, path: Path, prev: Optional[h5py.File] = None) -> h5py.File:
        """Initialize a container file with the minimal required attributes.

        Takes actual file path and optionally an open predecessor container file object.
        Returns a writable container file object.
        Used to init both new datasets as well as patches.
        """
        container = h5py.File(path, "x")  # create if does not exist, fail if it does
        container.attrs[cls.FORMAT] = cls.FORMAT_MAGIC_STR
        container.attrs[cls.CONTAINER_UUID] = str(uuid1())
        if prev is None:
            container.attrs[cls.DATASET_UUID] = str(uuid1())
            container.attrs[cls.PATCH_INDEX] = 0
        else:
            container.attrs[cls.DATASET_UUID] = prev.attrs[cls.DATASET_UUID]
            container.attrs[cls.PATCH_INDEX] = cls._patch_index(prev) + 1
            container.attrs[cls.PREV_PATCH] = prev.attrs[cls.CONTAINER_UUID]
            chksum = hashsum(open(prev.filename, "rb"), cls.HASH_ALG)
            container.attrs[cls.PREV_HASHSUM] = f"{cls.HASH_ALG}:{chksum}"
        return container

    def _check_container(self, file: h5py.File, prev: Optional[h5py.File] = None):
        """Check given container file.

        If prev is given, assumes a patch container, otherwise base container.

        Assumption: `_files` are initialized and sorted in patch order.
        """
        # check magic format marker
        if file.attrs.get(self.FORMAT, None) != self.FORMAT_MAGIC_STR:
            msg = f"attr '{self.FORMAT}' missing or invalid!"
            raise ValueError(f"{file.filename}: {msg}")
        # check presence of container uuid (will throw on failure)
        self._get_uuid(file, self.CONTAINER_UUID)
        # check presence+validity of dataset uuid (should be the same for all)
        if self._get_uuid(file, self.DATASET_UUID) != self.uuid:
            msg = f"attr '{self.DATASET_UUID}' inconsistent! Mixed up datasets?"
            raise ValueError(f"{file.filename}: {msg}")

        # check patch chain structure
        if prev is None:
            if self._patch_index(file) != 0:
                msg = "base container must have index 0!"
                raise ValueError(f"{file.filename}: {msg}")
            if self.PREV_PATCH in file.attrs:
                msg = f"base container must not have attribute '{self.PREV_PATCH}'!"
                raise ValueError(f"{file.filename}: {msg}")
            if self.PREV_HASHSUM in file.attrs:
                msg = f"base container must not have attribute '{self.PREV_HASHSUM}'!"
                raise ValueError(f"{file.filename}: {msg}")
        else:
            if self._patch_index(file) != self._patch_index(prev) + 1:
                msg = "patch container must have incremented index from predecessor!"
                raise ValueError(f"{file.filename}: {msg}")
            if self.PREV_PATCH not in file.attrs:
                msg = f"Patch must have an attribute '{self.PREV_PATCH}'!"
                raise ValueError(f"{file.filename}: {msg}")
            if self.PREV_HASHSUM not in file.attrs:
                msg = f"Patch must have an attribute '{self.PREV_HASHSUM}'!"
                raise ValueError(f"{file.filename}: {msg}")

            # claimed predecessor uuid must match with the predecessor by index
            # (can compare as strings directly, as we checked those already)
            file_prevpatch = file.attrs[self.PREV_PATCH]
            prev_uuid = prev.attrs[self.CONTAINER_UUID]
            if file_prevpatch != prev_uuid:
                msg = f"Patch for {file_prevpatch} != previous container {prev_uuid}"
                raise ValueError(f"{file.filename}: {msg}")

            # container hash must match (i.e. predecessor integrity check)
            chksum = hashsum(open(prev.filename, "rb"), self.HASH_ALG)
            if file.attrs[self.PREV_HASHSUM] != f"{self.HASH_ALG}:{chksum}":
                msg = f"Patch not applicable as {prev.filename} was modified!"
                raise ValueError(f"{file.filename}: {msg}")

    def _root_group(self) -> ArdiemGroup:
        return ArdiemGroup(self._files)

    # ---- public attributes and interface ----

    @property
    def uuid(self) -> UUID:
        """Return the common dataset UUID of the set of containers."""
        return self._get_uuid(self._files[0], self.DATASET_UUID)

    @property
    def name(self) -> str:
        """Inferred name of dataset (i.e. common filename prefix of the containers)."""
        path = Path(self._files[0].filename)
        return path.name.split(self.FILE_EXT)[0].split(self.PATCH_INFIX)[0]

    @property
    def containers(self) -> List[Path]:
        """List of container filenames this dataset consists of."""
        return [Path(f.filename) for f in self._files]

    def __init__(self, paths: List[Path]):
        """Open a dataset consisting of a base container + possible set of patches.

        Expects a set of full file paths forming a valid dataset.
        Will throw an exception in case of a detected inconsistency.
        """
        if not paths:
            raise ValueError("Cannot open empty list of containers!")

        self._has_writable: bool = False
        self._files: List[h5py.File] = [h5py.File(path, "r") for path in paths]
        # sort files by patch index order (important!)
        # if something is wrong with the indices, this will throw an exception.
        self._files.sort(key=self._patch_index)

        # check containers
        self._check_container(self._files[0])  # check base
        for i in range(1, len(self._files)):  # check patches
            self._check_container(self._files[i], self._files[i - 1])

        # additional sanity check: container uuids must be all distinct
        cn_uuids = {self._get_uuid(f, self.CONTAINER_UUID) for f in self._files}
        if len(cn_uuids) != len(self._files):
            msg = f"some '{self.CONTAINER_UUID}' is not unique, bad file set!"
            raise ValueError(f"{msg}")

    @classmethod
    def create(cls, dataset: Union[Path, str]) -> ArdiemDataset:
        """Create a new dataset consisting of a base container.

        The base container is exposed as the `writable` container.
        """
        dataset = Path(dataset)  # in case it was a str
        if not cls._is_valid_dataset_name(dataset.name):
            raise ValueError(f"Invalid dataset name: '{dataset.name}'")

        path = Path(f"{dataset}{cls.FILE_EXT}")
        ret = ArdiemDataset.__new__(ArdiemDataset)
        ret._has_writable = True
        ret._files = [cls._init_container(path)]
        return ret

    @classmethod
    def find_containers(cls, dataset: Path) -> List[Path]:
        """Return container names that look like they belong to the same dataset.

        This operation is based on purely syntactic pattern matching on file names.
        Given a path `/foo/bar`, it will find all containers in directory
        `/foo` whose name starts with `bar` followed by the correct file extension(s),
        such as `/foo/bar.rdm.h5` and `/foo/bar.p01.rdm.h5`.
        """
        dataset = Path(dataset)  # in case it was a str
        if not cls._is_valid_dataset_name(dataset.name):
            raise ValueError(f"Invalid dataset name: '{dataset.name}'")

        dataset = Path(dataset)  # in case it was a str
        globstr = f"{dataset.name}*{cls.FILE_EXT}"  # rough wildcard pattern
        # filter out possible false positives (i.e. foobar* matching foo* as well)
        paths = []
        for p in dataset.parent.glob(globstr):
            if re.match(f"^{dataset.name}[^{cls.ALLOWED_NAME_CHARS}]", p.name):
                paths.append(p)
        return paths

    @classmethod
    def open(cls, dataset: Path) -> ArdiemDataset:
        """Open a dataset for read access.

        This method uses `find_containers` to infer the correct file set.
        """
        paths = cls.find_containers(dataset)
        if not paths:
            raise ValueError(f"No containers found for dataset: {dataset}")
        return cls(paths)

    def close(self) -> None:
        """Close all containers that belong to that dataset.

        After this, the object may not be used anymore.
        """
        for f in self._files:
            f.close()
        self._files.clear()
        self._has_writable = False

    def create_patch(self) -> None:
        """Create a new patch.

        The patch file will be the new `writable` container.
        """
        if self._has_writable:
            raise ValueError("There already exists a writable container, commit first!")

        path = self._next_patch_filepath()
        self._files.append(self._init_container(path, self._files[-1]))
        self._has_writable = True

    def discard_patch(self) -> None:
        """Discard the current writable patch container."""
        if not self._has_writable:
            raise ValueError("Dataset is read-only, nothing to discard!")
        cfile = self._files[-1]
        if self.PREV_PATCH not in cfile.attrs:
            raise ValueError("Cannot discard base container! Just delete the file!")
            # reason: the base container provides dataset_uuid,
            # destroying it makes this object inconsistent / breaks invariants

        self._files.pop()
        self._has_writable = False
        fn = cfile.filename
        cfile.close()
        Path(fn).unlink()

    def commit(self) -> None:
        """Complete the current writable container (base or patch) for the dataset.

        Will perform checks on the new container and throw an exception on failure.

        After this, continuing to edit the writable container is prohibited.
        Instead, it is added to the dataset as a read-only base container or patch.
        """
        if not self._has_writable:
            raise ValueError("Dataset is read-only, nothing to commit!")
        cfile = self._files[-1]

        # check the new container (works for base as well as patch)
        pidx = self._patch_index(cfile)
        prev = self._files[-2] if pidx > 0 else None
        self._check_container(cfile, prev)  # throws on failure

        # TODO: here we would plug in more checks for the
        # fine-grained format... delegate to subclass function?
        # this should use the overlay to check the "resulting" dataset!

        # TODO: use h5diff to shrink patches?

        # reopen the writable file as read-only
        fn = cfile.filename
        cfile.close()
        self._has_writable = False
        self._files[-1] = h5py.File(fn, "r")

    # ---- context manager support (i.e. to use `with`) ----

    def __enter__(self):
        return self

    def __exit__(self, ex_type, ex_value, ex_traceback):
        self.close()

    # ---- pass through group methods to an implicit root group instance ----

    @property
    def attrs(self):
        return self._root_group().attrs

    def create_group(self, gpath: str):
        return self._root_group().create_group(gpath)

    def __contains__(self, key):
        return key in self._root_group()

    def __setitem__(self, key, value):
        self._root_group()[key] = value

    def __delitem__(self, key):
        del self._root_group()[key]

    def __getitem__(self, key):
        return self._root_group()[key]

    def keys(self):
        return self._root_group().keys()

    def values(self):
        return self._root_group().values()

    def items(self):
        return self._root_group().items()
