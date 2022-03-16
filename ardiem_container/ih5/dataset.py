"""
Management of immutable HDF5 container datasets.

A dataset consists of a base container and a number of patch containers.
This allows a dataset to work in settings where files are immutable, but still
provide a structured way of updating data stored inside.

Both base containers and patches are HDF5 files that are linked together
by some special attributes in the container root.
`IH5Dataset` is a class that wraps such a set of files. It features
* support for dataset creation and updating
* automatic handling of the patch mechanism (i.e., creating/finding corresponding files)
* transparent access to data in the dataset (possibly spanning multiple files)
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Callable, List, Optional, Tuple, Union
from uuid import UUID, uuid1

import h5py
from pydantic import BaseModel, Field, PrivateAttr
from typing_extensions import Annotated, Final

from ..util import hashsum
from .overlay import IH5Group, IH5Value

# the magic string we use to identify a valid container
FORMAT_MAGIC_STR: Final[str] = "ih5_v01"

# space to reserve at beginning of each HDF5 file in bytes.
# must be a power of 2 and at least 512 (required by HDF5)
USER_BLOCK_SIZE: Final[int] = 512

# algorithm to use and prepend to a hashsum
HASH_ALG = "sha256"


def compute_hashsum(filename: Path, skip_bytes: int = 0) -> str:
    """Compute hashsum of HDF5 file (ignoring the first `skip_bytes`)."""
    with open(filename, "rb") as f:
        f.seek(skip_bytes)
        chksum = hashsum(f, HASH_ALG)
    return f"{HASH_ALG}:{chksum}"


class IH5UserBlock(BaseModel):
    """IH5 metadata object parser and writer for the HDF5 user block.

    The userblock begins with the magic format string, followed by a newline,
    followed by a string encoding the length of the user block, followed by a newline,
    followed by a serialized JSON object without newlines containing the metadata,
    followed by a NUL byte.
    """

    _filename: Path = PrivateAttr(Path(""))
    """Filename this user block was loaded from."""

    _userblock_size: int = PrivateAttr(default=USER_BLOCK_SIZE)
    """User block size claimed in the block itself (second line)."""

    dataset_uuid: UUID
    """UUID linking together multiple HDF5 files that form a (patched) dataset."""

    patch_index: Annotated[int, Field(ge=0)]
    """Index with the current revision number, i.e. the file is the n-th patch."""

    patch_uuid: UUID
    """UUID representing a certain state of the data in the dataset."""

    prev_patch: Optional[UUID]
    """UUID of the previous patch UUID, so that that predecessor container is required."""

    data_hashsum: Annotated[str, Field(regex=r"^\w+:\w+$")]
    """Hashsum to verity integrity of the HDF5 data after the user block."""

    @classmethod
    def create(
        cls, prev: Optional[IH5UserBlock] = None, path: Optional[Path] = None
    ) -> IH5UserBlock:
        """Create a new user block for a base or patch container.

        If `prev` is None, will return a new base container block.
        Otherwise, will return a block linking back to the passed `prev` block.

        If `path` is passed, will set `_filename` attribute.
        """
        ret = cls(
            patch_uuid=uuid1(),
            dataset_uuid=uuid1() if prev is None else prev.dataset_uuid,
            patch_index=0 if prev is None else prev.patch_index + 1,
            prev_patch=None if prev is None else prev.patch_uuid,
            data_hashsum=f"{HASH_ALG}:toBeComputed",
        )
        if path is not None:
            ret._filename = path
        return ret

    @classmethod
    def read_head_raw(cls, stream, ub_size: int) -> Optional[Tuple[int, str]]:
        """Try reading user block.

        Args:
            stream: the open binary file stream
            ub_size: number of bytes to read

        Returns:
            (user block size claimed in block, embedded data until first NUL byte)
            or None, if block does not look right.
        """
        stream.seek(0)
        probe = stream.read(ub_size)
        dat = probe.decode("utf-8").split("\n")
        if len(dat) != 3 or dat[0] != FORMAT_MAGIC_STR:
            return None
        return (int(dat[1]), dat[2][: dat[2].find("\x00")])  # read until first NUL byte

    @classmethod
    def load(cls, filename: Path) -> IH5UserBlock:
        """Load a user block of the given HDF5 file."""
        with open(filename, "rb") as f:
            # try smallest valid UB size first
            head = cls.read_head_raw(f, 512)
            if head is None:
                raise ValueError(f"{filename}: it doesn't look like a valid IH5 file!")
            if head[0] > 512:  # if stored user block size is bigger, re-read
                head = cls.read_head_raw(f, head[0])
                assert head is not None
        ret = IH5UserBlock.parse_obj(json.loads(head[1]))
        ret._userblock_size = head[0]
        ret._filename = filename
        return ret

    def merge(self, other: IH5UserBlock, target: Path) -> IH5UserBlock:
        """Merge this user block with another one.

        This userblock must be the first one in a merged patch sequence,
        while the argument must be the userblock from the last one.
        """
        ret = self.copy()
        ret.patch_index = other.patch_index
        ret.patch_uuid = other.patch_uuid
        ret._filename = target
        ret.data_hashsum = ""  # to be computed
        return ret

    def save(self, filename: Optional[Path] = None):
        """Save this object in the user block of the given HDF5 file.

        If no path is given, will save back to file this block was loaded from.
        """
        dat_str = f"{FORMAT_MAGIC_STR}\n{self._userblock_size}\n{self.json()}"
        data = dat_str.encode("utf-8")
        assert len(data) < USER_BLOCK_SIZE
        filename = filename or self._filename
        with open(filename, "r+b") as f:
            # check that this HDF file actually has a user block
            check = f.read(4)
            if check == b"\x89HDF":
                raise ValueError(f"{filename}: no user block reserved, can't write!")
            f.seek(0)
            f.write(data)
            f.write(b"\x00")  # mark end of the data


class IH5Dataset:
    """
    Class representing a dataset, which consists of a collection of immutable files.

    One file is a base container (with no linked predecessor state),
    the remaining files are a linear sequence of patch containers.

    Runtime invariants:
        * all files of an instance are open for reading (until `close()` is called)
        * all files in `_files` are in patch index order
        * at most one file is open in writable mode (if any, it is the last one)
        * modifications are possible only after `create` or `create_patch` was called
          and until `commit` or `discard` was called, and at no other time

    Only creation of and access to containers is supported.
    Renaming or deleting a container collection is not supported.
    For this, use `IH5Dataset.find_containers` and apply standard tools.
    """

    # Characters that may appear in a dataset name.
    # (to be put into regex [..] symbol braces)
    ALLOWED_NAME_CHARS = r"A-Za-z0-9\-"

    # filenames for a dataset named NAME are of the shape:
    # NAME[<PATCH_INFIX>.*]?<FILE_EXT>
    # NOTE: the first symbol of these must be one NOT in ALLOWED_NAME_CHARS!
    # This constraint is needed for correctly filtering filenames
    PATCH_INFIX = ".p"
    FILE_EXT = ".ih5"

    @classmethod
    def _is_valid_dataset_name(cls, name: str) -> bool:
        """Return whether a dataset name is valid."""
        return re.match(f"^[{cls.ALLOWED_NAME_CHARS}]+$", name) is not None

    @classmethod
    def _base_filename(cls, dataset_path: Path) -> Path:
        """Given a dataset path, return path to canonical base container name."""
        return Path(f"{dataset_path}{cls.FILE_EXT}")

    def _next_patch_filepath(self) -> Path:
        """Compute filepath for the next patch based on the previous one."""
        path = Path(self._files[0].filename).parent
        patch_index = self._ublock(-1).patch_index + 1
        res = f"{path}/{self.name}{self.PATCH_INFIX}{patch_index}{self.FILE_EXT}"
        return Path(res)

    def _ublock(self, obj: Union[h5py.File, int]) -> IH5UserBlock:
        """Return the parsed user block of a container file."""
        f = obj if isinstance(obj, h5py.File) else self._files[obj]
        return self._ublocks[Path(f.filename)]

    @classmethod
    def _new_container(cls, path: Path) -> h5py.File:
        """Initialize a fresh container file with reserved user block."""
        # create if does not exist, fail if it does
        return h5py.File(path, mode="x", userblock_size=USER_BLOCK_SIZE)

    def _check_ublock(self, ub: IH5UserBlock, prev: Optional[IH5UserBlock] = None):
        """Check given container file.

        If `prev` block is given, assumes that `ub` is from a patch container,
        otherwise from base container.
        """
        # check presence+validity of dataset uuid (should be the same for all)
        if ub.dataset_uuid != self.uuid:
            msg = "'dataset_uuid' inconsistent! Mixed up datasets?"
            raise ValueError(f"{ub._filename}: {msg}")

        # hash must match with HDF5 content (i.e. integrity check)
        chksum = compute_hashsum(ub._filename, skip_bytes=USER_BLOCK_SIZE)
        if ub.data_hashsum != chksum:
            msg = "file has been modified, stored and computed checksum are different!"
            raise ValueError(f"{ub._filename}: {msg}")

        # check patch chain structure
        if prev is not None:
            if ub.patch_index <= prev.patch_index:
                msg = "patch container must have greater index than predecessor!"
                raise ValueError(f"{ub._filename}: {msg}")
            if ub.prev_patch is None:
                msg = "Patch must have an attribute 'prev_patch'!"
                raise ValueError(f"{ub._filename}: {msg}")

            # claimed predecessor uuid must match with the predecessor by index
            # (can compare as strings directly, as we checked those already)
            if ub.prev_patch != prev.patch_uuid:
                msg = f"Patch for {ub.prev_patch}, but predecessor is {prev.patch_uuid}"
                raise ValueError(f"{ub._filename}: {msg}")

    def _root_group(self) -> IH5Group:
        return IH5Group(self._files)

    # ---- public attributes and interface ----

    @property
    def uuid(self) -> UUID:
        """Return the common dataset UUID of the set of containers."""
        return self._ublock(0).dataset_uuid

    @property
    def name(self) -> str:
        """Inferred name of dataset (i.e. common filename prefix of the containers)."""
        path = Path(self._files[0].filename)
        return path.name.split(self.FILE_EXT)[0].split(self.PATCH_INFIX)[0]

    @property
    def containers(self) -> List[Path]:
        """List of container filenames this dataset consists of."""
        return [Path(f.filename) for f in self._files]

    @property
    def read_only(self) -> bool:
        """Return whether this dataset is read-only at the moment."""
        return not self._has_writable

    def __init__(self, paths: List[Path], allow_baseless: bool = False):
        """Open a dataset consisting of a base container + possible set of patches.

        Expects a set of full file paths forming a valid dataset.
        Will throw an exception in case of a detected inconsistency.
        """
        if not paths:
            raise ValueError("Cannot open empty list of containers!")

        self._has_writable: bool = False
        self._ublocks = {path: IH5UserBlock.load(path) for path in paths}
        self._files: List[h5py.File] = [h5py.File(path, "r") for path in paths]
        # sort files by patch index order (important!)
        # if something is wrong with the indices, this will throw an exception.
        self._files.sort(key=lambda f: self._ublock(f).patch_index)

        # check containers and relationship to each other:

        # check first container (it could be a base container and it has no predecessor)
        if not allow_baseless and self._ublock(0).prev_patch is not None:
            msg = "base container must not have attribute 'prev_patch'!"
            raise ValueError(f"{self._ublock(0)._filename}: {msg}")
        self._check_ublock(self._ublock(0))

        # check successive patches
        for i in range(1, len(self._files)):
            self._check_ublock(self._ublock(i), self._ublock(i - 1))

        # additional sanity check: container uuids must be all distinct
        cn_uuids = {self._ublock(f).patch_uuid for f in self._files}
        if len(cn_uuids) != len(self._files):
            raise ValueError("Some patch_uuid is not unique, invalid file set!")

    @classmethod
    def create(cls, dataset: Union[Path, str]) -> IH5Dataset:
        """Create a new dataset consisting of a base container.

        The base container is exposed as the `writable` container.
        """
        dataset = Path(dataset)  # in case it was a str
        if not cls._is_valid_dataset_name(dataset.name):
            raise ValueError(f"Invalid dataset name: '{dataset.name}'")

        path = cls._base_filename(dataset)
        ret = IH5Dataset.__new__(IH5Dataset)
        ret._has_writable = True
        ret._files = [cls._new_container(path)]
        ret._ublocks = {path: IH5UserBlock.create(prev=None, path=path)}
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
    def open(cls, dataset: Path) -> IH5Dataset:
        """Open a dataset for read access.

        This method uses `find_containers` to infer the correct file set.
        """
        paths = cls.find_containers(dataset)
        if not paths:
            raise ValueError(f"No containers found for dataset: {dataset}")
        return cls(paths)

    def close(self) -> None:
        """Commit changes and close all containers that belong to that dataset.

        After this, the object may not be used anymore.
        """
        if self._has_writable:
            self.commit()
        for f in self._files:
            f.close()
        self._files.clear()
        self._has_writable = False

    def create_patch(self) -> None:
        """Create a new patch container to enable writing to the dataset."""
        if self._has_writable:
            raise ValueError("There already exists a writable container, commit first!")

        path = self._next_patch_filepath()
        self._ublocks[path] = IH5UserBlock.create(prev=self._ublock(-1), path=path)
        self._files.append(self._new_container(path))
        self._has_writable = True

    def discard_patch(self) -> None:
        """Discard the current writable patch container."""
        if not self._has_writable:
            raise ValueError("Dataset is read-only, nothing to discard!")
        if self._ublock(-1).prev_patch is None:
            raise ValueError("Cannot discard base container! Just delete the file!")
            # reason: the base container provides dataset_uuid,
            # destroying it makes this object inconsistent / breaks invariants

        cfile = self._files.pop()
        fn = cfile.filename
        del self._ublocks[Path(fn)]
        cfile.close()
        Path(fn).unlink()
        self._has_writable = False

    def commit(self) -> None:
        """Complete the current writable container (base or patch) for the dataset.

        Will perform checks on the new container and throw an exception on failure.

        After this, continuing to edit the writable container is prohibited.
        Instead, it is added to the dataset as a read-only base container or patch.
        """
        if not self._has_writable:
            raise ValueError("Dataset is read-only, nothing to commit!")
        cfile = self._files[-1]
        filepath = Path(cfile.filename)
        cfile.close()  # must close it now, as we will write outside of HDF5 next

        # compute checksum, write user block
        chksum = compute_hashsum(filepath, skip_bytes=USER_BLOCK_SIZE)
        self._ublocks[filepath].data_hashsum = chksum
        self._ublocks[filepath].save(filepath)

        # TODO: here we would plug in more checks for the
        # fine-grained format... delegate to subclass function?
        # this should use the overlay to check the "resulting" dataset!

        # reopen the container file now as read-only
        self._files[-1] = h5py.File(filepath, "r")
        self._has_writable = False

    def merge(self, target: Path) -> Path:
        """Given a path with a dataset name, merge current dataset into new container.

        Returns full filename of the single resulting container file.
        """
        if self._has_writable:
            raise ValueError("Cannot merge, please commit or discard your changes!")

        with IH5Dataset.create(target) as ds:
            for k, v in self.attrs.items():  # copy root attributes
                ds.attrs[k] = v

            def copy_children(name, node):
                if isinstance(node, IH5Group):
                    ds.create_group(node._gpath)
                elif isinstance(node, IH5Value):
                    ds[node._gpath] = node[()]
                new_atrs = ds[node._gpath].attrs  # copy node attributes
                for k, v in node.attrs.items():
                    new_atrs[k] = v

            self.visititems(copy_children)

            cfile = ds.containers[0]  # store filename to override userblock afterwards
        # compute new merged userblock and store it
        ub = self._ublock(0).merge(self._ublock(-1), cfile)
        chksum = compute_hashsum(cfile, skip_bytes=USER_BLOCK_SIZE)
        ub.data_hashsum = chksum
        ub.save()
        return cfile

    # ---- context manager support (i.e. to use `with`) ----

    def __enter__(self):
        return self

    def __exit__(self, ex_type, ex_value, ex_traceback):
        # this will ensure that commit() is called and the files are closed
        self.close()

    # ---- pass through group methods to an implicit root group instance ----

    @property
    def attrs(self):
        return self._root_group().attrs

    def create_group(self, gpath: str) -> IH5Group:
        return self._root_group().create_group(gpath)

    def visit(self, func: Callable[[str], Optional[Any]]) -> Any:
        return self._root_group().visit(func)

    def visititems(self, func: Callable[[str, object], Optional[Any]]) -> Any:
        return self._root_group().visititems(func)

    def __iter__(self):
        return iter(self._root_group())

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
