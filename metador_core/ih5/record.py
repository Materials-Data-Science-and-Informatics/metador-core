"""
Immutable HDF5 container records.

A record consists of a base container and a number of patch containers.
This allows a record to work in settings where files are immutable, but still
provide a structured way of updating data stored inside.

Both base containers and patches are HDF5 files that are linked together
by some special attributes in the container root.
`IH5Record` is a class that wraps such a set of files. It features
* support for record creation and updating
* automatic handling of the patch mechanism (i.e., creating/finding corresponding files)
* transparent access to data in the record (possibly spanning multiple files)
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, TypeVar, Union
from uuid import UUID, uuid1

import h5py
from pydantic import BaseModel, Field, PrivateAttr
from typing_extensions import Annotated, Final

from ..hashutils import qualified_hashsum
from ..schema.types import hashsum_str
from .overlay import IH5Dataset, IH5Group, h5_copy_group_to_group

# the magic string we use to identify a valid container
FORMAT_MAGIC_STR: Final[str] = "ih5_v01"
"""Magic value at the beginning of the file to detect that an HDF5 file is valid IH5."""

USER_BLOCK_SIZE: Final[int] = 1024
"""Space to reserve at beginning of each HDF5 file in bytes.
Must be a power of 2 and at least 512 (required by HDF5)."""


def hashsum_file(filename: Path, skip_bytes: int = 0) -> str:
    """Compute hashsum of HDF5 file (ignoring the first `skip_bytes`)."""
    with open(filename, "rb") as f:
        f.seek(skip_bytes)
        return qualified_hashsum(f)


class IH5UserBlock(BaseModel):
    """IH5 metadata object parser and writer for the HDF5 user block.

    The user block stores technical administrative information linking together
    multiple HDF5 files that form a base container + patch sequence.

    The userblock begins with the magic format string, followed by a newline,
    followed by a string encoding the length of the user block, followed by a newline,
    followed by a serialized JSON object without newlines containing the metadata,
    followed by a NUL byte.
    """

    _userblock_size: int = PrivateAttr(default=USER_BLOCK_SIZE)
    """User block size claimed in the block itself (second line)."""

    record_uuid: UUID
    """UUID linking together multiple HDF5 files that form a (patched) record."""

    patch_index: Annotated[int, Field(ge=0)]
    """Index with the current revision number, i.e. the file is the n-th patch."""

    patch_uuid: UUID
    """UUID representing a certain state of the data in the record."""

    prev_patch: Optional[UUID]
    """UUID of the previous patch UUID (unless it is a base container, i.e. first one)."""

    hdf5_hashsum: Optional[hashsum_str] = None
    """Hashsum to verity integrity of the HDF5 data after the user block."""

    ub_exts: Dict[str, Any]
    """Any extra metadata to be stored in the user block, unvalidated in dicts.

    Subclasses must ensure that desired extra metadata is stored and loaded correctly.

    NOTE: In a merge of userblocks only newer extension section will be preserved!
    """

    @classmethod
    def create(cls, prev: Optional[IH5UserBlock] = None) -> IH5UserBlock:
        """Create a new user block for a base or patch container.

        If `prev` is None, will return a new base container block.
        Otherwise, will return a block linking back to the passed `prev` block.
        """
        ret = cls(
            patch_uuid=uuid1(),
            record_uuid=uuid1() if prev is None else prev.record_uuid,
            patch_index=0 if prev is None else prev.patch_index + 1,
            prev_patch=None if prev is None else prev.patch_uuid,
            ub_exts={},
        )
        return ret

    @classmethod
    def _read_head_raw(cls, stream, ub_size: int) -> Optional[Tuple[int, str]]:
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
            head = cls._read_head_raw(f, 512)
            if head is None:
                raise ValueError(f"{filename}: it doesn't look like a valid IH5 file!")
            if head[0] > 512:  # if stored user block size is bigger, re-read
                head = cls._read_head_raw(f, head[0])
                assert head is not None
        ret = IH5UserBlock.parse_obj(json.loads(head[1]))
        ret._userblock_size = head[0]
        return ret

    def save(self, filename: Union[Path, str]):
        """Save this object in the user block of the given HDF5 file."""
        filename = Path(filename)
        dat_str = f"{FORMAT_MAGIC_STR}\n{self._userblock_size}\n{self.json()}"
        data = dat_str.encode("utf-8")
        assert len(data) < USER_BLOCK_SIZE
        with open(filename, "r+b") as f:
            # check that this HDF file actually has a user block
            check = f.read(4)
            if check == b"\x89HDF":
                raise ValueError(f"{filename}: no user block reserved, can't write!")
            f.seek(0)
            f.write(data)
            f.write(b"\x00")  # mark end of the data


T = TypeVar("T", bound="IH5Record")


class IH5Record:
    """
    Class representing a record, which consists of a collection of immutable files.

    One file is a base container (with no linked predecessor state),
    the remaining files are a linear sequence of patch containers.

    Runtime invariants to be upheld before/after each method call (after __init__):

    * all files of an instance are open for reading (until `close()` is called)
    * all files in `_files` are in patch index order
    * at most one file is open in writable mode (if any, it is the last one)
    * modifications are possible only after `create` or `create_patch` was called
        and until `commit` or `discard_patch` was called, and at no other time
    """

    # Characters that may appear in a record name.
    # (to be put into regex [..] symbol braces)
    ALLOWED_NAME_CHARS = r"A-Za-z0-9\-"

    # filenames for a record named NAME are of the shape:
    # NAME[<PATCH_INFIX>.*]?<FILE_EXT>
    # NOTE: the first symbol of these must be one NOT in ALLOWED_NAME_CHARS!
    # This constraint is needed for correctly filtering filenames
    PATCH_INFIX = ".p"
    FILE_EXT = ".ih5"

    @classmethod
    def _is_valid_record_name(cls, name: str) -> bool:
        """Return whether a record name is valid."""
        return re.match(f"^[{cls.ALLOWED_NAME_CHARS}]+$", name) is not None

    @classmethod
    def _base_filename(cls, record_path: Path) -> Path:
        """Given a record path, return path to canonical base container name."""
        return Path(f"{record_path}{cls.FILE_EXT}")

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

    def _set_ublock(self, obj: Union[h5py.File, int], ub: IH5UserBlock):
        f = obj if isinstance(obj, h5py.File) else self._files[obj]
        self._ublocks[Path(f.filename)] = ub

    @classmethod
    def _new_container(cls, path: Path) -> h5py.File:
        """Initialize a fresh container file with reserved user block."""
        # create if does not exist, fail if it does
        return h5py.File(path, mode="x", userblock_size=USER_BLOCK_SIZE)

    def _check_ublock(
        self,
        filename: Union[str, Path],
        ub: IH5UserBlock,
        prev: Optional[IH5UserBlock] = None,
    ):
        """Check given container file.

        If `prev` block is given, assumes that `ub` is from a patch container,
        otherwise from base container.
        """
        filename = Path(filename)
        # check presence+validity of record uuid (should be the same for all)
        if ub.record_uuid != self.uuid:
            msg = "'record_uuid' inconsistent! Mixed up records?"
            raise ValueError(f"{filename}: {msg}")

        # hash must match with HDF5 content (i.e. integrity check)
        if ub.hdf5_hashsum is None:
            msg = "hdf5_checksum is missing!"
            raise ValueError(f"{filename}: {msg}")
        chksum = hashsum_file(filename, skip_bytes=USER_BLOCK_SIZE)
        if ub.hdf5_hashsum != chksum:
            msg = "file has been modified, stored and computed checksum are different!"
            raise ValueError(f"{filename}: {msg}")

        # check patch chain structure
        if prev is not None:
            if ub.patch_index <= prev.patch_index:
                msg = "patch container must have greater index than predecessor!"
                raise ValueError(f"{filename}: {msg}")
            if ub.prev_patch is None:
                msg = "patch must have an attribute 'prev_patch'!"
                raise ValueError(f"{filename}: {msg}")
            # claimed predecessor uuid must match with the predecessor by index
            # (can compare as strings directly, as we checked those already)
            if ub.prev_patch != prev.patch_uuid:
                msg = f"patch for {ub.prev_patch}, but predecessor is {prev.patch_uuid}"
                raise ValueError(f"{filename}: {msg}")

    def _expect_open(self):
        if self._files is None:
            raise ValueError("record is not open!")

    def _clear(self):
        """Clear all contents of the record."""
        for k in self.attrs.keys():
            del self.attrs[k]
        for k in self.keys():
            del self[k]

    # ---- public attributes and interface ----

    @property
    def uuid(self) -> UUID:
        """Return the common record UUID of the set of containers."""
        return self._ublock(0).record_uuid

    @property
    def name(self) -> str:
        """Inferred name of record (i.e. common filename prefix of the containers)."""
        path = Path(self._files[0].filename)
        return path.name.split(self.FILE_EXT)[0].split(self.PATCH_INFIX)[0]

    @property
    def containers(self) -> List[Path]:
        """List of container filenames this record consists of."""
        return [Path(f.filename) for f in self._files]

    @property
    def ih5_meta(self) -> List[IH5UserBlock]:
        """Return user block metadata, in container patch order."""
        return [self._ublock(i).copy() for i in range(len(self._files))]

    @property
    def read_only(self) -> bool:
        """Return whether this record is read-only at the moment."""
        return not self._has_writable

    @property
    def is_empty(self) -> bool:
        """Return whether this record currently contains any data."""
        return not self.attrs.keys() and not self.keys()

    def __init__(self, paths: List[Path], **kwargs):
        """Open a record consisting of a base container + possible set of patches.

        Expects a set of full file paths forming a valid record.
        Will throw an exception in case of a detected inconsistency.
        """
        allow_baseless = kwargs.get("allow_baseless", False)
        if not paths:
            raise ValueError("Cannot open empty list of containers!")

        self._has_writable: bool = False
        self._ublocks = {Path(path): IH5UserBlock.load(path) for path in paths}
        self._files: List[h5py.File] = [h5py.File(path, "r") for path in paths]
        # sort files by patch index order (important!)
        # if something is wrong with the indices, this will throw an exception.
        self._files.sort(key=lambda f: self._ublock(f).patch_index)

        # check containers and relationship to each other:

        # check first container (it could be a base container and it has no predecessor)
        if not allow_baseless and self._ublock(0).prev_patch is not None:
            msg = "base container must not have attribute 'prev_patch'!"
            raise ValueError(f"{self._files[0].filename}: {msg}")
        self._check_ublock(self._files[0].filename, self._ublock(0))

        # check successive patches
        for i in range(1, len(self._files)):
            self._check_ublock(
                self._files[i].filename, self._ublock(i), self._ublock(i - 1)
            )

        # additional sanity check: container uuids must be all distinct
        cn_uuids = {self._ublock(f).patch_uuid for f in self._files}
        if len(cn_uuids) != len(self._files):
            raise ValueError("Some patch_uuid is not unique, invalid file set!")

    @classmethod
    def create(cls: Type[T], record: Union[Path, str], **kwargs) -> T:
        """Create a new record consisting of a base container.

        The base container is exposed as the `writable` container.
        """
        record = Path(record)  # in case it was a str
        if not cls._is_valid_record_name(record.name):
            raise ValueError(f"Invalid record name: '{record.name}'")
        path = cls._base_filename(record)

        # if overwrite flag is set, check and remove old record if present
        overwrite = kwargs.get("overwrite", False)
        if overwrite and path.is_file():
            cls.delete(record)

        # create new container
        ret = cls.__new__(cls)
        ret._has_writable = True
        ret._files = [cls._new_container(path)]
        ret._ublocks = {path: IH5UserBlock.create(prev=None)}
        return ret

    @classmethod
    def find_containers(cls, record: Path) -> List[Path]:
        """Return container names that look like they belong to the same record.

        This operation is based on purely syntactic pattern matching on file names.
        Given a path `/foo/bar`, it will find all containers in directory
        `/foo` whose name starts with `bar` followed by the correct file extension(s),
        such as `/foo/bar.rdm.h5` and `/foo/bar.p01.rdm.h5`.
        """
        record = Path(record)  # in case it was a str
        if not cls._is_valid_record_name(record.name):
            raise ValueError(f"Invalid record name: '{record.name}'")

        record = Path(record)  # in case it was a str
        globstr = f"{record.name}*{cls.FILE_EXT}"  # rough wildcard pattern
        # filter out possible false positives (i.e. foobar* matching foo* as well)
        paths = []
        for p in record.parent.glob(globstr):
            if re.match(f"^{record.name}[^{cls.ALLOWED_NAME_CHARS}]", p.name):
                paths.append(p)
        return paths

    @classmethod
    def open(cls: Type[T], record: Path, **kwargs) -> T:
        """Open a record for read access.

        This method uses `find_containers` to infer the correct file set.
        """
        paths = cls.find_containers(record)
        if not paths:
            raise FileNotFoundError(f"No containers found for record: {record}")
        return cls(paths, **kwargs)

    def close(self) -> None:
        """Commit changes and close all containers that belong to that record.

        After this, the object may not be used anymore.
        """
        if self._files is None:
            return  # nothing to do

        if self._has_writable:
            self.commit()
        for f in self._files:
            f.close()
        self._files.clear()
        self._files = None  # type: ignore
        self._has_writable = False

    def create_patch(self) -> None:
        """Create a new patch container to enable writing to the record."""
        self._expect_open()
        if self._has_writable:
            raise ValueError("There already exists a writable container, commit first!")

        path = self._next_patch_filepath()
        self._ublocks[path] = IH5UserBlock.create(prev=self._ublock(-1))
        self._files.append(self._new_container(path))
        self._has_writable = True

    def _delete_latest_container(self) -> None:
        """Discard the current writable container (patch or base)."""
        cfile = self._files.pop()
        fn = cfile.filename
        del self._ublocks[Path(fn)]
        cfile.close()
        Path(fn).unlink()
        self._has_writable = False

    def discard_patch(self) -> None:
        """Discard the current writable patch container."""
        self._expect_open()
        if not self._has_writable:
            raise ValueError("Record is read-only, nothing to discard!")
        if len(self._files) == 1:
            raise ValueError("Cannot discard base container! Just delete the file!")
            # reason: the base container provides record_uuid,
            # destroying it makes this object inconsistent / breaks invariants
            # so if this is done, it should not be used anymore.
        return self._delete_latest_container()

    def commit(self, **kwargs) -> None:
        """Complete the current writable container (base or patch) for the record.

        Will perform checks on the new container and throw an exception on failure.

        After this, continuing to edit the writable container is prohibited.
        Instead, it is added to the record as a read-only base container or patch.
        """
        if kwargs:
            raise ValueError(f"Unknown keyword arguments: {kwargs}")

        self._expect_open()
        if not self._has_writable:
            raise ValueError("Record is read-only, nothing to commit!")
        cfile = self._files[-1]
        filepath = Path(cfile.filename)
        cfile.close()  # must close it now, as we will write outside of HDF5 next

        # compute checksum, write user block
        chksum = hashsum_file(filepath, skip_bytes=USER_BLOCK_SIZE)
        self._ublocks[filepath].hdf5_hashsum = chksum
        self._ublocks[filepath].save(filepath)

        # reopen the container file now as read-only
        self._files[-1] = h5py.File(filepath, "r")
        self._has_writable = False

    def _fixes_after_merge(self, merged_file, ub):
        """Run hook for subclasses into merge process.

        The method is called after creating the merged container, but before
        updating its user block on disk.

        The passed userblock is a prepared userblock with updated HDF5 hashsum for the
        merged container and adapted prev_patch field, as will it be written to the file.
        Additional changes done to it in-place will be included.

        The passed filename can be used to perform additional necessary actions.
        """
        pass

    def merge(self, target: Path) -> Path:
        """Given a path with a record name, merge current record into new container.

        Returns new resulting container.
        """
        self._expect_open()
        if self._has_writable:
            raise ValueError("Cannot merge, please commit or discard your changes!")

        with self.create(target) as ds:
            h5_copy_group_to_group(self["/"], ds["/"])
            cfile = ds.containers[0]  # store filename to override userblock afterwards

        # compute new merged userblock
        ub = self._ublock(-1).copy(update={"prev_patch": self._ublock(0).prev_patch})
        # update hashsum with saved new merged hdf5 payload
        chksum = hashsum_file(cfile, skip_bytes=USER_BLOCK_SIZE)
        ub.hdf5_hashsum = chksum

        self._fixes_after_merge(cfile, ub)  # for subclass hooks

        self._set_ublock(-1, ub)
        ub.save(cfile)
        return cfile

    @classmethod
    def delete(cls, record: Path):
        """Irreversibly(!) delete all containers matching the record path.

        This object is invalid after this operation.
        """
        for file in cls.find_containers(record):
            file.unlink()

    # ---- context manager support (i.e. to use `with`) ----

    def __enter__(self):
        return self

    def __exit__(self, ex_type, ex_value, ex_traceback):
        # this will ensure that commit() is called and the files are closed
        self.close()

    # ---- pass through HDF5 group methods to an implicit root group instance ----

    def _root_group(self) -> IH5Group:
        return IH5Group(self._files)

    @property
    def attrs(self):
        """See [h5py.Group.attrs](https://docs.h5py.org/en/latest/high/group.html#h5py.Group.attrs)."""
        return self._root_group().attrs

    def create_dataset(self, gpath: str, *args, **kwargs) -> IH5Dataset:
        """See [h5py.Group.create_group](https://docs.h5py.org/en/latest/high/group.html#h5py.Group.create_group)."""
        return self._root_group().create_dataset(gpath, *args, **kwargs)

    def create_group(self, gpath: str) -> IH5Group:
        """See [h5py.Group.create_group](https://docs.h5py.org/en/latest/high/group.html#h5py.Group.create_group)."""
        return self._root_group().create_group(gpath)

    def visit(self, func: Callable[[str], Optional[Any]]) -> Any:
        """See [h5py.Group.visit](https://docs.h5py.org/en/latest/high/group.html#h5py.Group.visit)."""
        return self._root_group().visit(func)

    def visititems(self, func: Callable[[str, object], Optional[Any]]) -> Any:
        """See [h5py.Group.visititems](https://docs.h5py.org/en/latest/high/group.html#h5py.Group.visititems)."""
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

    def __len__(self):
        return len(self._root_group())

    def get(self, key: str, default=None):
        return self._root_group().get(key, default)

    def keys(self):
        return self._root_group().keys()

    def values(self):
        return self._root_group().values()

    def items(self):
        return self._root_group().items()
