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
from typing import Any, Dict, List, Optional, Tuple, Type, TypeVar, Union, get_args
from uuid import UUID, uuid1

import h5py
from pydantic import BaseModel, Field, PrivateAttr
from typing_extensions import Annotated, Final, Literal

from ..schema.types import QualHashsumStr
from ..util.hashsums import qualified_hashsum
from .overlay import IH5Group, h5_copy_from_to

# the magic string we use to identify a valid container
FORMAT_MAGIC_STR: Final[str] = "ih5_v01"
"""Magic value at the beginning of the file to detect that an HDF5 file is valid IH5."""

USER_BLOCK_SIZE: Final[int] = 1024
"""Space to reserve at beginning of each HDF5 file in bytes.
Must be a power of 2 and at least 512 (required by HDF5)."""

T = TypeVar("T", bound="IH5Record")


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

    hdf5_hashsum: Optional[QualHashsumStr] = None
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


OpenMode = Literal["r", "r+", "a", "w", "w-", "x"]
"""User open modes that can be passed during initialization."""

_OPEN_MODES = list(get_args(OpenMode))


class IH5Record(IH5Group):
    """
    Class representing a record, which consists of a collection of immutable files.

    One file is a base container (with no linked predecessor state),
    the remaining files are a linear sequence of patch containers.

    Runtime invariants to be upheld before/after each method call (after __init__):

    * all files of an instance are open for reading (until `close()` is called)
    * all files in `__files__` are in patch index order
    * at most one file is open in writable mode (if any, it is the last one)
    * modifications are possible only after `create_patch` was called
        and until `commit_patch` or `discard_patch` was called, and at no other time
    """

    # Characters that may appear in a record name.
    # (to be put into regex [..] symbol braces)
    _ALLOWED_NAME_CHARS = r"A-Za-z0-9\-"

    # filenames for a record named NAME are of the shape:
    # NAME[<PATCH_INFIX>.*]?<FILE_EXT>
    # NOTE: the first symbol of these must be one NOT in ALLOWED_NAME_CHARS!
    # This constraint is needed for correctly filtering filenames
    _PATCH_INFIX = ".p"
    _FILE_EXT = ".ih5"

    # core "wrapped" objects
    __files__: List[h5py.File]

    # attributes
    _closed: bool  # True after close()
    _allow_patching: bool  # false iff opened with "r"
    _ublocks: Dict[Path, IH5UserBlock]  # in-memory copy of HDF5 user blocks

    def __new__(cls, *args, **kwargs):
        ret = super().__new__(cls)
        ret._allow_patching = True
        ret.__files__ = []
        return ret

    def __eq__(self, o) -> bool:
        return self.__files__ == o.__files__

    @property
    def _has_writable(self):
        """Return True iff an uncommitted patch exists."""
        if not self.__files__:
            return False
        f = self.__files__[-1]
        return bool(f) and f.mode == "r+"

    @classmethod
    def _is_valid_record_name(cls, name: str) -> bool:
        """Return whether a record name is valid."""
        return re.match(f"^[{cls._ALLOWED_NAME_CHARS}]+$", name) is not None

    @classmethod
    def _base_filename(cls, record_path: Path) -> Path:
        """Given a record path, return path to canonical base container name."""
        return Path(f"{record_path}{cls._FILE_EXT}")

    @classmethod
    def _infer_name(cls, record_path: Path) -> str:
        return record_path.name.split(cls._FILE_EXT)[0].split(cls._PATCH_INFIX)[0]

    def _next_patch_filepath(self) -> Path:
        """Compute filepath for the next patch based on the previous one."""
        path = Path(self.__files__[0].filename)
        parent = path.parent
        patch_index = self._ublock(-1).patch_index + 1
        res = f"{parent}/{self._infer_name(path)}{self._PATCH_INFIX}{patch_index}{self._FILE_EXT}"
        return Path(res)

    def _ublock(self, obj: Union[h5py.File, int]) -> IH5UserBlock:
        """Return the parsed user block of a container file."""
        f: h5py.File = obj if isinstance(obj, h5py.File) else self.__files__[obj]
        return self._ublocks[Path(f.filename)]

    def _set_ublock(self, obj: Union[h5py.File, int], ub: IH5UserBlock):
        f: h5py.File = obj if isinstance(obj, h5py.File) else self.__files__[obj]
        self._ublocks[Path(f.filename)] = ub

    @classmethod
    def _new_container(cls, path: Path, ub: IH5UserBlock) -> h5py.File:
        """Initialize a fresh container file with reserved user block."""
        # create if does not exist, fail if it does
        f = h5py.File(path, mode="x", userblock_size=USER_BLOCK_SIZE)
        # close to pre-fill userblock
        f.close()
        ub.save(path)
        # reopen the container file
        return h5py.File(path, "r+")

    def _check_ublock(
        self,
        filename: Union[str, Path],
        ub: IH5UserBlock,
        prev: Optional[IH5UserBlock] = None,
        check_hashsum: bool = True,
    ):
        """Check given container file.

        If `prev` block is given, assumes that `ub` is from a patch container,
        otherwise from base container.
        """
        filename = Path(filename)
        # check presence+validity of record uuid (should be the same for all)
        if ub.record_uuid != self.ih5_uuid:
            msg = "'record_uuid' inconsistent! Mixed up records?"
            raise ValueError(f"{filename}: {msg}")

        # hash must match with HDF5 content (i.e. integrity check)
        if check_hashsum and ub.hdf5_hashsum is None:
            msg = "hdf5_checksum is missing!"
            raise ValueError(f"{filename}: {msg}")
        if ub.hdf5_hashsum is not None:
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
        if self._closed:
            raise ValueError("Record is not open!")

    def _clear(self):
        """Clear all contents of the record."""
        for k in self.attrs.keys():
            del self.attrs[k]
        for k in self.keys():
            del self[k]

    def _is_empty(self) -> bool:
        """Return whether this record currently contains any data."""
        return not self.attrs.keys() and not self.keys()

    @classmethod
    def _create(cls: Type[T], record: Union[Path, str], truncate: bool = False) -> T:
        """Create a new record consisting of a base container.

        The base container is exposed as the `writable` container.
        """
        record = Path(record)  # in case it was a str
        if not cls._is_valid_record_name(record.name):
            raise ValueError(f"Invalid record name: '{record.name}'")
        path = cls._base_filename(record)

        # if overwrite flag is set, check and remove old record if present
        if truncate and path.is_file():
            cls.delete_files(record)

        # create new container
        ret = cls.__new__(cls)
        super().__init__(ret, ret)
        ret._closed = False

        ub = IH5UserBlock.create(prev=None)
        ret._ublocks = {path: ub}

        ret.__files__ = [cls._new_container(path, ub)]
        return ret

    @classmethod
    def _open(cls: Type[T], paths: List[Path], **kwargs) -> T:
        """Open a record consisting of a base container + possible set of patches.

        Expects a set of full file paths forming a valid record.
        Will throw an exception in case of a detected inconsistency.

        Will open latest patch in writable mode if it lacks a hdf5 checksum.
        """
        if not paths:
            raise ValueError("Cannot open empty list of containers!")
        allow_baseless: bool = kwargs.pop("allow_baseless", False)

        ret = cls.__new__(cls)
        super().__init__(ret, ret)
        ret._closed = False

        ret._ublocks = {Path(path): IH5UserBlock.load(path) for path in paths}
        # files, sorted  by patch index order (important!)
        # if something is wrong with the indices, this will throw an exception.
        ret.__files__ = [h5py.File(path, "r") for path in paths]
        ret.__files__.sort(key=lambda f: ret._ublock(f).patch_index)
        # ----
        has_patches: bool = len(ret.__files__) > 1

        # check containers and relationship to each other:

        # check first container (it could be a base container and it has no predecessor)
        if not allow_baseless and ret._ublock(0).prev_patch is not None:
            msg = "base container must not have attribute 'prev_patch'!"
            raise ValueError(f"{ret.__files__[0].filename}: {msg}")
        ret._check_ublock(ret.__files__[0].filename, ret._ublock(0), None, has_patches)

        # check patches except last one (with checking the hashsum)
        for i in range(1, len(ret.__files__) - 1):
            filename = ret.__files__[i].filename
            ret._check_ublock(filename, ret._ublock(i), ret._ublock(i - 1), True)
        if has_patches:  # check latest patch (without checking hashsum)
            ret._check_ublock(
                ret.__files__[-1].filename, ret._ublock(-1), ret._ublock(-2), False
            )

        # now check whether the last container (patch or base or whatever) has a checksum
        if ret._ublock(-1).hdf5_hashsum is None:
            if kwargs.pop("reopen_incomplete_patch", False):
                # if opening in writable mode, allow to complete the patch
                f = ret.__files__[-1]
                path = ret.__files__[-1].filename
                f.close()
                ret.__files__[-1] = h5py.File(Path(path), "r+")

        # additional sanity check: container uuids must be all distinct
        cn_uuids = {ret._ublock(f).patch_uuid for f in ret.__files__}
        if len(cn_uuids) != len(ret.__files__):
            raise ValueError("Some patch_uuid is not unique, invalid file set!")
        # all looks good
        return ret

    # ---- public attributes and interface ----

    @property
    def ih5_uuid(self) -> UUID:
        """Return the common record UUID of the set of containers."""
        return self._ublock(0).record_uuid

    @property
    def ih5_files(self) -> List[Path]:
        """List of container filenames this record consists of."""
        return [Path(f.filename) for f in self.__files__]

    @property
    def ih5_meta(self) -> List[IH5UserBlock]:
        """Return user block metadata, in container patch order."""
        return [self._ublock(i).copy() for i in range(len(self.__files__))]

    @classmethod
    def find_files(cls, record: Path) -> List[Path]:
        """Return file names that look like they belong to the same record.

        This operation is based on purely syntactic pattern matching on file names
        that follow the default naming convention.

        Given a path `/foo/bar`, will find all containers in directory
        `/foo` whose name starts with `bar` followed by the correct file extension(s),
        such as `/foo/bar.ih5` and `/foo/bar.p1.ih5`.
        """
        record = Path(record)  # in case it was a str
        if not cls._is_valid_record_name(record.name):
            raise ValueError(f"Invalid record name: '{record.name}'")

        globstr = f"{record.name}*{cls._FILE_EXT}"  # rough wildcard pattern
        # filter out possible false positives (i.e. foobar* matching foo* as well)
        return [
            p
            for p in record.parent.glob(globstr)
            if re.match(f"^{record.name}[^{cls._ALLOWED_NAME_CHARS}]", p.name)
        ]

    @classmethod
    def list_records(cls, dir: Path) -> List[Path]:
        """Return paths of records found in the given directory.

        Will NOT recurse into subdirectories.

        This operation is based on purely syntactic pattern matching on file names
        that follow the default naming convention (i.e. just as `find_files`).

        Returned paths can be used as-is for opening the (supposed) record.
        """
        dir = Path(dir)  # in case it was a str
        if not dir.is_dir():
            raise ValueError(f"'{dir}' is not a directory")

        ret = []
        namepat = f"[{cls._ALLOWED_NAME_CHARS}]+(?=[^{cls._ALLOWED_NAME_CHARS}])"
        for p in dir.glob(f"*{cls._FILE_EXT}"):
            if m := re.match(namepat, p.name):
                ret.append(m.group(0))
        return list(map(lambda name: dir / name, set(ret)))

    def __init__(self, record: Union[str, Path], mode: OpenMode = "r", **kwargs):
        """Open or create a record.

        This method uses `find_files` to infer the correct set of files syntactically.

        The open mode semantics are the same as for h5py.File.

        If the mode is 'r', then creating, committing or discarding patches is disabled.

        If the mode is 'a' or 'r+', then a new patch will be created in case the latest
        patch has already been committed.
        """
        super().__init__(self)

        record = Path(record)
        if mode not in _OPEN_MODES:
            raise ValueError(f"Unknown file open mode: {mode}")

        if mode[0] == "w" or mode == "x":
            # create new or overwrite to get new
            ret = self._create(record, truncate=(mode == "w"))
            self.__dict__.update(ret.__dict__)
            return

        if mode == "a" or mode[0] == "r":
            paths = self.find_files(record)

            if not paths:
                if mode != "a":  # r/r+ need existing containers
                    raise FileNotFoundError(f"No files found for record: {record}")
                else:  # a means create new if not existing (will be writable)
                    ret = self._create(record, truncate=False)
                    self.__dict__.update(ret.__dict__)
                    return

            # open existing (will be ro if everything is fine, writable if latest patch was uncommitted)
            want_rw = mode != "r"
            ret = self._open(paths, reopen_incomplete_patch=want_rw, **kwargs)
            self.__dict__.update(ret.__dict__)
            self._allow_patching = want_rw

            if want_rw and not self._has_writable:
                # latest patch was completed correctly -> make writable by creating new patch
                self.create_patch()

    @property
    def mode(self) -> Literal["r", "r+"]:
        return "r+" if self._allow_patching else "r"

    def close(self, commit: bool = True) -> None:
        """Close all files that belong to this record.

        If there exists an uncommited patch, it will be committed
        (unless `commit` is set to false).

        After this, the object may not be used anymore.
        """
        if self._closed:
            return  # nothing to do

        if self._has_writable and commit:
            self.commit_patch()
        for f in self.__files__:
            f.close()
        self.__files__ = []
        self._closed = True

    def _expect_not_ro(self):
        if self.mode == "r":
            raise ValueError("The container is opened as read-only!")

    def create_patch(self) -> None:
        """Create a new patch in order to update the record."""
        self._expect_open()
        self._expect_not_ro()
        if self._has_writable:
            raise ValueError("There already exists a writable container, commit first!")

        path = self._next_patch_filepath()
        ub = IH5UserBlock.create(prev=self._ublock(-1))
        self.__files__.append(self._new_container(path, ub))
        self._ublocks[path] = ub

    def _delete_latest_container(self) -> None:
        """Discard the current writable container (patch or base)."""
        cfile = self.__files__.pop()
        fn = cfile.filename
        del self._ublocks[Path(fn)]
        cfile.close()
        Path(fn).unlink()

    def discard_patch(self) -> None:
        """Discard the current incomplete patch container."""
        self._expect_open()
        self._expect_not_ro()
        if not self._has_writable:
            raise ValueError("No patch to discard!")
        if len(self.__files__) == 1:
            raise ValueError("Cannot discard base container! Just delete the file!")
            # reason: the base container provides record_uuid,
            # destroying it makes this object inconsistent / breaks invariants
            # so if this is done, it should not be used anymore.
        return self._delete_latest_container()

    def commit_patch(self, **kwargs) -> None:
        """Complete the current writable container (base or patch) for the record.

        Will perform checks on the new container and throw an exception on failure.

        After committing the patch is completed and cannot be edited anymore, so
        any further modifications must go into a new patch.
        """
        if kwargs:
            raise ValueError(f"Unknown keyword arguments: {kwargs}")

        self._expect_open()
        self._expect_not_ro()
        if not self._has_writable:
            raise ValueError("No patch to commit!")
        cfile = self.__files__[-1]
        filepath = Path(cfile.filename)
        cfile.close()  # must close it now, as we will write outside of HDF5 next

        # compute checksum, write user block
        chksum = hashsum_file(filepath, skip_bytes=USER_BLOCK_SIZE)
        self._ublocks[filepath].hdf5_hashsum = QualHashsumStr(chksum)
        self._ublocks[filepath].save(filepath)

        # reopen the container file now as read-only
        self.__files__[-1] = h5py.File(filepath, "r")

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

    def merge_files(self, target: Path) -> Path:
        """Given a path with a record name, merge current record into new container.

        Returns new resulting container.
        """
        self._expect_open()
        if self._has_writable:
            raise ValueError("Cannot merge, please commit or discard your changes!")

        with type(self)(target, "x") as ds:
            source_node = self["/"]
            target_node = ds["/"]
            for k, v in source_node.attrs.items():  # copy root attributes
                target_node.attrs[k] = v
            for name in source_node.keys():  # copy each entity (will recurse)
                h5_copy_from_to(source_node[name], target_node, name)

            cfile = ds.ih5_files[0]  # store filename to override userblock afterwards

        # compute new merged userblock
        ub = self._ublock(-1).copy(update={"prev_patch": self._ublock(0).prev_patch})
        # update hashsum with saved new merged hdf5 payload
        chksum = hashsum_file(cfile, skip_bytes=USER_BLOCK_SIZE)
        ub.hdf5_hashsum = QualHashsumStr(chksum)

        self._fixes_after_merge(cfile, ub)  # for subclass hooks

        self._set_ublock(-1, ub)
        ub.save(cfile)
        return cfile

    @classmethod
    def delete_files(cls, record: Path):
        """Irreversibly(!) delete all containers matching the record path.

        This object is invalid after this operation.
        """
        for file in cls.find_files(record):
            file.unlink()

    def __repr__(self):
        return f"<IH5 record (mode {self.mode}) {self.__files__}>"

    # ---- context manager support (i.e. to use `with`) ----

    def __enter__(self):
        return self

    def __exit__(self, ex_type, ex_value, ex_traceback):
        # this will ensure that commit_patch() is called and the files are closed
        self.close()
