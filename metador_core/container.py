"""Metador container."""

from __future__ import annotations

from pathlib import Path
from typing import Optional, Type

from .hashutils import DirDiff, DirHashsums, dir_hashsums, rel_symlink
from .ih5.manifest import IH5ManifestFile
from .ih5.record import IH5Record, IH5UserBlock
from .packer.util import MetadorValidationErrors


class MetadorContainer:
    """IH5-based records for the Metador platform.

    This class extends the IH5 record concept with manifest files that contain enough
    metadata in order to compute shallow update containers without having the
    original record containers available locally.

    Furthermore, it adds a layer of validation of the container creation to satisfy
    additional requirements beyond being valid IH5 containers.
    """

    _path: Path
    _record: Optional[IH5Record]
    _manifest: Optional[IH5ManifestFile]

    MANIFEST_EXT = f"{IH5Record.FILE_EXT}mf.json"

    @property
    def name(self) -> str:
        return self._path.name

    @property
    def record(self) -> Optional[IH5Record]:
        return self._record

    @property
    def manifest(self) -> Optional[IH5ManifestFile]:
        return self._manifest

    @classmethod
    def _manifest_filepath(cls, record: Path) -> Path:
        """Return filename of manifest based on prefix path of record."""
        return Path(f"{record}{cls.MANIFEST_EXT}")

    @classmethod
    def _manifest_match(cls, mf: IH5ManifestFile, ub: IH5UserBlock) -> bool:
        """Check that a manifest matches a certain patch."""
        return (
            ub.record_uuid == mf.record_uuid
            and ub.patch_uuid == mf.patch_uuid
            and ub.patch_index == mf.patch_index
        )

    @classmethod
    def open(cls, record: Path, **kwargs) -> MetadorRecord:
        """Open a record at given path.

        This method takes two mutually exclusive keyword arguments:

        If only_manifest=True, will allow to load only a manifest file.
        This can be used to write patches without having the original containers
        (if doing so, a stub container the patch is based on will be also created).

        If missing_manifest=True, will allow to load a record without manifest file.
        In this case a manifest will inferred from the record. This can be used to patch
        the record even if the original manifest file was irreversibly lost.
        The next patch in that case will be effectively a full base container,
        but will carry on the metadata of the original record.
        """
        unknown_kwargs = set(kwargs.keys()) - set(["only_manifest", "missing_manifest"])
        if unknown_kwargs:
            raise ValueError(f"Unknown kwargs: {unknown_kwargs}")

        only_manifest = kwargs.get("only_manifest", False)
        missing_manifest = kwargs.get("missing_manifest", False)
        if only_manifest and missing_manifest:
            msg = "only_manifest and missing_manifest are mutually exclusive!"
            raise ValueError(msg)

        ret = cls.__new__(cls)
        ret._path = record
        ret._manifest = None
        ret._record = None

        hsfp = cls._manifest_filepath(record)
        if not hsfp.is_file():
            if not missing_manifest:
                msg = f"Manifest file '{hsfp}' does not exist! Record incomplete!"
                raise FileNotFoundError(msg)
        else:  # load the existing manifest file
            ret._manifest = IH5ManifestFile.parse_file(hsfp)

        # if opening or reading or parsing fails, exception will be thrown
        if not only_manifest:
            ret._record = IH5Record.open(record)

        # if we have containers and also a real manifest, check that they match
        if not only_manifest and not missing_manifest:
            assert ret._manifest is not None and ret._record is not None
            if not cls._manifest_match(ret._manifest, ret._record._ublock(-1)):
                ret.record.close()
                msg = f"Manifest file '{hsfp}' does not match latest HDF5 container!"
                raise ValueError(msg)

        return ret

    def close(self):
        if self._record is not None:
            self._record.close()
        self._record = None
        self._manifest = None  # type: ignore
        self._path = None  # type: ignore

    def check_directory_common(self, data_dir: Path) -> MetadorValidationErrors:
        """Check directory constraints and invariants that are packer-independent."""
        errs = MetadorValidationErrors()
        # check symlinks inside of data directory
        for path in sorted(data_dir.rglob("*")):
            key = str(path.relative_to(data_dir))
            if path.is_symlink() and rel_symlink(data_dir, path) is None:
                errs.add(key, "Invalid out-of-data-directory symlink!")
        return errs

    def check_record_common(self) -> MetadorValidationErrors:
        """Check record constraints and invariants that are packer-independent."""
        assert self.record is not None
        errs = MetadorValidationErrors()
        # TODO: check presence of env meta, packer name + container version
        # and check consistency of TOC

        # errs.append(PackerMeta.check_path(PACKER_META_PATH, self.record))
        # errs.append(TOCMeta.check_path(TOC_META_PATH, self.record))
        return errs

    def check_directory(
        self, data_dir: Path, packer: Optional[Type[MetadorPacker]]
    ) -> MetadorValidationErrors:
        """Check the structure of the directory.

        Will always perform the packer-independent checks. If a packer is provided,
        will also run the packer-specific directory checks.
        """
        errs = self.check_directory_common(data_dir)
        if packer is not None:
            errs.append(packer.check_directory(data_dir))
        return errs

    def check_record(
        self, packer: Optional[Type[MetadorPacker]]
    ) -> MetadorValidationErrors:
        """Check the structure of the record.

        Will always perform the packer-independent checks. If a packer is provided,
        will also run the packer-specific record checks.
        """
        assert self.record is not None
        errs = self.check_record_common()
        if packer is not None:
            errs.append(packer.check_record(self.record))
        return errs

    def check_packer_compatible(self, packer: Type[MetadorPacker]):
        """Check whether the packer is compatible with the record."""
        pmeta: Optional[PackerMeta] = None
        if self.manifest is not None:
            pmeta = self.manifest.packer
        else:
            assert self.record is not None
            if not self.record.is_empty:
                pmeta = PackerMeta.from_record(self.record, PACKER_META_PATH)
        if pmeta is None:
            return  # no manifest, empty record -> fresh dataset
        if not (
            packer.PACKER_ID == pmeta.id
            and packer.PACKER_VERSION[0] == pmeta.version[0]
            and packer.PACKER_VERSION[1] >= pmeta.version[1]
        ):
            needs = f"{pmeta.id}>={pmeta.version}"
            raise ValueError(f"Incompatible packer, needs {needs}!")

    @classmethod
    def create(
        cls, target: Path, data_dir: Path, packer: Type[MetadorPacker], **kwargs
    ) -> MetadorRecord:
        """Create a fresh record.

        Will create an IH5 record + manifest file with names based on `target`
        and pack contents of `data_dir` using provided `packer` class.
        """
        if not data_dir.is_dir():
            raise ValueError(f"Invalid data directory: '{data_dir}'")
        if not any(data_dir.iterdir()):
            raise ValueError(f"Data directory is empty: '{data_dir}'")

        overwrite = kwargs.get("overwrite", False)

        ret = cls.__new__(cls)
        ret._path = target
        # fresh IH5 record
        ret._record = IH5Record.create(target, overwrite=overwrite)
        ret._manifest = None  # will be created by update
        # call packer. creation = "update" from empty record
        ret.update(data_dir, packer)
        return ret

    def finalize_header(self, packer: Type[MetadorPacker]):
        """Create common derivative metadata structures in header."""
        assert self._record is not None
        if PACKER_META_PATH in self._record:
            del self._record[PACKER_META_PATH]
        self._record[PACKER_META_PATH] = packer.packer_meta().json()

        if TOC_META_PATH in self._record:
            del self._record[TOC_META_PATH]
        self._record[TOC_META_PATH] = TOCMeta.for_record_body(self._record).json()

    def update(
        self, data_dir: Path, packer: Type[MetadorPacker], allow_unchanged: bool = False
    ):
        """Update a record by writing a patch.

        Will create a new patch and update (overwrite!) the manifest file.
        If record is already writable, will use the current patch.

        If the record is manifest-only and the record is missing,
        this will also create a stub container that is based on the
        structural skeleton embedded in the initial manifest file.

        If there is a record, but no manifest, the resulting patch will be
        essentially a fresh record, but have the metadata of an updated version.

        The update is created using given `packer` class on the `data_dir`.
        """
        # if anything goes wrong, we can restore the old manifest later
        old_manifest = self._manifest
        # if there is no manifest provided, we take an empty hashsum list
        missing_manifest = True
        old_hashsums: DirHashsums = {}
        if old_manifest is not None:
            missing_manifest = False
            old_hashsums = old_manifest.hashsums

        # check that the directory is suitable for the packer
        try:
            errs = self.check_directory(data_dir, packer)
            if errs:
                raise errs
        except Exception as e:  # in case of any exception, cleanup and abort
            if self._record and not self._record.read_only:  # fresh record or patch?
                self._record._delete_latest_container()  # kill it
            raise e

        # create a stub for the patch, if containers are missing
        if self._record is None:
            assert self._manifest is not None
            st_ub = self._manifest.to_userblock()  # userblock for the stub
            self._record = create_stub_base(self._path, st_ub, self._manifest.skeleton)
        assert self._record is not None

        fresh = True  # initially assume that we are building a fresh record
        if self._record.read_only:
            # read-only can only mean that we opened an existing record (or stub)
            # so we are doing a non-trivial patch -> not fresh, must create patch
            fresh = False
            self._record.create_patch()
        if missing_manifest:  # if there was no manifest provided -> clear record
            self._record._clear()  # so the packer can treat it like a fresh record
            fresh = True  # reset to true (previous if could have been executed)
        assert fresh == self._record.is_empty

        # check that the packer of dataset is compatible with passed packer
        # (based on packer info in manifest or in the record)
        try:
            self.check_packer_compatible(packer)
        except ValueError as e:
            self._record._delete_latest_container()  # kill the new base or patch
            raise e

        # when patching a record, ensure that record is valid according to packer
        if not fresh:
            errs = self.check_record(packer)
            if errs:
                self._record._delete_latest_container()  # kill the new base or patch
                raise errs

        # prepare new manifest file
        self._manifest = IH5ManifestFile.from_userblock(self._record.ih5_meta[-1])
        # compute hashsums and diff of directory (the packer will get the diff)
        self._manifest.hashsums = dir_hashsums(data_dir)
        diff = DirDiff.compare(old_hashsums, self._manifest.srcdir_hashsums)
        if diff.is_empty and not allow_unchanged:
            # kill the new patch, restore old state
            self._record._delete_latest_container()
            self._manifest = old_manifest
            raise ValueError(f"No changes detected in '{data_dir}', aborting patch!")

        assert not self._record.read_only
        try:
            # run packer... which must run through without throwing exceptions
            packer.pack_directory(data_dir, diff, self._record, fresh)

            # do common finalization of header part of container
            self.finalize_header(packer)

            # verify general container constraints that all packers must satisfy
            errs = self.check_record(packer)
            if errs:
                raise errs
        except Exception as e:
            # kill the new patch, restore old state
            self._record._delete_latest_container()
            self._manifest = old_manifest
            raise e

        # if execution was not interrupted by now, we can commit the packed changes
        assert not self._record.read_only  # expect that packer did not commit
        self._record.commit()
        assert self._record.read_only

        # update skeleton with resulting record
        self._manifest.skeleton = ih5_skeleton(self._record)
        # as everything is fine, finally (over)write manifest file on disk...
        with open(self._manifest_filepath(self._path), "w") as f:
            f.write(self._manifest.json(indent=2))
            f.flush()
        # success - this object is now the updated record

    # ---- context manager support (i.e. to use `with`) ----

    def __enter__(self):
        return self

    def __exit__(self, ex_type, ex_value, ex_traceback):
        # this will ensure that commit() is called and the files are closed
        self.close()
