"""Ardiem dataset."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional
from uuid import UUID, uuid1

from pydantic import BaseModel

from .ih5.dataset import (
    HASH_ALG,
    IH5Dataset,
    IH5UserBlock,
    create_stub_base,
    ih5_skeleton,
)
from .packer import ArdiemPacker
from .util import DirDiff, DirHashsums, ValidationErrors, dir_hashsums

# TODO: should the manifest file be hashed itself,
# and listed as sidecar as "sidecar UUID -> hashsum" in user block?
# that way a container could recognize its "original" manifest file

# TODO: common check_directory method for standardized common metadata?


class ValidationError(ValueError):
    """Exception when dataset validation fails."""

    pass


def _join_errs(errs: ValidationErrors, more_errs: ValidationErrors) -> ValidationErrors:
    for k, v in more_errs.items():
        if k not in errs:
            errs[k] = v
        else:
            errs[k] += v
    return errs


class ManifestFile(BaseModel):
    """A hashsum file, which is a sidecar file of a collection of IH5 dataset files.

    It collects the current hashsums for all files in the directory
    for the state of the data at the time of the creation of a certain patch.

    It is used to be able to create a patch container updating a dataset
    without having access to the predecessor containers.

    Packer plugins can figure out what needs to be included in the patch
    based on the changes in the raw unpacked data directory.
    """

    # uuid for the hashsum file itself (so the filename does not matter)
    sidecar_uuid: UUID

    # should be equal to those of the patch this hashsum file is based on
    dataset_uuid: UUID
    patch_uuid: UUID
    patch_index: int

    hashsums: DirHashsums  # computed with dir_hashsums
    skeleton: Dict[str, str]  # computed with ih5_skeleton

    @classmethod
    def create(cls, ub: IH5UserBlock) -> ManifestFile:
        """Create a manifest file based on a user block.

        Hashsums and skeleton must still be added to make the instance complete.
        """
        return cls(
            sidecar_uuid=uuid1(),
            dataset_uuid=ub.dataset_uuid,
            patch_uuid=ub.patch_uuid,
            patch_index=ub.patch_index,
            hashsums={},
            skeleton={},
        )

    def to_userblock(self) -> IH5UserBlock:
        """Return a stub userblock based on information stored in manifest."""
        return IH5UserBlock(
            patch_uuid=self.patch_uuid,
            dataset_uuid=self.dataset_uuid,
            patch_index=self.patch_index,
            prev_patch=None,
            hdf5_hashsum=f"{HASH_ALG}:toBeComputed",
            is_stub=True,
        )


class ArdiemDataset:
    """IH5-based datasets for the Ardiem platform.

    This class extends the IH5 dataset concept with manifest files that contain enough
    metadata in order to compute shallow update containers without having the
    original dataset containers available locally.

    Furthermore, it adds a layer of validation of the container creation to satisfy
    additional requirements beyond being valid IH5 containers.
    """

    _name: Path
    _dataset: Optional[IH5Dataset]
    _manifest: Optional[ManifestFile]

    MANIFEST_EXT = f"{IH5Dataset.FILE_EXT}mf.json"

    @property
    def name(self) -> Path:
        return self._name

    @property
    def dataset(self) -> Optional[IH5Dataset]:
        return self._dataset

    @property
    def manifest(self) -> Optional[ManifestFile]:
        return self._manifest

    @classmethod
    def _manifest_filepath(cls, dataset: Path) -> Path:
        """Return filename of manifest based on prefix path of dataset."""
        return Path(f"{dataset}{cls.MANIFEST_EXT}")

    @classmethod
    def _manifest_match(cls, mf: ManifestFile, ub: IH5UserBlock) -> bool:
        """Check that a manifest matches a certain patch."""
        return (
            ub.dataset_uuid == mf.dataset_uuid
            and ub.patch_uuid == mf.patch_uuid
            and ub.patch_index == mf.patch_index
        )

    @classmethod
    def open(cls, dataset: Path, **kwargs) -> ArdiemDataset:
        """Open a dataset at given path.

        This method takes two mutually exclusive keyword arguments:

        If only_manifest=True, will allow to load only a manifest file.
        This can be used to write patches without having the original containers
        (if doing so, a stub container the patch is based on will be also created).

        If missing_manifest=True, will allow to load a dataset without manifest file.
        In this case a manifest will inferred from the dataset. This can be used to patch
        the dataset even if the original manifest file was irreversibly lost.
        The next patch in that case will be effectively a full base container,
        but will carry on the metadata of the original dataset.
        """
        only_manifest = kwargs.get("only_manifest", False)
        missing_manifest = kwargs.get("missing_manifest", False)
        if only_manifest and missing_manifest:
            msg = "only_manifest and missing_manifest are mutually exclusive!"
            raise ValueError(msg)

        ret = cls.__new__(cls)
        ret._name = dataset
        ret._manifest = None
        ret._dataset = None

        hsfp = cls._manifest_filepath(dataset)
        if not hsfp.is_file():
            if not missing_manifest:
                msg = f"Manifest file '{hsfp}' does not exist! Dataset incomplete!"
                raise ValidationError(msg)
        else:  # load the existing manifest file
            ret._manifest = ManifestFile.parse_file(hsfp)

        # if opening or reading or parsing fails, exception will be thrown
        if not only_manifest:
            ret._dataset = IH5Dataset.open(dataset)

        # if we have containers and also a real manifest, check that they match
        if not only_manifest and not missing_manifest:
            assert ret._manifest is not None and ret._dataset is not None
            if not cls._manifest_match(ret._manifest, ret._dataset._ublock(-1)):
                ret.dataset.close()
                msg = f"Manifest file '{hsfp}' does not match latest HDF5 container!"
                raise ValidationError(msg)

        return ret

    def close(self):
        if self._dataset is not None:
            self._dataset.close()
        self._dataset = None
        self._manifest = None  # type: ignore
        self._name = None  # type: ignore

    def check_dataset_common(self) -> ValidationErrors:
        """Check dataset constraints and invariants that are packer-independent."""
        assert self.dataset is not None
        return {}

    def check_directory_common(self, data_dir: Path) -> ValidationErrors:
        """Check directory constraints and invariants that are packer-independent."""
        return {}

    def check_directory(
        self, data_dir: Path, packer: Optional[ArdiemPacker]
    ) -> ValidationErrors:
        """Check the structure of the directory.

        Will always perform the packer-independent checks. If a packer is provided,
        will also run the packer-specific directory checks.
        """
        errs = self.check_directory_common(data_dir)
        if packer is not None:
            _join_errs(errs, packer.check_directory(data_dir))
        return errs

    def check_dataset(self, packer: Optional[ArdiemPacker]) -> ValidationErrors:
        """Check the structure of the dataset.

        Will always perform the packer-independent checks. If a packer is provided,
        will also run the packer-specific dataset checks.
        """
        assert self.dataset is not None
        errs = self.check_dataset_common()
        if packer is not None:
            _join_errs(errs, packer.check_dataset(self.dataset))
        return errs

    @classmethod
    def create(
        cls, target: Path, data_dir: Path, packer: ArdiemPacker
    ) -> ArdiemDataset:
        """Create a fresh dataset.

        Will create an IH5 dataset + manifest file with names based on `target`
        and pack contents of `data_dir` using provided `packer` class.
        """
        ret = cls.__new__(cls)
        ret._name = target
        ret._dataset = IH5Dataset.create(target)  # fresh IH5 dataset
        ret._manifest = None
        # call packer. creation = "update" from empty dataset
        ret.update(data_dir, packer)
        return ret

    def update(self, data_dir: Path, packer: ArdiemPacker):
        """Update a dataset by writing a patch.

        Will create a new patch and update (overwrite!) the manifest file.
        If dataset is already writable, will use the current patch.

        If the dataset is manifest-only and the dataset is missing,
        this will also create a stub container that is based on the
        structural skeleton embedded in the initial manifest file.

        If there is a dataset, but no manifest, the resulting patch will be
        essentially a fresh dataset, but have the metadata of an updated version.

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
        errs = self.check_directory(data_dir, packer)
        if errs:
            if self._dataset and not self._dataset.read_only:  # fresh dataset?
                self._dataset._delete_latest_container()  # destroy it completely
            raise ValidationError(errs)

        # create a stub for the patch, if containers are missing
        if self._dataset is None:
            assert self._manifest is not None
            st_ub = self._manifest.to_userblock()  # userblock for the stub
            self._dataset = create_stub_base(self.name, st_ub, self._manifest.skeleton)
        assert self._dataset is not None

        fresh = True  # initially assume that we are building a fresh dataset
        if self._dataset.read_only:
            # read-only can only mean that we opened an existing dataset (or stub)
            # so we are doing a non-trivial patch -> not fresh, must create patch
            fresh = False
            self._dataset.create_patch()
        if missing_manifest:  # if there was no manifest provided -> clear dataset
            self._dataset._clear()  # so the packer can treat it like a fresh dataset
            fresh = True  # reset to true (previous if could have been executed)
        assert fresh == self._dataset.is_empty

        # when patching a dataset, ensure that dataset is valid according to packer
        if not fresh:
            errs = self.check_dataset(packer)
            if errs:
                self._dataset._delete_latest_container()  # kill the new base or patch
                raise ValidationError(errs)

        # prepare new manifest file
        self._manifest = ManifestFile.create(self._dataset.ih5meta[-1])
        # compute hashsums and diff of directory (the packer will get the diff)
        self._manifest.hashsums = dir_hashsums(data_dir, HASH_ALG)
        diff = DirDiff.compare(old_hashsums, self._manifest.hashsums)
        assert diff is not None  # it must always return a top-level diff object

        assert not self._dataset.read_only
        try:
            # run packer... which must run through without throwing exceptions
            packer.pack_directory(data_dir, diff, self._dataset, fresh)
            # verify general container constraints that all packers must satisfy
            errs = self.check_dataset(packer)
            if errs:
                raise ValidationError(errs)
        except Exception as e:
            # kill the new patch, restore old state
            self._dataset._delete_latest_container()
            self._manifest = old_manifest
            raise e

        # if execution was not interrupted by now, we can commit the packed changes
        assert not self._dataset.read_only  # expect that packer did not commit
        self._dataset.commit()
        assert self._dataset.read_only

        # update skeleton with resulting dataset
        self._manifest.skeleton = ih5_skeleton(self._dataset)
        # as everything is fine, finally (over)write manifest file on disk...
        with open(self._manifest_filepath(self.name), "w") as f:
            f.write(self._manifest.json())
            f.flush()
        # success - this object is now the updated dataset
