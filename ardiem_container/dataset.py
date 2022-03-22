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

# TODO: should the hashsum file be hashed itself
# and listed as sidecar as "sidecar UUID -> hashsum" in user block?
# that way a container knows its "original" manifest file


class ValidationError(ValueError):
    """Exception when dataset validation fails."""

    pass


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

    hashsums: Optional[DirHashsums]  # computed with dir_hashsums
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
            hashsums=None,
            skeleton={},
        )

    @classmethod
    def infer(cls, ds: IH5Dataset) -> ManifestFile:
        """Create a manifest file based on a IH5 dataset.

        Like `create`, but will create a skeleton, and have dummy hashsum dict.
        This means, we pretend that the original data directory was "empty".
        This should make the next patch create a possibly redundant, but
        at least correct and complete container (i.e. as if it was a fresh dataset).
        """
        ret = cls.create(ds.ih5meta[-1])
        ret.skeleton = ih5_skeleton(ds)
        ret.hashsums = {}  # says "data dir was empty" so next patch adds all data anew
        return ret

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
    _manifest: ManifestFile

    MANIFEST_EXT = ".manifest.json"

    @property
    def name(self) -> Path:
        return self._name

    @property
    def dataset(self) -> Optional[IH5Dataset]:
        return self._dataset

    @property
    def manifest(self) -> ManifestFile:
        return self._manifest

    @classmethod
    def _manifest_filepath(cls, dataset: Path) -> Path:
        return Path(f"{dataset}{cls.MANIFEST_EXT}")

    @classmethod
    def open(cls, dataset: Path, **kwargs) -> ArdiemDataset:
        """Open a dataset at given path.

        This method takes two mutually exclusive keyword arguments:

        If only_manifest=True, will allow to load only a manifest file.
        This can be used to write patches without having the original containers
        (if doing so, a stub container the patch is based on will be also created).

        If infer_manifest=True, will allow to load a dataset without manifest file
        and will infer a valid manifest from the dataset. This can be used to patch
        the dataset even if the original manifest file was irreversibly lost.
        The next patch in that case will be effectively a full base container,
        but will carry on the metadata of the original dataset.
        """
        only_manifest = kwargs.get("only_manifest", False)
        infer_manifest = kwargs.get("infer_manifest", False)
        if only_manifest and infer_manifest:
            raise ValueError("only_manifest and infer_manifest are mutually exclusive!")

        ret = cls.__new__(cls)
        ret._name = dataset
        ret._manifest = None
        ret._dataset = None

        hsfp = cls._manifest_filepath(dataset)
        if not hsfp.is_file():
            if not infer_manifest:
                raise ValidationError(
                    f"Manifest file '{hsfp}' does not exist! Dataset incomplete!"
                )
        else:  # load the existing manifest file
            ret._manifest = ManifestFile.parse_file(hsfp)

        # if opening or reading or parsing fails, exception will be thrown
        if not only_manifest:
            ret._dataset = IH5Dataset.open(dataset)

        # now infer usable manifest based on dataset files if we haven't one already
        if ret._manifest is None and infer_manifest:
            ret._manifest = ManifestFile.infer(ret.dataset)

        # if we have both containers and a manifest, check that they match
        if not only_manifest:
            if (
                ret.dataset._ublock(-1).dataset_uuid != ret.manifest.dataset_uuid
                or ret.dataset._ublock(-1).patch_uuid != ret.manifest.patch_uuid
            ):
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

    def check_common(self) -> ValidationErrors:
        """Check dataset constraints and invariants that are packer-independent."""
        assert self.dataset is not None
        # return {"/": ["fail"]}
        return {}

    def check(self, packer: Optional[ArdiemPacker]) -> ValidationErrors:
        """Check the structure of the dataset.

        Will always perform the packer-independent checks. If a packer is provided,
        will also run the packer-specific dataset checks.
        """
        assert self.dataset is not None
        errs = self.check_common()
        if packer is not None:
            for k, v in packer.check_dataset(self.dataset).items():
                if k not in errs:
                    errs[k] = v
                else:
                    errs[k] += v
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
        # create new manifest file based on new container
        ret._manifest = ManifestFile.create(ret._dataset.ih5meta[-1])
        # call packer. creation = update from empty dataset
        ret.update(data_dir, packer)
        return ret

    def update(self, data_dir: Path, packer: ArdiemPacker) -> ArdiemDataset:
        """Update a dataset by writing a patch.

        Will create a new patch and update (overwrite!) the manifest file.
        If dataset is already writable, will use the current patch.

        If the dataset is manifest-only and the dataset is missing,
        this will also create a stub container that is based on the
        structural skeleton embedded in the initial manifest file.

        The update is created using given `packer` class on the `data_dir`.
        """
        # check that the directory is suitable for the packer
        errs = packer.check_directory(data_dir)
        if errs:
            raise ValidationError(errs)

        # create a stub for the patch, if containers are missing
        if self._dataset is None:
            st_ub = self._manifest.to_userblock()  # userblock for the stub
            self._dataset = create_stub_base(self.name, st_ub, self._manifest.skeleton)

        assert self._dataset is not None
        if self._dataset.read_only:
            self._dataset.create_patch()
            self._manifest = ManifestFile.create(self._dataset.ih5meta[-1])

        # compute hashsums and diff of directory (the packer will get the diff)
        old_hashsums = self._manifest.hashsums
        self._manifest.hashsums = dir_hashsums(data_dir, HASH_ALG)
        diff = DirDiff.compare(old_hashsums, self._manifest.hashsums)

        # run packer... which must run through without throwing exceptions
        try:
            packer.pack_directory(data_dir, self._dataset, diff)
        except Exception as e:
            self._dataset._delete_latest_container()
            raise e

        # verify general container constraints that all packers must satisfy
        errs = self.check(packer)
        if errs:
            self._dataset._delete_latest_container()
            raise ValidationError(errs)

        # if execution was not interrupted by now, we can commit the packed changes
        self._dataset.commit()
        assert self._dataset.read_only

        # update skeleton with resulting dataset
        self._manifest.skeleton = ih5_skeleton(self._dataset)

        # as everything is fine, finally (over)write manifest file...
        with open(self._manifest_filepath(self.name), "w") as f:
            f.write(self._manifest.json())
            f.flush()
        return self  # ...and return the packed dataset
