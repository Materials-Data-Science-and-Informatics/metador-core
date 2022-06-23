"""Sidecar JSON file storing a skeleton to create stubs and patch containers."""
from __future__ import annotations

from io import BytesIO
from pathlib import Path
from typing import Dict, List, Optional, Union
from uuid import UUID, uuid1

from pydantic import BaseModel

from ..hashutils import DEF_HASH_ALG, DirHashsums, dir_hashsums, qualified_hashsum
from ..packer.interface import PackerInfo
from ..schema.types import hashsum_str
from .record import USER_BLOCK_SIZE, IH5Record, IH5UserBlock, hashsum_file
from .skeleton import ih5_skeleton, init_stub_container

# TODO: should the manifest file be hashed itself,
# and listed as sidecar as "sidecar UUID -> hashsum" in user block?
# that way a container could recognize its "original" manifest file


class IH5ManifestFile(BaseModel):
    """A metadata sidecar file for a collection of IH5 record files.

    It contains the skeleton of the container, in order to be able to create a
    patch for an IH5Record the data is not locally available for (but manifest is).

    For IH5 containers created by a packer plugin from a well-defined directory
    it also collects the current hashsums for all files in the source directory
    at the time of the creation of a certain patch, and info about the used packer.

    The correct packer plugin can figure out what needs to be included in a patch
    based on the changes in the raw unpacked data directory (that must be available).
    """

    # uuid for the manifest file itself (so the filename does not matter)
    manifest_uuid: UUID

    # should be equal to those of the patch this hashsum file is based on
    # (from the patch user block)
    record_uuid: UUID
    patch_uuid: UUID
    patch_index: int

    skeleton: Dict[str, str]  # computed with ih5_skeleton, used to create a stub

    # relevant for packers:
    srcdir_hashsums: DirHashsums = {}  # if set, computed with dir_hashsums
    srcdir_packer: Optional[PackerInfo] = None  # if set, metadata of the used packer

    @classmethod
    def from_userblock(cls, ub: IH5UserBlock) -> IH5ManifestFile:
        """Create a manifest file based on a user block.

        Hashsums and skeleton must still be added to make the instance complete.
        """
        return cls(
            manifest_uuid=uuid1(),
            record_uuid=ub.record_uuid,
            patch_uuid=ub.patch_uuid,
            patch_index=ub.patch_index,
            skeleton={},
            srcdir_hashsums={},
            srcdir_packer=None,
        )

    def to_userblock(self) -> IH5UserBlock:
        """Return a stub userblock based on information stored in manifest."""
        return IH5UserBlock(
            patch_uuid=self.patch_uuid,
            record_uuid=self.record_uuid,
            patch_index=self.patch_index,
            prev_patch=None,
            hdf5_hashsum="INVALID:0",  # to be computed on commit()
            is_stub=True,
            ext={"manifest_uuid": self.manifest_uuid},
        )


MF_EXT_NAME: str = "manifest"
"""Name of user block extension section for stub and manifest info."""


class IH5UBExtManifest(BaseModel):
    """IH5 user block extension for stub and manifest support."""

    is_stub_container: bool = False
    """True if file has the structure of another container, without actual data inside."""

    manifest_uuid: Optional[UUID] = None
    """UUID of the manifest file that belongs to this IH5 file."""

    manifest_hashsum: Optional[hashsum_str] = None
    """Hashsum of the manifest file that belongs to this IH5 file."""

    @classmethod
    def from_userblock(cls, ub: IH5UserBlock) -> Optional[IH5UBExtManifest]:
        """Parse extension metadata from userblock, if it is available."""
        if MF_EXT_NAME not in ub.exts:
            return None
        return cls.parse_obj(ub.exts[MF_EXT_NAME])

    def into_userblock(self, ub: IH5UserBlock):
        """Create or overwrite extension metadata in given userblock."""
        ub.exts[MF_EXT_NAME] = self.dict()


def create_stub_base(
    record: Union[Path, str],
    ub: IH5UserBlock,
    skel: Dict[str, str],
) -> IH5Record:
    """Create a stub base container for a record.

    The stub is based on the latest user block of a real IH5 record
    and the skeleton of the overlay structure (as returned by `ih5_skeleton`).

    Patches created on top of the stub are compatible with the original record
    whose metadata the stub is based on.
    """
    ds = IH5Record.create(Path(record))
    init_stub_container(ds, skel)

    # fix user block to make it appear as valid base container
    new_ub = ub.copy(update={"prev_patch": None})
    new_ub.exts[MF_EXT_NAME] = IH5UBExtManifest(is_stub_container=True).dict()
    # overwrite user block of fresh record, mark it as a base container stub
    ds._ublocks[Path(ds._files[0].filename)] = new_ub
    return ds


class IH5MFRecord(IH5Record):
    """IH5Record extended by a manifest file.

    The manifest file contains enough information to support the creation
    of a stub container and patching a dataset without having the actual container.

    The intended use case of this IH5 variant is to be created by automated packers
    from suitably prepared source directories, to be uploaded to remote locations,
    and for these packers being able to create patches for the record without
    access to all the data containers (based only on the most recent manifest file).
    """

    MANIFEST_EXT: str = "mf.json"

    _manifest: IH5ManifestFile
    """Manifest of newest loaded container file."""

    @property
    def manifest(self) -> IH5ManifestFile:
        """Manifest file of latest committed record patch."""
        return self._manifest

    @classmethod
    def _manifest_filepath(cls, record: str) -> Path:
        """Return filename of manifest based on path of a container file."""
        return Path(f"{str(record)}{cls.MANIFEST_EXT}")

    # Override to also load and check latest manifest
    def __init__(self, paths: List[Path], **kwargs):
        manifest_file: Path = kwargs.pop("manifest_file", None)
        super().__init__(paths, **kwargs)

        # if not given explicitly, infer correct manifest filename
        # based on logically latest container (they are sorted after parent init)
        if manifest_file is None:
            manifest_file = self._manifest_filepath(self._files[-1].filename)

        # for latest container, check linked manifest against given/inferred one
        ub = self._ublock(-1)
        ubext = IH5UBExtManifest.from_userblock(ub)

        # check that manifest file exists and has correct hashsum
        chksum = hashsum_file(manifest_file)
        if ubext.manifest_hashsum != chksum:
            msg = "Manifest has been modified, unexpected hashsum!"
            raise ValueError(f"{ub._filename}: {msg}")

        # check if latest user block and the manifest agree
        self._manifest = IH5ManifestFile.parse_file(manifest_file)
        if ubext.manifest_uuid != self._manifest.manifest_uuid:
            raise ValueError(f"{ub._filename}: Manifest file has wrong UUID!")

    # Override to also check user block extension
    def _check_ublock(self, ub: IH5UserBlock, prev: Optional[IH5UserBlock] = None):
        super()._check_ublock(ub, prev)

        # We expect to find additional info in the userblock
        ubext = IH5UBExtManifest.from_userblock(ub)
        if ubext is None:
            raise ValueError(f"{ub._filename}: Missing manifest extension metadata!")

        # we only allow to write patches on top of stubs,
        # but not have stubs on top of something else.
        if prev is not None and ubext.is_stub_container:
            msg = "Found stub patch container, only base container may be a stub!"
            raise ValueError(f"{ub._filename}: {msg}")

    # Override to prevent merge if a stub is present
    def merge(self, target: Path) -> Path:
        def is_stub(x):
            return IH5UBExtManifest.from_userblock(x).is_stub_container

        if any(map(is_stub, self.ih5_meta)):
            raise ValueError("Cannot merge, files contain a stub!")

        # the following creates a new manifest file,
        # but will not have hashsums or packer info!
        # packers should not need "merge" anyway, this is something for end-users.

        # TODO: is this ok? otherwise have to:
        # need to read latest manifest,
        # copy it over as the manifest for the new target,
        # and change the user block in the target, updating the manifest uuid and hashsum
        return super().merge(target)

    def _make_manifest(
        self, src_dir: Optional[Path] = None, packer_info: Optional[PackerInfo] = None
    ) -> IH5ManifestFile:
        """Return new manifest based on current state of the record."""
        mf = IH5ManifestFile.from_userblock(self._ublock(-1))
        mf.skeleton = ih5_skeleton(self)
        if src_dir is not None:
            mf.srcdir_hashsums = dir_hashsums(src_dir)
        if packer_info is not None:
            mf.srcdir_packer = packer_info
        return mf

    # Override to create skeleton and dir hashsums, write manifest and add to user block
    def commit(self, **kwargs) -> None:
        src_dir = kwargs.get("source_dir")
        packer_info = kwargs.get("packer_info")
        mf = self._make_manifest(src_dir, packer_info)

        # convert to bytes already to compute hashsum (these bytes will go to file)
        # add a newline, as otherwise behaviour with text editors will be confusing
        # (e.g. vim automatically adds a trailing newline that it hides)
        # https://stackoverflow.com/questions/729692/why-should-text-files-end-with-a-newline
        mf_json: bytes = (mf.json(indent=2) + "\n").encode("utf-8")
        mf_hashsum: str = qualified_hashsum(BytesIO(mf_json))

        # prepare new user block that links to the prospective manifest
        old_ub = self._ublock(-1)
        new_ub = old_ub.copy()
        new_ub.exts[MF_EXT_NAME] = IH5UBExtManifest(
            is_stub_container=False,  # stubs are not supposed to be created like this!
            manifest_uuid=mf.manifest_uuid,
            manifest_hashsum=mf_hashsum,
        ).dict()
        self._set_ublock(-1, new_ub)

        # try writing
        try:
            super().commit()
        except ValueError as e:  # some checks failed
            self._set_ublock(-1, old_ub)
            raise e

        # as everything is fine, finally (over)write manifest here and on disk
        self._manifest = mf
        with open(self._manifest_filepath(self._files[-1].filename), "wb") as f:
            f.write(mf_json)
            f.flush()
