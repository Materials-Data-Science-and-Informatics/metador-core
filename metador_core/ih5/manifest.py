"""Sidecar JSON file storing a skeleton to create stubs and patch containers."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from uuid import UUID, uuid1

from pydantic import BaseModel

from ..schema.types import QualHashsumStr
from ..util.hashsums import qualified_hashsum
from .record import IH5Record, IH5UserBlock, hashsum_file
from .skeleton import IH5Skeleton, init_stub_base


class IH5Manifest(BaseModel):
    """A metadata sidecar file for a collection of IH5 record files.

    It contains the skeleton of the container, in order to be able to create a
    patch for an IH5Record the data is not locally available for (but manifest is).
    """

    # uuid for the manifest file itself (so the filename does not matter)
    manifest_uuid: UUID

    user_block: IH5UserBlock  # copy of user block (without the manifest extension part)

    skeleton: IH5Skeleton  # computed with IH5Skeleton, used to create a stub

    manifest_exts: Dict[str, Any]  # Arbitrary extensions, similar to IH5UserBlock

    @classmethod
    def from_userblock(cls, ub: IH5UserBlock, skeleton={}, exts={}) -> IH5Manifest:
        """Create a manifest file based on a user block."""
        ub_copy = ub.copy()
        # only keep other extensions (otherwise its circular)
        ub_copy.ub_exts = {
            k: v for k, v in ub.ub_exts.items() if k != IH5UBExtManifest.ext_name()
        }
        return cls(
            manifest_uuid=uuid1(),
            user_block=ub_copy,
            skeleton=skeleton,
            manifest_exts=exts,
        )

    def __bytes__(self) -> bytes:
        """Serialize to JSON and return UTF-8 encoded bytes to be written in a file."""
        # add a newline, as otherwise behaviour with text editors will be confusing
        # (e.g. vim automatically adds a trailing newline that it hides)
        # https://stackoverflow.com/questions/729692/why-should-text-files-end-with-a-newline
        return (self.json(indent=2) + "\n").encode(encoding="utf-8")

    def save(self, path: Path):
        """Save manifest (as returned by bytes()) into a file."""
        with open(path, "wb") as f:
            f.write(bytes(self))
            f.flush()


class IH5UBExtManifest(BaseModel):
    """IH5 user block extension for stub and manifest support."""

    is_stub_container: bool
    """True if file has the structure of another container, without actual data inside."""

    manifest_uuid: UUID
    """UUID of the manifest file that belongs to this IH5 file."""

    manifest_hashsum: QualHashsumStr
    """Hashsum of the manifest file that belongs to this IH5 file."""

    @classmethod
    def ext_name(cls) -> str:
        """Name of user block extension section for stub and manifest info."""
        return "ih5mf_v01"

    @classmethod
    def get(cls, ub: IH5UserBlock) -> Optional[IH5UBExtManifest]:
        """Parse extension metadata from userblock, if it is available."""
        if cls.ext_name() not in ub.ub_exts:
            return None
        return cls.parse_obj(ub.ub_exts[cls.ext_name()])

    def update(self, ub: IH5UserBlock):
        """Create or overwrite extension metadata in given userblock."""
        ub.ub_exts[self.ext_name()] = self.dict()


class IH5MFRecord(IH5Record):
    """IH5Record extended by a manifest file.

    The manifest file is a sidcar JSON file that contains enough information to support
    the creation of a stub container and patching a dataset without having the actual
    container locally available.

    In a chain of container files, only the base container may be a stub.
    All files without the manifest extension in the userblock are considered not stubs.

    An IH5MFRecord is a valid IH5Record (the manifest file then is simply ignored).
    Also, it is possible to open an IH5Record as IH5MFRecord and turn it into a
    valid IH5MFRecord by committing a patch (this will create the missing manifest).

    In addition to the ability to create stubs, the manifest file can be used to carry
    information that should be attached to a container, but is too large or inappropriate
    for storage in the userblock (e.g. should be available separately).

    The manifest should store information that applies semantically to the whole fileset
    at the current patch level, it MUST NOT be required to have manifest files for each
    ih5 patch. Additional information stored in the manifest is inherited to the
    manifest of successive patches until overridden.

    The main use case of this extension is to be used by automated packers
    from suitably prepared source directories, to be uploaded to remote locations,
    and for these packers being able to create patches for the record without
    access to all the data containers (based only on the most recent manifest file).
    """

    MANIFEST_EXT: str = "mf.json"

    _manifest: Optional[IH5Manifest] = None
    """Manifest of newest loaded container file (only None for new uncommited records)."""

    @property
    def manifest(self) -> IH5Manifest:
        """Return loaded manifest object of latest committed record patch."""
        if self._manifest is None:  # should only happen with fresh create()d records
            raise ValueError("No manifest exists yet! Did you forget to commit?")
        return self._manifest

    def _fresh_manifest(self) -> IH5Manifest:
        """Return new manifest based on current state of the record."""
        ub = self._ublock(-1)
        skel = IH5Skeleton.for_record(self)
        return IH5Manifest.from_userblock(ub, skeleton=skel, exts={})

    @classmethod
    def _manifest_filepath(cls, record: str) -> Path:
        """Return canonical filename of manifest based on path of a container file."""
        return Path(f"{str(record)}{cls.MANIFEST_EXT}")

    # Override to also load and check latest manifest
    @classmethod
    def _open(cls, paths: List[Path], **kwargs):
        manifest_file: Optional[Path] = kwargs.pop("manifest_file", None)
        ret: IH5MFRecord = super()._open(paths, **kwargs)

        # if not given explicitly, infer correct manifest filename
        # based on logically latest container (they are sorted after parent init)
        if manifest_file is None:
            manifest_file = cls._manifest_filepath(ret._files[-1].filename)

        # for latest container, check linked manifest (if any) against given/inferred one
        ub = ret._ublock(-1)
        ubext = IH5UBExtManifest.get(ub)
        if ubext is not None:
            if not manifest_file.is_file():
                msg = f"Manifest file {manifest_file} does not exist, cannot open!"
                raise ValueError(f"{ret._files[-1].filename}: {msg}")

            chksum = hashsum_file(manifest_file)
            if ubext.manifest_hashsum != chksum:
                msg = "Manifest has been modified, unexpected hashsum!"
                raise ValueError(f"{ret._files[-1].filename}: {msg}")

            ret._manifest = IH5Manifest.parse_file(manifest_file)
            # NOTE: as long as we enforce checksum of manifest, this failure can't happen:
            # if ubext.manifest_uuid != self._manifest.manifest_uuid:
            #     raise ValueError(f"{ub._filename}: Manifest file has wrong UUID!")
        # all looks good
        return ret

    # Override to also check user block extension
    def _check_ublock(
        self,
        filename: Union[str, Path],
        ub: IH5UserBlock,
        prev: Optional[IH5UserBlock] = None,
        check_hashsum: bool = True,
    ):
        super()._check_ublock(filename, ub, prev, check_hashsum)
        # Try getting manifest info in the userblock.
        # If it is missing, probably we're opening a "raw" IH5Record or a messed up mix
        ubext = IH5UBExtManifest.get(ub)
        # we only allow to write patches on top of stubs,
        # but not have stubs on top of something else.
        # If something creates a patch that is (marked as) a stub, its a developer error.
        # If the ub ext is missing, then we must assume that it is not a stub.
        assert prev is None or ubext is None or not ubext.is_stub_container

    def _fixes_after_merge(self, file, ub):
        # if a manifest exists for the current dataset,
        # copy its manifest to overwrite the fresh one of the merged container
        # and fix its user block
        if self._manifest is not None:
            # check that new userblock inherited the original linked manifest
            ext = IH5UBExtManifest.get(ub)
            assert ext is not None and ext.manifest_uuid == self.manifest.manifest_uuid
            # overwrite the "fresh" manifest from merge with the original one
            self.manifest.save(self._manifest_filepath(file))

    # Override to prevent merge if a stub is present
    def merge_files(self, target: Path):
        def is_stub(x):
            ext = IH5UBExtManifest.get(x)
            # missing ext -> not a stub (valid stub has ext + is marked as stub)
            return ext is not None and ext.is_stub_container

        if any(map(is_stub, self.ih5_meta)):
            raise ValueError("Cannot merge, files contain a stub!")

        return super().merge_files(target)

    # Override to create skeleton and dir hashsums, write manifest and add to user block
    # Will inherit old manifest extensions, unless overridden by passed argument
    def commit_patch(self, **kwargs) -> None:
        # is_stub == True only if called from create_stub!!! (NOT for the "end-user"!)
        is_stub = kwargs.pop("__is_stub__", False)
        exts = kwargs.pop("manifest_exts", None)

        # create manifest for the new patch
        mf = self._fresh_manifest()
        if self._manifest is not None:  # inherit attached data, if manifest exists
            mf.manifest_exts = self.manifest.manifest_exts
        if exts is not None:  # override, if extensions provided
            mf.manifest_exts = exts

        old_ub = self._ublock(-1)  # keep ref in case anything goes wrong
        # prepare new user block that links to the prospective manifest
        new_ub = old_ub.copy()
        IH5UBExtManifest(
            is_stub_container=is_stub,
            manifest_uuid=mf.manifest_uuid,
            manifest_hashsum=qualified_hashsum(bytes(mf)),
        ).update(new_ub)

        # try writing new container
        self._set_ublock(-1, new_ub)
        try:
            super().commit_patch(**kwargs)
        except ValueError as e:  # some checks failed
            self._set_ublock(-1, old_ub)  # reset current user block
            raise e

        # as everything is fine, finally (over)write manifest here and on disk
        self._manifest = mf
        mf.save(self._manifest_filepath(self._files[-1].filename))

    @classmethod
    def create_stub(
        cls,
        record: Union[Path, str],
        manifest_file: Path,
    ) -> IH5MFRecord:
        """Create a stub base container for patching an existing but unavailable record.

        The stub is based on the user block of a real IH5 record container line
        and the skeleton of the overlay structure (as returned by `IH5Skeleton`),
        which are taken from a provided manifest file.

        Patches created on top of the stub are compatible with the original record
        whose metadata the stub is based on.

        The returned container is read-only and only serves as base for patches.
        """
        manifest = IH5Manifest.parse_file(manifest_file)

        skeleton: IH5Skeleton = manifest.skeleton
        user_block: IH5UserBlock = manifest.user_block.copy()

        # the manifest-stored user block has no manifest extension itself - create new
        # based on passed manifest.
        # mark it as stub in extra metadata now! important to avoid accidents!
        # must pass it in like that, because the container will be auto-commited.
        ubext = IH5UBExtManifest(
            is_stub_container=True,  # <- the ONLY place where this is allowed!
            manifest_uuid=manifest.manifest_uuid,
            manifest_hashsum=hashsum_file(manifest_file),
        )
        ubext.update(user_block)

        # create and finalize the stub (override userblock and create skeleton structure)
        ds = IH5MFRecord._create(Path(record))
        init_stub_base(ds, user_block, skeleton)  # prepares structure and user block
        # commit_patch() completes stub + fixes the hashsum
        ds.commit_patch(__is_stub__=True)
        assert not ds._has_writable

        return ds
