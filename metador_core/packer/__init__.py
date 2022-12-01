"""Definition of HDF5 packer plugin interface."""
from __future__ import annotations

from abc import ABC, abstractmethod
from io import UnsupportedOperation
from pathlib import Path
from typing import Tuple, Type

import wrapt
from overrides import EnforceOverrides, overrides

from metador_core.ih5.manifest import IH5MFRecord
from metador_core.plugins import plugingroups

from ..container import MetadorContainer
from ..plugin import interface as pg
from ..schema.core import MetadataSchema
from ..schema.plugins import PluginPkgMeta
from ..util.diff import DirDiff
from ..util.hashsums import DirHashsums, dir_hashsums
from .types import DirValidationErrors


class Packer(ABC, EnforceOverrides):
    """Interface to be implemented by Metador HDF5 packer plugins.

    These plugins is how support for wildly different domain-specific
    use-cases can be added to Metador in a opt-in and loosely-coupled way.

    Users can install only the packer plugins they need for their use-cases,
    and such plugins can be easily developed independently from the rest
    of the Metador tooling, as long as this interface is respected.

    Carefully read the documentation for the required attributes and methods
    and implement them for your use-case in a subclass.
    See `metador_core.packer.example.GenericPacker` for an example plugin.

    Requirements for well-behaved packers:

    1. No closing of the container:
    The packer gets a writable record and is only responsible for performing
    the neccessary additions, deletions and modifications. It is not allowed
    to `close()` the container.

    2. No access to data in the container:
    Data in the container MUST NOT be read or be relied on for doing an update,
    as the nodes could be dummy stubs. One MAY rely on existence or absence of
    Groups, Datasets, Attributes and Metadata in the container (e.g. `in` or `keys`).

    3. Source directory is read-only:
    Files or directories inside of `data_dir` MUST NOT be created, deleted or
    modified by this method.

    4. Exceptional termination:
    In case that packing must be aborted, and exception MUST be raised.
    If the exception happened due to invalid data or metadata, it MUST be
    an DirValidationError object like in the other methods above, helping to find
    and fix the problem. Otherwise, a different appropriate exception may be used.

    5. Semantic correctness:
    Packing a directory into a fresh container and updating an existing container
    MUST lead to the same observable result.

    If you cannot guarantee this in full generality, do not implement `update`.
    In that case, if a container is updated, it will be cleared and then `pack` is
    called on it, as if it was a fresh container. In this case, there is no space
    advantage gained over a fresh container (but it will keep its UUID).

    6. Semantic versioning of packers:

    A packer MUST be able to update records that were created by this packer
    of the same or an earlier MINOR version.

    More formally, the version MAJOR.MINOR.PATCH
    MUST adhere to the following contract:

    1. increasing MAJOR means a break in backward-compatibility
    for older datasets (i.e. new packer cannot work with old records),

    2. increasing MINOR means a break in forward-compatibility
    for newer datasets (i.e. older packers will not work with newer records),

    3. increasing PATCH does not affect compatibility
    for datasets with the same MAJOR and MINOR version.

    When the packer is updated, the Python package version MUST increase
    in a suitable way. As usual, whenever an earlier number is increased,
    the following numbers are reset to zero.

    This means, the PATCH version should increase for e.g. bugfixes that do
    not change the structure or metadata stored in the dataset,
    MINOR should increase whenever from now on older versions of the packer
    would not be able to produce a valid update for a dataset created with this version,
    but upgrading older existing datasets with this version still works.
    Finally, MAJOR version is increased when all compatibility guarantees are off
    and the resulting container cannot be migrated or updated automatically.

    You SHOULD provide tooling to migrate datasets between major versions.
    """

    Plugin: PackerPlugin

    @classmethod
    @abstractmethod
    def check_dir(cls, data_dir: Path) -> DirValidationErrors:
        """Check whether the given directory is suitable for packing with this plugin.

        This method will be called before `pack` or `update` and MUST detect
        all problems (such as missing or invalid data or metadata) that can be
        expected to be fixed by the user in preparation for the packing.

        More specifically, it MUST cover all metadata that is to be provided directly by
        the user (i.e. is not inferred or extracted from generated data) for the purpose
        of packing and SHOULD try to cover as many problems with data and metadata as
        possible to avoid failure during the actual packing process.

        Files or directories inside of `data_dir` MUST NOT be created,
        deleted or modified by this method.

        Args:
            data_dir: Directory containing all the data to be packed.

        Returns:
            DirValidationError initialized with a dict mapping file paths
            (relative to `dir`) to lists of detected errors.

            The errors must be either a string (containing a human-readable summary of all
            problems with that file), or another dict with more granular error messages,
            in case that the file is e.g. a JSON-compatible file subject to validation
            with JSON Schemas.
        """
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def update(cls, mc: MetadorContainer, data_dir: Path, diff: DirDiff):
        """Update a MetadorContainer with changes done to the data source directory.

        The `container` is assumed to be writable, and is either empty
        or was previously packed by a compatible version of the packer.

        The `data_dir` is assumed to be suitable (according to `check_dir`).

        The `diff` structure contains information about changed paths.

        If not implemented, updates will be created by clearing the provided
        container and using `pack` on it.

        Args:
            container: Metador IH5 record to pack the data into or update
            data_dir: Directory containing all the data to be packed
            diff: Diff tree of dirs and files in data_dir compared to a previous state
        """
        # default fallback implementation using pack
        for obj in [mc, mc.attrs, mc.meta]:
            for key in obj.keys():
                del obj[key]
        return cls.pack(mc, data_dir)

    @classmethod
    @abstractmethod
    def pack(cls, mc: MetadorContainer, data_dir: Path):
        """Pack a directory into an MetadorContainer.

        The `container` is assumed to be writable and empty.

        The `data_dir` is assumed to be suitable (according to `check_dir`).

        If not implemented, initial packing is done using `update`
        with an empty container and a diff containing all the files.

        Args:
            container: Metador IH5 record to pack the data into or update
            data_dir: Directory containing all the data to be packed
        """
        # default fallback implementation using update
        return cls.update(mc, data_dir, DirDiff.compare({}, dir_hashsums(data_dir)))


class PackerInfo(MetadataSchema):
    """Schema for info about the packer that was used to create a container."""

    class Plugin:
        name = "core.packerinfo"
        version = (0, 1, 0)

    packer: PGPacker.PluginRef
    """Packer plugin used to pack the container."""

    pkg: PluginPkgMeta
    """Python package that provides the packer plugin."""

    source_dir: DirHashsums = {}
    """Directory skeleton with hashsums of files at the time of packing."""

    @classmethod
    def for_packer(cls, packer_name: str, packer_version=None) -> PackerInfo:
        from ..plugins import packers

        p_ref = packers.resolve(packer_name, packer_version)
        return PackerInfo(
            packer=p_ref,
            pkg=packers.provider(p_ref),
        )


class Unclosable(wrapt.ObjectProxy):
    """Wrapper to prevent packers from closing/completing a container file."""

    _self_MSG = "Packers must not finalize the container!"

    def close(self):
        raise UnsupportedOperation(self._self_MSG)

    # specific for IH5Record subtypes:

    def discard_patch(self):
        raise UnsupportedOperation(self._self_MSG)

    def commit_patch(self):
        raise UnsupportedOperation(self._self_MSG)


PACKER_GROUP_NAME = "packer"


class PackerPlugin(pg.PluginBase):
    ...


class PGPacker(pg.PluginGroup[Packer]):
    """Packer plugin group interface."""

    class Plugin:
        name = PACKER_GROUP_NAME
        version = (0, 1, 0)
        plugin_class = Packer
        plugin_info_class = PackerPlugin
        requires = [
            plugingroups.PluginRef(name="schema", version=(0, 1, 0)),
            plugingroups.PluginRef(name="harvester", version=(0, 1, 0)),
        ]

    _PACKER_INFO_NAME = PackerInfo.Plugin.name

    @overrides
    def check_plugin(self, ep_name: str, plugin: Type[Packer]):
        pg.util.check_implements_method(ep_name, plugin, Packer.check_dir)
        missing_pack = pg.util.implements_method(plugin, Packer.pack)
        missing_update = pg.util.implements_method(plugin, Packer.update)
        if missing_pack and missing_update:
            raise TypeError(f"{ep_name}: Neither pack nor update are implemented!")

    # ----

    def _prepare(self, pname: str, srcdir: Path) -> Tuple[Type[Packer], DirHashsums]:
        """Return packer class and hashsums of given directory.

        Raises an exception if packer is not found or `packer.check_dir` fails.
        """
        packer = self[pname]
        if errs := packer.check_dir(srcdir):
            raise errs
        return (packer, dir_hashsums(srcdir))

    def pack(self, packer_name: str, data_dir: Path, target: Path, h5like_cls):
        """Pack a directory into a container using an installed packer.

        `packer_name` must be an installed packer plugin.

        `data_dir` must be an existing directory suitable for the packer.

        `target` must be a non-existing path and will be passed into `h5like_cls` as-is.

        `h5like_cls` must be a class compatible with MetadorContainer.

        In case an exception happens during packing, notice that no cleanup is done.

        The user is responsible for removing inconsistent files that were created.

        Args:
            packer_name: installed packer plugin name
            data_dir: data source directory
            target: target path for resulting container
            h5like_cls: class to use for creating the container
        """
        packer, hashsums = self._prepare(packer_name, data_dir)
        # use skel_only to enforce stub-compatibility of packer
        container = MetadorContainer(h5like_cls(target, "x")).restrict(skel_only=True)
        packer.pack(Unclosable(container), data_dir)
        self._finalize(packer_name, hashsums, container)

    def update(self, packer_name: str, data_dir: Path, target: Path, h5like_cls):
        """Update a container from its source directory using an installed packer.

        Like `pack`, but the `target` must be a container which can be opened
        with the `h5like_cls` and was packed by a compatible packer.

        In case an exception happens during packing, notice that no cleanup is done
        and if the container has been written to, the changes persist.

        The user is responsible for removing inconsistent files that were created
        and ensuring that the previous state can be restored, e.g. from a backup.
        """
        packer, hashsums = self._prepare(packer_name, data_dir)
        # use skel_only to enforce stub-compatibility of packer
        container = MetadorContainer(h5like_cls(target, "r+")).restrict(skel_only=True)

        # check compatibility
        pinfo = container.meta.get(self._PACKER_INFO_NAME)
        if not pinfo:
            msg = f"Container does not have {self._PACKER_INFO_NAME} metadata!"
        curr_ref = self.resolve(packer_name)
        if not curr_ref.supports(pinfo.packer):
            msg = f"{curr_ref} (installed) does not support {pinfo.packer} (container)!"
            raise ValueError(msg)

        diff = DirDiff.compare(pinfo.source_dir, hashsums)
        packer.update(Unclosable(container), data_dir, diff)
        self._finalize(packer_name, hashsums, container)

    def _finalize(self, pname: str, hsums: DirHashsums, cont: MetadorContainer):
        """Set or update packer info in container and close it."""
        if self._PACKER_INFO_NAME in cont.meta:
            del cont.meta[self._PACKER_INFO_NAME]

        pinfo = PackerInfo.for_packer(pname)
        pinfo.source_dir = hsums
        cont.meta[self._PACKER_INFO_NAME] = pinfo

        if isinstance(cont, IH5MFRecord):
            # when using IH5MFRecord,
            # we want the packerinfo in the manifest, so tooling can decide
            # if a container can be updated without having it available.
            # (default manifest already has enough info for creating stubs)
            cont.manifest.manifest_exts[self.name] = pinfo.dict()

        cont.close()
