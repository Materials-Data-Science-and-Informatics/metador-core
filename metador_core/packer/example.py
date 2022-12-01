"""
This is an example packer plugin for simple general data types.

A packer plugin implements use-case specific container-related
functionality for Metador containers.

To develop your own packer plugin, implement a class deriving from
`Packer` and register the class as an entrypoint of your package
(see the `pyproject.toml` of this package, where `GenericPacker`
is registered as a packer plugin called `example`.)
"""

from pathlib import Path
from typing import Any, Union

import pandas
from overrides import overrides

from ..util.diff import DiffNode, DirDiff
from . import MetadorContainer, Packer
from .utils import DirValidationErrors, check_metadata_file, embed_file

BibMeta = Any
TableMeta = Any


class GenericPacker(Packer):
    """The generic packer is demonstrating how a packer can be implemented.

    It will pack CSV tables with metadata into corresponding HDF5 containers,
    and it will pack all other kinds of files as embedded opaque blobs.

    Both kinds of nodes will have corresponding metadata attributes attached.

    The directory is expected to have a _meta.yaml file in the container root
    and each CSV file file.csv needs a companion metadata file file.csv_meta.yaml.

    All symlinks inside the directory are completely ignored.

    This packer does very verbose logging for didactic purposes.
    Other packers may log their actions as they deem appropriate.
    """

    class Plugin:
        name = "core.generic"
        version = (0, 1, 0)

    META_SUFFIX: str = "_meta.yaml"

    @classmethod
    def sidecar_for(cls, path: Union[Path, str]) -> str:
        """Sidecar file name for given path."""
        return f"{path}{cls.META_SUFFIX}"

    @classmethod
    @overrides
    def check_dir(cls, data_dir: Path) -> DirValidationErrors:
        print("--------")
        print("called check_dir")
        errs = DirValidationErrors()
        errs.update(
            check_metadata_file(
                data_dir / cls.META_SUFFIX, required=True, schema=BibMeta
            )
        )
        return errs

    @classmethod
    @overrides
    def update(cls, mc: MetadorContainer, data_dir: Path, diff: DirDiff):
        print("--------")
        print("called update")

        for path, dnode in diff.annotate(data_dir).items():
            # the status indicates whether the file was added, removed or modified
            status = diff.status(dnode)
            print(status.value, path)

            if dnode is None:  # unchanged paths in the directory have no diff node
                print("IGNORE:", path, "(unchanged)")
                continue  # nothing to do

            if path.is_symlink():  # we ignore symlinks in the data directory
                print("IGNORE:", path, "(symlink)")
                continue

            if path.name.lower().endswith(".csv_meta.yaml"):
                # will be taken care of when the CSV file is processed
                print("IGNORE:", path, "(sidecar file)")
                continue

            # for this simple packer, each file maps 1-to-1 to a container path
            key = f"{dnode.path}"  # path inside the container

            if status == DiffNode.Status.removed:  # entity was removed ->
                # also remove in container, if it was not a symlink (which we ignored)
                if dnode.prev_type != DiffNode.ObjType.symlink:
                    print("DELETE:", key)
                    del mc[key]
                continue

            if status == DiffNode.Status.modified:  # changed
                if dnode.prev_type == dnode.curr_type and path.is_dir():
                    continue  # a changed dir should already exist + remain in container

                # otherwise it was replaced either file -> dir or dir -> file, so
                # remove entity, proceeding with loop body to add new entity version
                print("DELETE:", key)
                del mc[key]

            # now we (re-)add new or modified entities:
            if path.is_dir():
                print("CREATE:", path, "->", key, "(dir)")

                mc.create_group(key)

            elif path.is_file():
                if path.name.endswith(cls.META_SUFFIX):
                    if key == cls.META_SUFFIX:
                        # update root meta
                        print("CREATE:", path, "->", key, "(biblio metadata)")
                        mc.meta["common_biblio"] = BibMeta.parse_file(path)
                else:

                    if path.name.lower().endswith(".csv"):
                        # embed CSV as numpy array with table metadata
                        print("CREATE:", path, "->", key, "(table)")

                        mc[key] = pandas.read_csv(path).to_numpy()  # type: ignore
                        mc[key].meta["common_table"] = TableMeta.for_file(
                            cls.sidecar_for(path)
                        )

                    elif path.name.lower().endswith((".jpg", ".jpeg", ".png")):
                        # embed image file with image-specific metadata
                        print("CREATE:", path, "->", key, "(image)")
                        embed_file(mc, key, path)
                        # mc[key].meta["common_image"] = image_meta_for(path)

                    else:
                        # treat as opaque blob and add file metadata
                        print("CREATE:", path, "->", key, "(file)")
                        embed_file(mc, key, path)

                    # mc[key].meta["common_file"] = file_meta_for(path)
