"""
This is an example packer plugin.

A packer plugin implements use-case specific container-related
functionality for Metador containers.

To develop your own packer plugin, implement a class deriving from
`MetadorPacker` and register the class as an entrypoint of your package
(see the `pyproject.toml` of this package, where `GenericPacker`
is registered as a packer plugin called `example`.)
"""

from pathlib import Path

import h5py
import numpy as np
import pandas

from ..schema.common import FileMeta, ImageMeta, TableMeta
from ..schema.common.biblio import BibDummyMeta
from . import MetadorContainer, Packer
from .diff import DiffNode, DirDiff
from .util import DirValidationError


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

    @classmethod
    def check_dir(cls, data_dir: Path):
        print("--------")
        print("called check_directory")
        errs = DirValidationError()
        errs.append(BibDummyMeta.check_path(data_dir / "biblio.yaml"))
        if errs:
            raise errs

    @classmethod
    def update(cls, container: MetadorContainer, data_dir: Path, diff: DirDiff):
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

            # for this packer, each file maps 1-to-1 to a container path
            key = f"/body/{dnode.path}"  # the path inside the container

            if path.name.lower().endswith(".csv"):  # for CSV files:
                key = key[:-4]  # drop file extension for array inside container

            # special case - some custom metadata with nontrivial mapping
            if path.relative_to(data_dir) == Path("example_meta.yaml"):
                key = "/body/custom"

            if status == DiffNode.Status.removed:  # entity was removed ->
                # also remove in container, if it was not a symlink (which we ignored)
                if dnode.prev_type != DiffNode.ObjType.symlink:
                    print("DELETE:", key)
                    del container[key]
                continue

            if status == DiffNode.Status.modified:  # changed
                if dnode.prev_type == dnode.curr_type and path.is_dir():
                    continue  # a changed dir should already exist + remain in container

                # remove entity, proceeding with loop body to add new entity version
                print("DELETE:", key)
                del container[key]

            # now we (re-)add new or modified entities:
            if path.is_dir():
                print("CREATE:", path, "->", key, "(dir)")

                container.create_group(key)

            elif path.is_file():
                if key == "/body/custom":
                    # update custom ExampleMeta stuff
                    print("CREATE:", path, "->", key, "(custom ExampleMeta)")
                    container[key] = BibDummyMeta.from_path(path).json()

                elif path.name.lower().endswith(".csv"):
                    # embed CSV as numpy array with table metadata
                    print("CREATE:", path, "->", key, "(table)")

                    container[key] = pandas.read_csv(path).to_numpy()  # type: ignore
                    metafile = Path(f"{path}_meta.yaml")
                    metadata = TableMeta.parse_file(metafile).json()
                    container[key].attrs["node_meta"] = metadata

                elif path.name.lower().endswith((".jpg", ".jpeg", ".png")):
                    # embed image file with image-specific metadata
                    print("CREATE:", path, "->", key, "(image)")

                    data = path.read_bytes()
                    val = np.void(data) if len(data) else h5py.Empty("b")
                    meta = ImageMeta.for_file(path).json()
                    container[key] = val
                    container[key].attrs["node_meta"] = meta

                else:
                    # treat as opaque blob and add file metadata
                    print("CREATE:", path, "->", key, "(file)")

                    data = path.read_bytes()
                    val = np.void(data) if len(data) else h5py.Empty("b")
                    meta = FileMeta.for_file(path).json()
                    container[key] = val
                    container[key].attrs["node_meta"] = meta
