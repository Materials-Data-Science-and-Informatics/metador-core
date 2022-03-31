"""
This is an example packer plugin.

A packer plugin implements use-case specific container-related
functionality for Ardiem containers.

To develop your own packer plugin, implement a class deriving from
`ArdiemPacker` and register the class as an entrypoint of your package
(see the `pyproject.toml` of this package, where `ExamplePacker`
is registered as a packer plugin called `example`.)
"""

import json
from json.decoder import JSONDecodeError
from pathlib import Path

import h5py
import numpy as np
import pandas
from pydantic import BaseModel, ValidationError

from ..metadata import FileMeta, PackerMeta, TableMeta
from . import (
    ArdiemPacker,
    DiffObjType,
    DirDiff,
    IH5Record,
    PathStatus,
    ValidationErrors,
)


class ExampleMeta(BaseModel):
    author: str


PACKER_META_PATH = "/head/packer"


class ExamplePacker(ArdiemPacker):
    """The example packer is demonstrating how a packer can be implemented.

    It will pack CSV tables with metadata into corresponding HDF5 records,
    and it will pack all other kinds of files as embedded opaque blobs.

    Both kinds of nodes will have corresponding metadata attributes attached.

    The record is expected to have a _meta.json file in the record root
    and each CSV file file.csv needs a companion metadata file file.csv_meta.json.

    All symlinks inside the record will be completely ignored.

    This packer does very verbose logging for didactic purposes.
    Other packers may log their actions as they deem appropriate.
    """

    PACKER_ID = "example"
    PACKER_VERSION = "1.0"

    @classmethod
    def check_directory(cls, data_dir: Path) -> ValidationErrors:
        print("--------")
        print("called check_directory")
        errs = {}

        metafile = data_dir / "_meta.json"
        try:
            ExampleMeta.parse_file(metafile)
        except JSONDecodeError:
            errs[str(metafile)] = ["Cannot parse JSON file!"]
        except (ValidationError, FileNotFoundError) as e:
            errs[str(metafile)] = [str(e)]

        return errs

    @classmethod
    def check_record(cls, record: IH5Record) -> ValidationErrors:
        print("--------")
        print("called check_container")
        errs = {}

        def add_err(k, v):
            if k not in errs:
                errs[k] = []
            errs[k].append(v)

        if PACKER_META_PATH in record:
            try:
                pmeta = PackerMeta.parse_obj(json.loads(record[PACKER_META_PATH][()]))
                if pmeta.id != cls.PACKER_ID:
                    msg = (
                        f"detected packer: '{pmeta.id}' expected: '{cls.PACKER_ID}'",
                    )
                    add_err(PACKER_META_PATH, msg)
            except JSONDecodeError:
                add_err(PACKER_META_PATH, "Cannot parse JSON!")
            except ValidationError as e:
                add_err(PACKER_META_PATH, str(e))
        else:
            add_err(PACKER_META_PATH, "missing")

        return errs

    @classmethod
    def pack_directory(
        cls, data_dir: Path, diff: DirDiff, record: IH5Record, fresh: bool
    ):
        print("--------")
        print("called pack_directory")

        # create/update packer metadata in header
        if not fresh:
            del record["/head/packer"]
        record["/head/packer"] = packer_meta().json()

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

            if path.name.lower().endswith(".csv_meta.json"):
                # will be taken care of when the CSV file is processed
                print("IGNORE:", path, "(sidecar file)")
                continue

            # for this packer, each file maps 1-to-1 to a record path
            key = f"/body/{dnode.path}"  # the path inside the record
            if path.name.lower().endswith(".csv"):  # for CSV files:
                key = key[:-4]  # drop file extension for array inside record

            if status == PathStatus.removed:  # entity was removed ->
                # also remove in record, if it was not a symlink (which we ignored)
                if dnode.prev_type != DiffObjType.symlink:
                    print("DELETE:", key)
                    del record[key]
                continue

            if status == PathStatus.modified:  # changed
                if dnode.prev_type == dnode.curr_type and path.is_dir():
                    continue  # a changed dir should already exist + remain in record

                # remove entity, proceeding with loop body to add new entity version
                print("DELETE:", key)
                del record[key]

            # now we (re-)add new or modified entities:
            if path.is_dir():
                print("CREATE:", path, "->", key, "(dir)")

                record.create_group(key)

            elif path.is_file():
                if path.name.lower().endswith(".csv"):
                    # embed CSV as numpy array with table metadata
                    print("CREATE:", path, "->", key, "(table)")

                    record[key] = pandas.read_csv(path).to_numpy()  # type: ignore
                    metafile = Path(f"{path}_meta.json")
                    metadata = TableMeta.parse_file(metafile).json()
                    record[key].attrs["node_meta"] = metadata

                else:
                    # treat as opaque blob and add file metadata
                    print("CREATE:", path, "->", key, "(file)")

                    data = path.read_bytes()
                    val = np.void(data) if len(data) else h5py.Empty("b")
                    meta = FileMeta.from_file(path).json()
                    record[key] = val
                    record[key].attrs["node_meta"] = meta


def packer_meta() -> PackerMeta:
    """Return metadata info about this packer."""
    return PackerMeta(
        id=ExamplePacker.PACKER_ID,
        version=ExamplePacker.PACKER_VERSION,
        uname=PackerMeta.get_uname(),
        python_package="ardiem-container",
        python_source="https://github.com/Materials-Data-Science-and-Informatics/ardiem-container",
    )
