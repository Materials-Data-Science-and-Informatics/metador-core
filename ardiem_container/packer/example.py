"""
This is an example packer plugin.

A packer plugin implements use-case specific container-related
functionality for Ardiem containers.

To develop your own packer plugin, implement a class deriving from
`ArdiemPacker` and register the class as an entrypoint of your package
(see the `pyproject.toml` of this package, where `ExamplePacker`
is registered as a packer plugin called `example`.)
"""

from json.decoder import JSONDecodeError
from pathlib import Path

from pydantic import BaseModel, ValidationError

# from ..metadata import File, Table
from . import ArdiemPacker, DirDiff, IH5Dataset, ValidationErrors


class ExampleMeta(BaseModel):
    author: str


class ExamplePacker(ArdiemPacker):
    """The example packer is a demonstration of the correct implementation of a packer.

    It will pack numpy tables with metadata into corresponding HDF5 datasets,
    and it will pack all other kinds of files as embedded blobs.

    Both kinds of nodes will have metadata attributes attached.
    """

    PACKER_NAME = "example"
    VERSION = 1

    @staticmethod
    def check_directory(data_dir: Path) -> ValidationErrors:
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

    @staticmethod
    def check_dataset(dataset: IH5Dataset) -> ValidationErrors:
        print("called check_container")
        errs = {}

        def add_err(k, v):
            if k not in errs:
                errs[k] = []
            errs[k].append(v)

        for atr in ["packer", "packer_version"]:
            if atr not in dataset.attrs:
                add_err("", f"missing attribute: {atr}")

        pname = dataset.attrs.get("packer", None)
        if pname != ExamplePacker.PACKER_NAME:
            add_err("@packer", f"unexpected value: {pname}")

        return errs

    @staticmethod
    def pack_directory(data_dir: Path, diff: DirDiff, dataset: IH5Dataset, fresh: bool):
        print("called pack_directory")

        dataset.attrs["packer"] = ExamplePacker.PACKER_NAME
        dataset.attrs["packer_version"] = ExamplePacker.VERSION

        for path, status in diff.annotate(data_dir).items():
            print(status, path)
