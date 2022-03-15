"""
This is an example packer plugin.

A packer plugin implements use-case specific container-related
functionality for Ardiem containers.

To develop your own packer plugin, implement a class deriving from
`ArdiemPacker` and register the class as an entrypoint of your package
(see the `pyproject.toml` of this package, where `ExamplePacker`
is registered as a packer plugin called `example`.)
"""

from pathlib import Path
from typing import Dict

from ardiem_container.dataset import IH5Dataset
from ardiem_container.packer import ArdiemPacker


class ExamplePacker(ArdiemPacker):
    @staticmethod
    def check_directory(data_dir: Path) -> Dict[str, str]:
        print("called check_directory")
        # TODO: dirschema
        return {}

    @staticmethod
    def check_container(container: IH5Dataset) -> Dict[str, str]:
        print("called check_container")
        # TODO: dirschema
        return {}

    @staticmethod
    def pack_directory(data_dir: Path, container: IH5Dataset, update: bool):
        print("called pack_directory")
