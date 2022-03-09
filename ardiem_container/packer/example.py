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

from ardiem_container.packer import ArdiemPacker


class ExamplePacker(ArdiemPacker):
    @staticmethod
    def check_directory(dir: Path) -> Dict[str, str]:
        # TODO: dirschema
        return {}

    @staticmethod
    def check_container(container: Path) -> Dict[str, str]:
        # TODO: dirschema
        return {}

    @staticmethod
    def pack_directory(dir: Path, container: Path):
        print("called pack_directory")
