from pathlib import Path

import magic
from PIL import Image

from ..harvester import Harvester, HarvesterPlugin
from ..hashutils import hashsum
from ..plugins import installed

_SCHEMAS = installed.group("schema")

FileMeta = _SCHEMAS["core.file"]
ImageFileMeta = _SCHEMAS["core.imagefile"]
BibMeta = _SCHEMAS["core.bib"]
TableMeta = _SCHEMAS["core.table"]


class FileMetaHarvester(Harvester):
    """Default harvester for common.file metadata."""

    class Plugin(HarvesterPlugin):
        name = "core.file.generic"
        version = (0, 1, 0)
        returns = FileMeta.Plugin.ref(version=(0, 1, 0))

    def __init__(self, path: Path):
        self.path = path

    def harvest(self):
        """Return FileMeta object for a file using magic and computing a hashsum."""
        sz = self.path.stat().st_size
        hs = hashsum(open(self.path, "rb"), "sha256")
        mt = magic.from_file(self.path, mime=True)
        return self.schema(
            id_=self.path.name, contentSize=sz, sha256=hs, encodingFormat=[mt]
        )


class ImageFileMetaHarvester(Harvester):
    """Default harvester for basic imagefile-specific metadata (width and height)."""

    class Plugin(HarvesterPlugin):
        name = "core.imagefile.dim"
        version = (0, 1, 0)
        returns = ImageFileMeta.Plugin.ref(version=(0, 1, 0))

    def __init__(self, path: Path):
        self.path = path

    def harvest(self):
        with Image.open(self.path) as img:
            width, height = img.size
        return self.schema(width=width, height=height)
