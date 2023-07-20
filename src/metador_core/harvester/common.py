import magic
from PIL import Image

from ..harvester import FileHarvester
from ..plugins import schemas
from ..util.hashsums import hashsum

FileMeta = schemas["core.file"]
ImageFileMeta = schemas["core.imagefile"]
BibMeta = schemas["core.bib"]
TableMeta = schemas["core.table"]


class FileMetaHarvester(FileHarvester):
    """Default harvester for basic common.file metadata.

    Harvests file name, file size, mimetype and hashsum of the file.
    """

    class Plugin:
        name = "core.file.generic"
        version = (0, 1, 0)
        returns = schemas.PluginRef(name="core.file", version=(0, 1, 0))

    def run(self):
        path = self.args.filepath

        sz = path.stat().st_size
        hs = hashsum(open(path, "rb"), "sha256")
        mt = magic.from_file(path, mime=True)
        return self.schema(
            filename=path.name, contentSize=sz, sha256=hs, encodingFormat=mt
        )


class ImageFileMetaHarvester(FileHarvester):
    """Harvester to obtain dimensions (width and height) of an image file."""

    class Plugin:
        name = "core.imagefile.dim"
        version = (0, 1, 0)
        returns = schemas.PluginRef(name="core.imagefile", version=(0, 1, 0))

    def run(self):
        path = self.args.filepath

        with Image.open(path) as img:
            width, height = img.size
        return self.schema(width=width, height=height)
