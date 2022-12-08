from metador_core.container import MetadorContainer
from metador_core.harvester import file_harvester_pipeline, harvest, metadata_loader
from metador_core.packer.utils import embed_file


def test_pack(plugingroups_test, tutorialfiles, tmp_ds_path):
    """Pack a container with various file types."""
    schemas = plugingroups_test["schema"]
    harvesters = plugingroups_test["harvester"]

    BibMeta = schemas["core.bib"]
    DBMeta = schemas["core.dashboard"]
    ImgMeta = schemas["core.imagefile"]

    HrvFile = harvesters["core.file.generic"]
    HrvImgDim = harvesters["core.imagefile.dim"]
    ImgMetaLoader = metadata_loader(ImgMeta, use_sidecar=True)

    bibmeta_path = tutorialfiles("test.bibmeta.yaml")
    imgfile_path = tutorialfiles("test.png")
    jsonfile_path = tutorialfiles("test.json")
    mdfile_path = tutorialfiles("test.md")
    htmlfile_path = tutorialfiles("test.html")
    pdffile_path = tutorialfiles("test.pdf")

    image_pipeline = file_harvester_pipeline(HrvFile, HrvImgDim, ImgMetaLoader)
    imgmeta = harvest(ImgMeta, image_pipeline(imgfile_path))

    tmp_ds_path.mkdir()
    with MetadorContainer(tmp_ds_path / "test.h5", "w") as m:
        m.meta["core.bib"] = BibMeta.parse_file(bibmeta_path)

        node = embed_file(m, "foo/bar", imgfile_path, metadata=imgmeta)
        node.meta[DBMeta] = DBMeta.show(group=1)
        node = embed_file(m, "pdffile", pdffile_path)
        node.meta[DBMeta] = DBMeta.show(group=1, priority=10)
        node = embed_file(m, "jsonfile", jsonfile_path)
        node.meta[DBMeta] = DBMeta()
        node = embed_file(m, "mdfile", mdfile_path)
        node.meta[DBMeta] = DBMeta()
        node = embed_file(m, "htmlfile", htmlfile_path)
        node.meta[DBMeta] = DBMeta.show(
            [
                DBMeta.widget(widget_name="core.file.text.code", group=2),
                DBMeta.widget(group=2),
            ]
        )

    m = MetadorContainer("test.h5", "a")
    m.close()

    # TODO: assert some stuff
