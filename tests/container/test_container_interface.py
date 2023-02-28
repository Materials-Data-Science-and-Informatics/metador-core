import pytest

from metador_core.container import MetadorContainer
from metador_core.container.drivers import get_driver_type
from metador_core.container.utils import METADOR_VERSION_PATH
from metador_core.container.wrappers import (
    NodeAcl,
    UnsupportedOperationError,
    WrappedAttributeManager,
)


@pytest.fixture
def bibmeta_example(schemas):
    """Return a BibMeta instance.

    BibMeta is used here, because it is:
    * commonly relevant
    * actually using schema inheritance
    """
    BibMeta = schemas.get("core.bib", (0, 1, 0))
    Person = BibMeta.Fields.author.schemas.Person
    obj = BibMeta(
        name="Dataset1",
        abstract="Some longer description text.",
        dateCreated="2023-01-23",
        author=[Person(name="Jane Doe")],
        creator=Person(name="Jane Doe"),
    )
    return obj


def test_container_init(tmp_mc_path, mc_driver):
    drv_cls = mc_driver.value
    with MetadorContainer(tmp_mc_path, "w", driver=drv_cls) as m:
        c_uuid = m.metador.container_uuid
        c_src = m.metador.source

        # as expected
        assert m["/"]._self_container is m
        assert m.metador.spec_version[0] == 1
        assert m.metador.driver == drv_cls
        assert m.metador.driver_type == mc_driver
        assert m.mode == "r+"

    # can open based on retrieved "data source object"
    with MetadorContainer(c_src, "r", driver=drv_cls) as m:
        assert m.metador.container_uuid == c_uuid  # UUID did not change
        assert m.mode == "r"

    # can also just wrap a raw container that is already opened
    with drv_cls(c_src, "r") as r:
        assert MetadorContainer(r).__wrapped__ is r  # unpacked container object


def test_container_init_fail(tmp_mc_path, mc_driver):
    driver = mc_driver.value
    with driver(tmp_mc_path, "w") as r:
        # create empty container file and close it
        ...

    with pytest.raises(ValueError):
        # read-only and non-metador -> can't init
        with driver(tmp_mc_path, "r") as r:
            MetadorContainer(r)

    with MetadorContainer(driver(tmp_mc_path, "w")) as m:
        # init container, once writable, once only readable
        assert m.metador.container_uuid
        assert m.metador.spec_version[0] == 1

    with driver(tmp_mc_path, "w") as r:
        # mark with invalid version
        r[METADOR_VERSION_PATH] = "2.0"

    with pytest.raises(ValueError):
        # invalid spec version is assigned
        MetadorContainer(driver(tmp_mc_path, "r"))


# ----


def test_container_acl(tmp_mc_path, mc_driver):
    """Test soft ACL features of node wrappers."""
    drv_cls = mc_driver.value
    with MetadorContainer(tmp_mc_path, "w", driver=drv_cls) as m:
        m["foo/bar"] = [1, 2, 3]
        m["foo/bar"].attrs["hello"] = 123

        g = m["/"]
        g.restrict(read_only=True, local_only=True)
        g.restrict(read_only=False)  # should have no effect (can only set)
        assert g.acl[NodeAcl.read_only] == True

        # check that ACL is inherited downward
        g2 = g["foo"]
        assert g2.acl[NodeAcl.read_only] == True
        assert g2.acl[NodeAcl.local_only] == True
        # check that accessing parent does not allow to escape acl
        assert g2.parent.name == g.name
        assert g2.parent.acl == g.acl

        # narrow down local scope
        g2.restrict(local_only=True)

        # test local_only restriction:
        # can't access container or parent group
        with pytest.raises(UnsupportedOperationError):
            g2.file
        with pytest.raises(UnsupportedOperationError):
            g2.parent
        with pytest.raises(ValueError):
            g2["/"]  # absolute paths forbidden

        # can go down and back up to current level (local_only)
        d = g2["bar"]
        assert d.parent.name == g2.name
        assert d.parent.acl == g2.acl  # acl still not lost

        # test read_only restriction:
        with pytest.raises(UnsupportedOperationError):
            # can't add new dataset
            g2["qux"] = 123
        with pytest.raises(UnsupportedOperationError):
            # can't remove dataset
            del g2["bar"]
        with pytest.raises(UnsupportedOperationError):
            # can't edit in dataset
            d[0] = [4, 5, 6]
        with pytest.raises(UnsupportedOperationError):
            d.resize(1, 2, 3)

        # test attribute wrapper
        assert d.acl[NodeAcl.read_only]
        a = d.attrs
        assert isinstance(a, WrappedAttributeManager)
        assert a._self_acl == d.acl

        assert "hello" in a
        assert a["hello"] == 123
        assert a.get("hello") == 123
        assert list(a.keys()) == ["hello"]

        with pytest.raises(UnsupportedOperationError):
            # can't add attribute
            d.attrs["something"] = "new"
        with pytest.raises(UnsupportedOperationError):
            # can't remove attribute
            del d.attrs["hello"]

        # test skel_only on dataset and attributes
        d.restrict(skel_only=True)
        with pytest.raises(UnsupportedOperationError):
            d[()]  # cannot read data
        a = d.attrs
        assert "hello" in a
        assert list(iter(a)) == ["hello"]
        assert list(a.keys()) == ["hello"]
        with pytest.raises(UnsupportedOperationError):
            a["hello"]  # cannot read attribute
        with pytest.raises(UnsupportedOperationError):
            a.get("hello")
        with pytest.raises(UnsupportedOperationError):
            a.values()
        with pytest.raises(UnsupportedOperationError):
            a.items()

        # even with all restrictions enabled,
        # can still access metador specific properties
        d.meta
        d.metador


def test_container_toc_infos(tmp_mc_path, mc_driver):
    drv_cls = mc_driver.value
    with MetadorContainer(tmp_mc_path, "w", driver=drv_cls) as m:
        assert m.metador.driver == drv_cls
        assert m.metador.driver_type == get_driver_type(m.__wrapped__)
        assert m.metador.source is not None

        assert m.metador.container_uuid is not None
        assert m.metador.spec_version == [1, 0]


def test_node_metadata(tmp_mc_path, mc_driver, schemas, bibmeta_example):
    """Check that basic operations on node metadata work correctly."""
    meta = bibmeta_example
    BibMeta = type(meta)
    BibMeta_ref = BibMeta.Plugin.ref()
    DirMeta = schemas.get(schemas.parent_path(BibMeta)[-2])
    meta2 = meta.copy(update=dict(name="Dataset2"))
    assert meta != meta2

    drv_cls = mc_driver.value
    with MetadorContainer(tmp_mc_path, "w", driver=drv_cls) as m:
        m["foo/bar"] = [1, 2, 3]
        ds = m["foo/bar"]

        # try accessing non-existing metadata
        with pytest.raises(KeyError):
            ds.meta["core.bib"]

        # add metadata object
        assert len(ds.meta) == 0
        ds.meta["core.bib"] = meta
        assert len(ds.meta.items()) == 1

        # try to overwrite
        with pytest.raises(ValueError):
            ds.meta["core.bib"] = meta

        # get expected data back
        ret1 = ds.meta["core.bib"]
        assert isinstance(ret1, BibMeta)
        assert ret1.name == "Dataset1"

        # check contains (it has "inheritance semantics")
        assert "" not in ds.meta
        assert "something" not in ds.meta
        assert "core.bib" in ds.meta
        assert "core.dir" in ds.meta  # by inheritance (!!!)

        # get data back as parent schema (!!!)
        ret1b = ds.meta["core.dir"]
        assert isinstance(ret1b, DirMeta)
        assert ret1b.name == "Dataset1"

        # add another, actual instance, for the parent schema
        # that will take precedence over the "coerced" instance
        ds.meta["core.dir"] = meta2
        assert len(ds.meta.values()) == 2
        # get expected metadata back
        ret2 = ds.meta.get("core.dir")
        assert isinstance(ret1, DirMeta)
        assert ret2.name == "Dataset2"

        # query by schema name and version, check that hits are as expected

        # try passing no schema, plugin reference or schema class
        assert len(list(ds.meta.query())) == 2  # anything goes
        assert list(ds.meta.query(BibMeta_ref)) == [BibMeta_ref]
        assert len(list(ds.meta.query(BibMeta))) == 1
        # ask for any version
        assert len(list(ds.meta.query("core.bib"))) == 1
        assert len(list(ds.meta.query("core.dir"))) == 2
        # ask for a newer minor version - should return the older instances
        assert len(list(ds.meta.query("core.bib", (0, 2, 0)))) == 1
        assert len(list(ds.meta.query("core.dir", (0, 2, 0)))) == 2
        # ask for a newer major version - incompatible -> no hits
        assert len(list(ds.meta.query("core.bib", (1, 0, 0)))) == 0
        assert len(list(ds.meta.query("core.dir", (1, 0, 0)))) == 0

        # delete metadata -> should be gone
        del ds.meta["core.bib"]
        assert len(ds.meta.keys()) == 1
        assert len(list(iter(ds.meta))) == 1
        assert "core.bib" not in ds.meta
        assert "core.dir" in ds.meta

        del ds.meta["core.dir"]
        assert next(ds.meta.query(), None) is None
        assert "core.dir" not in ds.meta


def test_toc_metadata_schemas_packages(tmp_mc_path, mc_driver, bibmeta_example):
    """Check that schema and package tracking works correctly."""
    meta = bibmeta_example
    BibMeta = type(meta)
    BibMeta_ref = BibMeta.Plugin.ref()
    meta2 = meta.copy(update=dict(name="Dataset2"))
    assert meta != meta2

    drv_cls = mc_driver.value
    with MetadorContainer(tmp_mc_path, "w", driver=drv_cls) as m:
        m["foo/bar"] = [1, 2, 3]

        assert len(m.metador.schemas) == 0  # nothing should be here
        # attach some metadata with a non-trivial schema using inheritance:
        m["foo/bar"].meta["core.bib"] = meta

        # schema + providing package registered. check it is as expected
        assert len(m.metador.schemas) == 1  # core.bib is added
        assert BibMeta_ref in m.metador.schemas
        assert isinstance(m.metador.schemas[BibMeta_ref], dict)  # embedded json schema
        assert m.metador.schemas.provider(BibMeta_ref).name == "metador-core"

        # embedded schema parents/children interface (important for granular queries)
        bm_pp = m.metador.schemas.parent_path(BibMeta)
        assert len(bm_pp) == 2  # [core.dir ref, core.bib ref]
        assert bm_pp[-1] == BibMeta_ref
        assert bm_pp[-2].name == "core.dir"
        assert BibMeta_ref in m.metador.schemas.children("core.dir", (0, 1, 0))

        # add another similar metadata entry
        m["foo/qux"] = [4, 5, 6]
        m["foo/qux"].meta["core.dir"] = meta  # store explicitly as parent schema
        assert len(m.metador.schemas) == 2  # core.dir is added

        # removes nodes (should also kill metadata and clean up TOC)
        del m["foo/qux"]
        assert len(m.metador.schemas) == 1
        assert len(m.metador.schemas.packages) == 1
        del m["foo/bar"]
        assert len(m.metador.schemas) == 0
        assert len(m.metador.schemas.packages) == 0


def test_container_queries(tmp_mc_path, mc_driver, bibmeta_example):
    """Check that container/group-wide query works correctly."""
    meta = bibmeta_example
    BibMeta = type(meta)
    BibMeta_ref = BibMeta.Plugin.ref()
    meta2 = meta.copy(update=dict(name="Meta2"))
    assert meta != meta2

    drv_cls = mc_driver.value
    with MetadorContainer(tmp_mc_path, "w", driver=drv_cls) as m:
        # add some (meta)data
        m["foo/bar"] = [1, 2, 3]
        m["foo/bar"].meta["core.bib"] = meta

        # try container-wide queries
        assert len(list(m.metador.query("not_existing"))) == 0

        assert len(list(m.metador.query(BibMeta))) == 1
        assert len(list(m.metador.query(BibMeta_ref))) == 1
        assert len(list(m.metador.query("core.bib"))) == 1
        assert len(list(m.metador.query("core.bib", (0, 2, 0)))) == 1
        assert len(list(m.metador.query("core.bib", (1, 0, 0)))) == 0

        assert len(list(m.metador.query("core.dir"))) == 1
        assert len(list(m.metador.query("core.dir", (0, 2, 0)))) == 1
        assert len(list(m.metador.query("core.dir", (1, 0, 0)))) == 0

        # explicitly add parent schema
        m["foo/qux"] = [4, 5, 6]
        m["foo/qux"].meta["core.dir"] = meta2
        # create an empty group just for testing purposes
        m.create_group("baz")

        assert len(list(m.metador.query("core.dir"))) == 2
        assert len(list(m.metador.query("core.dir", (0, 2, 0)))) == 2

        assert len(list(m["baz"].metador.query("core.dir"))) == 0
        assert len(list(m["foo"].metador.query("core.dir"))) == 2
        assert len(list(m["foo/qux"].metador.query("core.bib"))) == 0
        assert len(list(m["foo/bar"].metador.query("core.bib"))) == 1


def test_group_operations_metadata_correct(tmp_mc_path, mc_driver, bibmeta_example):
    """Check that groups and datasets are moved together with metadata."""
    meta = bibmeta_example
    drv_cls = mc_driver.value
    with MetadorContainer(tmp_mc_path, "w", driver=drv_cls) as m:
        # create a group with dataset, both with a metadata entry
        # -> non-trivial case, covering relevant differences and most cases
        m["foo/bar"] = [1, 2, 3]
        m["foo/bar"].meta["core.bib"] = meta  # dataset metadata
        m["foo"].meta["core.bib"] = meta  # group metadata

        # sanity check
        expected = {"/foo", "/foo/bar"}
        assert set(map(lambda x: x.name, m.metador.query("core.bib"))) == expected
        assert len(m.metador._links.find_broken()) == 0  # check TOC was fine before
        assert len(m.metador._links.find_missing(m["/"])) == 0

        # ----
        # perform move and copy operations on both dataset and group nodes with metadata

        # copy dataset:
        m.copy("foo/bar", "foo/baz")
        expected = {"/foo", "/foo/bar", "/foo/baz"}
        assert set(map(lambda x: x.name, m.metador.query("core.bib"))) == expected
        # move dataset:
        m.move("foo/bar", "qux")
        expected = {"/foo", "/foo/baz", "/qux"}
        assert set(map(lambda x: x.name, m.metador.query("core.bib"))) == expected
        # copy and move a group:
        m.copy("foo", "blub")
        m.move("foo", "bla")
        expected = {"/bla", "/bla/baz", "/blub", "/blub/baz", "/qux"}
        assert set(map(lambda x: x.name, m.metador.query("core.bib"))) == expected

        # check that all metadata node links in TOC are still functional
        assert len(m.metador._links.find_missing(m["/"])) == 0
        assert len(m.metador._links.find_broken()) == 0

        uuids = set()
        for n_path in expected:
            # check that the metadata can actually be accessed
            assert m[n_path].meta.get("core.bib") is not None

            # check that uuid mangling worked for move/copy (agrees with dataset node path)
            meta_info = m[n_path].meta._objs["core.bib"]
            assert meta_info.uuid == type(meta_info).from_node(meta_info.node).uuid
            uuids.add(meta_info.uuid)  # collect
        # check that the metadata objects are all actually distinct
        assert len(uuids) == len(expected)

        # ----
        # remove and see everything is really gone

        del m["/qux"]  # dataset
        del m["/bla"]  # group
        del m["/blub"]  # group

        # no data and metadata should be found now!
        assert len(list(m.keys())) == 0
        assert set(map(lambda x: x.name, m.metador.query("core.bib"))) == set()
        # no metadata node links in TOC or container should be left
        assert len(m.metador._links.find_missing(m["/"])) == 0
        assert len(m.metador._links.find_broken()) == 0
