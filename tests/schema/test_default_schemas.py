import pytest
from hypothesis import assume, given
from hypothesis import strategies as st
from pydantic import ValidationError

FILE_DICT = dict(
    filename="test.txt", encodingFormat="text/plain", contentSize=0, sha256="abc"
)


@given(st.sets(st.sampled_from(list(FILE_DICT.keys()))))
def test_filemeta(schemas_test, ks):
    assume(len(ks) < len(FILE_DICT))

    FileMeta = schemas_test["core.file"]
    with pytest.raises(ValidationError):
        FileMeta(**{k: v for k, v in FILE_DICT.items() if k in ks})


def test_filemeta2(schemas_test):
    FileMeta = schemas_test["core.file"]
    m = FileMeta(**FILE_DICT)
    assert m.dict().get("@context").find("ro/crate") > 0
    assert m.dict().get("@type") == "File"


DIR_DICT = dict(name="directory")


def test_dirmeta(schemas_test):
    DirMeta = schemas_test["core.dir"]
    m = DirMeta(**DIR_DICT)
    assert m.dict().get("@context").find("ro/crate") > 0
    assert m.dict().get("@type") == "Dataset"


# TODO: test organization
BIB_DICT = dict(
    name="My dataset",
    abstract="some description",
    dateCreated="2123-04-05",
    creator=dict(name="Jane Doe"),
    author=[],
)


def test_bibmeta(schemas_test):
    BibMeta = schemas_test["core.bib"]
    m = BibMeta(**BIB_DICT)
    print(m)
    assert m.dict().get("@context").find("ro/crate") > 0
    assert m.dict().get("@type") == "Dataset"


# TODO: test imagefile meta, TableMeta, SIValue, NumValue
