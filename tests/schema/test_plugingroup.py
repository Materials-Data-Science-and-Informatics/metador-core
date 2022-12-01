import pytest


def test_schema_pg(plugingroups_test):
    # BibMeta has DirMeta as parent. we use that for testing
    schemas = plugingroups_test.get("schema")
    BibMeta = schemas.get("core.bib")
    DirMeta = schemas.get("core.dir")
    b_ref = BibMeta.Plugin.ref()
    d_ref = DirMeta.Plugin.ref()

    # parentpath
    expected = [d_ref, b_ref]
    assert schemas.parent_path(b_ref.name, b_ref.version) == expected
    assert schemas.parent_path(b_ref) == expected
    assert schemas.parent_path(BibMeta) == expected

    assert schemas.parent_path(d_ref.name, d_ref.version) == [d_ref]

    with pytest.raises(KeyError):  # no such version of schema
        schemas.parent_path(b_ref.name, (0, 1, 2))
    with pytest.raises(KeyError):  # no schema
        schemas.parent_path("invalid", (0, 0, 1))

    # children
    expected = {b_ref}
    assert schemas.children(d_ref.name, d_ref.version) == expected
    assert schemas.children(d_ref) == {b_ref} == expected
    assert schemas.children(DirMeta) == {b_ref} == expected

    assert schemas.children(b_ref.name, b_ref.version) == set()

    with pytest.raises(KeyError):  # no such version of schema
        schemas.children(b_ref.name, (0, 1, 2))
    with pytest.raises(KeyError):  # no schema
        schemas.children("invalid", (0, 0, 1))
