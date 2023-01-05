from hypothesis import given
from hypothesis import strategies as st

from metador_core.container.utils import (
    is_internal_path,
    is_meta_base_path,
    to_data_node_path,
    to_meta_base_path,
)


def test_is_internal_path():
    assert not is_internal_path("test")
    assert not is_internal_path("_metador_something")
    assert not is_internal_path("/_metador_something")
    assert not is_internal_path("/test/_metador_something")
    assert is_internal_path("metador_something")
    assert is_internal_path("/metador_something")
    assert is_internal_path("/test/metador_something_else/")

    assert is_internal_path("/a/prefix_something/b", "prefix_")
    assert not is_internal_path("/a/_prefix_something/b", "prefix_")


def test_is_meta_base_path():
    assert not is_meta_base_path("_metador_meta_b")
    assert is_meta_base_path("metador_meta_")
    assert is_meta_base_path("/metador_meta_a")
    assert is_meta_base_path("/a/metador_meta_b")


def test_to_meta_base_path():
    assert to_meta_base_path("/", False) == "/metador_meta_"
    assert to_meta_base_path("/", True) == "/metador_meta_"
    assert to_meta_base_path("/a/b", False) == "/a/b/metador_meta_"
    assert to_meta_base_path("/a/b", True) == "/a/metador_meta_b"


def test_to_data_node_path():
    assert to_data_node_path("/metador_meta_") == "/"
    assert to_data_node_path("/metador_meta_a") == "/a"
    assert to_data_node_path("a/metador_meta_b") == "a/b"
    assert to_data_node_path("a/b/metador_meta_") == "a/b"


# NOTE: 0-~ excludes /, but includes most normal ASCII printable characters
rel_paths = st.from_regex(r"([0-~]+)(/[0-~]+)*", fullmatch=True)
abs_paths = st.one_of(st.sampled_from("/"), rel_paths.map(lambda s: f"/{s}"))
paths = st.one_of(abs_paths, rel_paths)


@given(st.tuples(paths, st.booleans()))
def test_meta_base_data_node_inverses(node):
    """Test that converting a path into the metadata directory path and back is correct."""
    n_path, n_type = node
    meta_path = to_meta_base_path(n_path, n_type)
    data_path = to_data_node_path(meta_path)
    assert n_path == data_path
