from metador_core.util.types import H5DatasetLike, H5FileLike, H5GroupLike, H5NodeLike


def check_protocols_for(f):
    # create group and dataset node
    f["group/dataset"] = 123

    # check that behavior is correct:

    # file-like:
    assert isinstance(f, H5NodeLike)
    assert isinstance(f, H5FileLike)
    assert isinstance(f, H5GroupLike)
    assert not isinstance(f, H5DatasetLike)

    # group-like:
    g = f["group"]
    assert isinstance(f, H5NodeLike)
    assert not isinstance(g, H5FileLike)
    assert isinstance(g, H5GroupLike)
    assert not isinstance(g, H5DatasetLike)

    # dataset-like:
    d = g["dataset"]
    assert isinstance(f, H5NodeLike)
    assert not isinstance(d, H5FileLike)
    assert not isinstance(d, H5GroupLike)
    assert isinstance(d, H5DatasetLike)


def test_instance_checks(fresh_mc):
    # Check that all driver classes work correctly with the protocol types.
    #
    # Expected relationships (< = base_class_of):
    #  H5DatasetLike > H5NodeLike < H5GroupLike < H5FileLike
    check_protocols_for(fresh_mc.__wrapped__)


def test_instance_checks_wrapped(fresh_mc):
    # same, but wrapped with MetadorContainer
    check_protocols_for(fresh_mc)
