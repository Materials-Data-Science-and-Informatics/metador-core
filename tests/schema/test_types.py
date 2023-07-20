import isodate
import pytest
from hypothesis import assume, given
from hypothesis import strategies as st
from pydantic import BaseModel, ValidationError, parse_obj_as

import metador_core.schema.types as t
from metador_core.util.pytest import parameters

# ----
# Test our provided hints for default primitive types behave as they should


@pytest.mark.parametrize(
    "typ,val",
    parameters(
        {
            t.Bool: [0, 1, -1, 2, 0.0, 1.0, 2.1],
            t.Int: [True, False, 0.0, 1.0, 2.1],
            t.Float: [True, False, -1, 0, 1, 2],
        }
    ),
)
def test_strict_numeric_reject(typ, val):
    # make sure there is no type coercion between numeric(-like) types
    with pytest.raises(ValidationError):
        parse_obj_as(typ, val)


P_INF = float("inf")
N_INF = float("-inf")


@given(data=st.data())
@pytest.mark.parametrize(
    "t, h",
    [(bool, t.Bool), (int, t.Int), (float, t.Float), (bytes, t.Bytes), (str, t.Str)],
)
def test_def_types_accept(t, h, data):
    strategy = st.from_type(h)  # hint must be hypothesis compatible

    x = data.draw(strategy)
    assume(x == x and x != P_INF and x != N_INF)

    assert isinstance(x, t)  # result is plain type
    assert h(x) == x  # type hint is pass through
    assert parse_obj_as(h, x) == x  # works for parsing


# ----


@pytest.mark.parametrize("x", ["foo", " \tbar "])
def test_nonemptystr_accept(x):
    parse_obj_as(t.NonEmptyStr, x)


@pytest.mark.parametrize("x", ["", "\t", "  "])
def test_nonemptystr_reject(x):
    with pytest.raises(ValidationError):
        parse_obj_as(t.NonEmptyStr, x)


@pytest.mark.parametrize("x", ["application/json", "application/JSON;q=0.9;v=abc"])
def test_mimetypestr_accept(x):
    parse_obj_as(t.MimeTypeStr, x)


@pytest.mark.parametrize("x", ["invalid/mime/type", "invalid mime", "invalidMime"])
def test_mimetypestr_reject(x):
    with pytest.raises(ValidationError):
        parse_obj_as(t.MimeTypeStr, x)


@pytest.mark.parametrize("x", ["sha256:aebf", "sha512:aebf"])
def test_qualhashsumstr_accept(x):
    parse_obj_as(t.QualHashsumStr, x)


@pytest.mark.parametrize(
    "x", ["wrong:aebf", "sha512:invalid", "sha256", "sha256:", "aebf", ":aebf"]
)
def test_qualhashsumstr_reject(x):
    with pytest.raises(ValidationError):
        parse_obj_as(t.QualHashsumStr, x)


# ----


@pytest.mark.parametrize("x", [123, True])
def test_duration_reject(x):
    with pytest.raises(ValidationError):
        parse_obj_as(t.Duration, x)


@pytest.mark.parametrize("x", ["PT3H4M1S", "P3Y6M4DT12H30M5S"])
def test_duration_accept(x):
    assert isinstance(parse_obj_as(t.Duration, x), isodate.Duration)


# ----


@pytest.mark.parametrize(
    "t_cls,x",
    parameters(
        {
            t.PintUnit: [
                "meter / (second * kg) ** 2",
                "dimensionless",
                t.PintUnit("second"),
                "1",
            ],
            t.PintQuantity: [
                "123 meter / (second * kg) ** 2",
                "5",
                t.PintQuantity("7 seconds"),
            ],
        }
    ),
)
def test_pint_accept(t_cls, x):
    parse_obj_as(t_cls, x)


@pytest.mark.parametrize(
    "t_cls,x",
    parameters(
        {
            t.PintUnit: ["invalid", "2", "", 123],
            t.PintQuantity: ["123 bla", 23.12, False],
        }
    ),
)
def test_pint_reject(t_cls, x):
    with pytest.raises(ValidationError):
        parse_obj_as(t_cls, x)


@pytest.mark.parametrize(
    "cls,val", [(t.PintUnit, "meters * second"), (t.PintQuantity, "5 meters * second")]
)
def test_pint_schema_description(cls, val):
    # make sure the description is attached to jsonschema

    class SomeModel(BaseModel):
        u: cls

    assert (
        SomeModel(u=val).schema()["properties"]["u"]["title"].lower().find("pint") >= 0
    )


# from metador_core.ih5.record import IH5Record
# from metador_core.packer.util import MetadorValidationErrors
# from metador_core.schema.common import FileMeta
#
# @pytest.mark.skip(reason="FIXME port to equivalent tests in new (packer) API")
# def test_filemeta(tmp_path, tmp_ds_path):
#     # get metadata for a file, save to yaml, load from yaml, check it
#     file = tmp_path / "myfile.txt"
#     with open(file, "w") as f:
#         f.write("hello world!")

#     with open(tmp_path / "myfile_meta.yaml", "w") as f:
#         f.write(FileMeta.for_file(file).yaml())

#     # failure gives MetadorValidationErrors
#     with pytest.raises(MetadorValidationErrors):
#         FileMeta.from_file(tmp_path / "non-existing.yaml")

#     # check success
#     fmeta = FileMeta.from_file(tmp_path / "myfile_meta.yaml")
#     assert isinstance(fmeta, FileMeta)
#     assert fmeta.filename == "myfile.txt"
#     assert (
#         fmeta.hashsum
#         == "sha256:7509e5bda0c762d2bac7f90d758b5b2263fa01ccbc542ab5e3df163be08e6ca9"
#     )
#     assert fmeta.mimetype == "text/plain"

#     # try with reading from a record
#     with IH5Record(tmp_ds_path, "w") as ds:
#         ds.create_dataset("group/dataset", data=fmeta.json())
#         ds["group/dataset"].attrs["attr"] = fmeta.json()
#         ds["invalid"] = "--"
#         ds["incomplete"] = "{}"

#         with pytest.raises(MetadorValidationErrors) as e:
#             FileMeta.from_record(ds, "group")
#         assert e.value.errors["group"][0].lower().find("group") >= 0

#         with pytest.raises(MetadorValidationErrors) as e:
#             FileMeta.from_record(ds, "not-existing")
#         assert e.value.errors["not-existing"][0].lower().find("not found") >= 0

#         with pytest.raises(MetadorValidationErrors) as e:
#             FileMeta.from_record(ds, "invalid")
#         assert e.value.errors["invalid"][0].lower().find("cannot parse") >= 0

#         with pytest.raises(MetadorValidationErrors) as e:
#             FileMeta.from_record(ds, "incomplete")
#         assert e.value.errors["incomplete"][0].lower().find("missing") >= 0
