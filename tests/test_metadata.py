import pytest
from pydantic import BaseModel, ValidationError, parse_obj_as

import metador_core.schema.types as t
from metador_core.ih5.record import IH5Record
from metador_core.packer.util import MetadorValidationErrors

# from metador_core.schema.common import FileMeta, ImageMeta, TableMeta


def test_str_types():
    # nonempty_str
    parse_obj_as(t.nonempty_str, "a")
    with pytest.raises(ValidationError):
        parse_obj_as(t.nonempty_str, "")

    # mimetype_str
    parse_obj_as(t.mimetype_str, "application/json")
    parse_obj_as(t.mimetype_str, "application/JSON;q=0.9;v=abc")
    with pytest.raises(ValidationError):
        parse_obj_as(t.mimetype_str, "invalid/mime/type")
    with pytest.raises(ValidationError):
        parse_obj_as(t.mimetype_str, "invalid mime")
    with pytest.raises(ValidationError):
        parse_obj_as(t.mimetype_str, "invalidMime")

    # hashsum_str
    parse_obj_as(t.hashsum_str, "sha256:aebf")
    parse_obj_as(t.hashsum_str, "sha512:aebf")
    with pytest.raises(ValidationError):
        parse_obj_as(t.hashsum_str, "wrong:aebf")
    with pytest.raises(ValidationError):
        parse_obj_as(t.hashsum_str, "sha512:invalid")
    with pytest.raises(ValidationError):
        parse_obj_as(t.hashsum_str, "sha256")
    with pytest.raises(ValidationError):
        parse_obj_as(t.hashsum_str, "sha256:")
    with pytest.raises(ValidationError):
        parse_obj_as(t.hashsum_str, "aebf")
    with pytest.raises(ValidationError):
        parse_obj_as(t.hashsum_str, ":aebf")

    # PintUnit
    parse_obj_as(t.PintUnit, "meter / (second * kg) ** 2")
    parse_obj_as(t.PintUnit, "dimensionless")
    parse_obj_as(t.PintUnit, t.PintUnit.Parsed("second"))
    parse_obj_as(t.PintUnit, "1")
    with pytest.raises(ValidationError):
        parse_obj_as(t.PintUnit, "invalid")
    with pytest.raises(ValidationError):
        parse_obj_as(t.PintUnit, "2")
    with pytest.raises(ValidationError):
        parse_obj_as(t.PintUnit, "")
    with pytest.raises(ValidationError):
        parse_obj_as(t.PintUnit, 123)

    class SomeModel(BaseModel):
        u: t.PintUnit

    SomeModel(u="meters * second").schema_json().lower().find("pint") >= 0  # type: ignore


@pytest.mark.skip(reason="FIXME port to equivalent tests in new API")
def test_filemeta(tmp_path, tmp_ds_path):
    # get metadata for a file, save to yaml, load from yaml, check it
    file = tmp_path / "myfile.txt"
    with open(file, "w") as f:
        f.write("hello world!")

    with open(tmp_path / "myfile_meta.yaml", "w") as f:
        f.write(FileMeta.for_file(file).yaml())

    # failure gives MetadorValidationErrors
    with pytest.raises(MetadorValidationErrors):
        FileMeta.from_file(tmp_path / "non-existing.yaml")

    # check success
    fmeta = NodeMeta.from_file(tmp_path / "myfile_meta.yaml")
    assert isinstance(fmeta, FileMeta)
    assert fmeta.filename == "myfile.txt"
    assert (
        fmeta.hashsum
        == "sha256:7509e5bda0c762d2bac7f90d758b5b2263fa01ccbc542ab5e3df163be08e6ca9"
    )
    assert fmeta.mimetype == "text/plain"

    # try with reading from a record
    with IH5Record.create(tmp_ds_path) as ds:
        ds.create_dataset("group/dataset", data=fmeta.json())
        ds["group/dataset"].attrs["attr"] = fmeta.json()
        ds["invalid"] = "--"
        ds["incomplete"] = "{}"

        with pytest.raises(MetadorValidationErrors) as e:
            FileMeta.from_record(ds, "group")
        assert e.value.errors["group"][0].lower().find("group") >= 0

        with pytest.raises(MetadorValidationErrors) as e:
            FileMeta.from_record(ds, "not-existing")
        assert e.value.errors["not-existing"][0].lower().find("not found") >= 0

        with pytest.raises(MetadorValidationErrors) as e:
            FileMeta.from_record(ds, "invalid")
        assert e.value.errors["invalid"][0].lower().find("cannot parse") >= 0

        with pytest.raises(MetadorValidationErrors) as e:
            FileMeta.from_record(ds, "incomplete")
        assert e.value.errors["incomplete"][0].lower().find("missing") >= 0
