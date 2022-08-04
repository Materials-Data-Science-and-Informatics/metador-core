"""Tests for MetadorContainer."""
# from pathlib import Path

# import h5py
# from overrides import overrides
# import pytest

# from metador_core.container import MetadorContainer
# from metador_core.hashutils import DEF_HASH_ALG, dir_hashsums
# from metador_core.ih5.container import IH5Record
# from metador_core.packer import DirDiff, Packer
from metador_core.packer.util import DirValidationErrors


def test_errors_add_append():
    errs = DirValidationErrors()
    assert not errs

    errs.add("a", "entry1")
    errs.add("a", "entry2")
    assert errs
    assert errs.errors["a"] == ["entry1", "entry2"]

    errs2 = DirValidationErrors()
    errs2.add("a", "entry3")
    errs2.add("a", "entry4")
    errs2.add("b", "entry5")
    assert errs2.errors["a"] == ["entry3", "entry4"]
    assert errs2.errors["b"] == ["entry5"]

    errs3 = DirValidationErrors()
    errs3.add("b", "entry6")

    errs.append(errs2, errs3)
    assert errs.errors["a"] == ["entry1", "entry2", "entry3", "entry4"]
    assert errs.errors["b"] == ["entry5", "entry6"]


# class DummyPacker(Packer):
#     """Do-nothing packer testing packing api."""

#     fail_check_directory = False
#     ex_check_directory = False
#     ex_pack_directory = False

#     @classmethod
#     @overrides
#     def check_dir(cls, data_dir: Path) -> DirValidationErrors:
#         if cls.ex_check_directory:
#             raise Exception("check_directory failed.")

#         if cls.fail_check_directory:
#             return DirValidationErrors({"error": ["check_directory"]})
#         return DirValidationErrors()

#     @classmethod
#     @overrides
#     def pack(
#         cls, data_dir: Path, diff: DirDiff, record: IH5Record, fresh: bool
#     ):
#         if cls.ex_pack_directory:
#             raise ValueError("pack_directory failed.")

#         # just count number of patches (base container is 0)
#         if "updates" not in record.attrs:
#             record.attrs["updates"] = 0
#         else:
#             if not isinstance(record.attrs["updates"], h5py.Empty):
#                 record.attrs["updates"] = record.attrs["updates"] + 1
