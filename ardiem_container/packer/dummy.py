"""Dummy packers for testing."""
import inspect
from pathlib import Path

from . import ArdiemPacker, DirDiff, IH5Dataset, ValidationErrors


class DummyPacker(ArdiemPacker):
    """The dummy packer."""

    @staticmethod
    def check_directory(data_dir: Path) -> ValidationErrors:
        print(f"called: {inspect.currentframe().f_code.co_name}")  # type: ignore
        # return {"error": "value"}
        return {}  # no errors

    @staticmethod
    def check_dataset(dataset: IH5Dataset) -> ValidationErrors:
        print(f"called: {inspect.currentframe().f_code.co_name}")  # type: ignore
        return {}  # no errors

    @staticmethod
    def pack_directory(data_dir: Path, diff: DirDiff, dataset: IH5Dataset, fresh: bool):
        print(f"called: {inspect.currentframe().f_code.co_name}")  # type: ignore
        print(f"from {data_dir} to {dataset._files[-1].filename} (fresh={fresh})")
        print(diff)
        dataset.attrs["packer"] = "dummy"
        # no exceptions -> everything ok
