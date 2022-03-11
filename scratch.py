from pathlib import Path

from ardiem_container.dataset import ArdiemDataset

try:
    ds = ArdiemDataset.create(Path("test-ds"))
except FileExistsError:
    ds = ArdiemDataset.open(Path("test-ds"))
