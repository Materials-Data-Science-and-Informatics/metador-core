"""Immutable HDF5-based multi-container datasets.

This API **supports a subset of h5py**, namely reading, writing and deleting groups,
values and attributes. It **does not** support [hard, symbolic or external
links](https://docs.h5py.org/en/latest/high/group.html#link-classes),
so the data must be self-contained and strictly hierarchical.

**Correspondence of IH5 overlay classes and raw h5py classes:**

| IH5 API      | h5py         |
| ------------ | ------------ |
| `IH5Dataset` | [h5py.File](https://docs.h5py.org/en/latest/high/file.html) |
| `IH5Group`   | [h5py.Group](https://docs.h5py.org/en/latest/high/group.html) |
| `IH5Value`   | [h5py.Dataset](https://docs.h5py.org/en/latest/high/dataset.html) |

If you are missing some functionality from h5py in the overlay classes,
please contact us or open an issue and we will see whether and how the missing
methods can be added in a coherent manner matching the IH5 semantics.

## Getting Started

A quite minimal working example:

```python
# just use IH5Dataset instead of h5py.File:
from ardiem_container.ih5 import IH5Dataset


# initial creation:
with IH5Dataset.create("dataset_name") as ds:
    # A new dataset is automatically in writable mode,
    # so let us write some data, just like with h5py:
    ds["foo/bar"] = "something"
    ds["foo"].attrs["attribute"] = 123

# updating the dataset we created later on:
with IH5Dataset.open("dataset_name") as ds:
    # A dataset is opened in read-only mode, so
    # before we can add, modify or delete anything, we need to call:
    ds.create_patch()

    del ds["foo/bar"]
    ds["/foo/bar/baz"] = [1, 2, 3]
```

You SHOULD use `IH5Dataset` as a context manager, like in the example above.
If you for some reason do not, be aware that you MUST manually call `commit()`
to finilize your changes. You can also just use `close()`, which will also call
`commit()` for you. **Consider it good practice calling `commit()`
manually after completing your updates of the dataset.**
"""

from .dataset import IH5Dataset, IH5UserBlock  # noqa: F401
from .overlay import IH5AttributeManager, IH5Group, IH5Value  # noqa: F401
