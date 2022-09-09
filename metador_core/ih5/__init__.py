"""Immutable HDF5-based multi-container records.

This API **supports a subset of h5py**, namely reading, writing and deleting groups,
values and attributes. It **does not** support [hard, symbolic or external
links](https://docs.h5py.org/en/latest/high/group.html#link-classes),
so the data must be self-contained and strictly hierarchical.


**Correspondence of IH5 overlay classes and raw h5py classes:**

| IH5 API      | h5py         |
| ------------ | ------------ |
| `IH5Record`  | [h5py.File](https://docs.h5py.org/en/latest/high/file.html) |
| `IH5Group`   | [h5py.Group](https://docs.h5py.org/en/latest/high/group.html) |
| `IH5Dataset` | [h5py.Dataset](https://docs.h5py.org/en/latest/high/dataset.html) |
| `IH5AttributeManager` | [h5py.AttributeManager](https://docs.h5py.org/en/latest/high/attr.html) |

Anything that can be done to an IH5 Group, Dataset or AttributeManager can also be done to
the h5py counterparts.

If you are missing some functionality from h5py in the overlay classes,
please contact us or open an issue and we will see whether and how the missing
methods can be added in a coherent manner matching the IH5 semantics.

## Getting Started

A quite minimal working example:

```python
# just use IH5Record instead of h5py.File:
from metador_core.ih5.container import IH5Record


# initial creation:
with IH5Record("record_name", "w") as ds:
    # A new record is automatically in writable mode,
    # so let us write some data, just like with h5py:
    ds["foo/bar"] = "something"
    ds["foo"].attrs["attribute"] = 123

# updating the record we created later on:
with IH5Record("record_name", "r") as ds:
    # A record is opened in read-only mode, so
    # before we can add, modify or delete anything, we need to call:
    ds.create_patch()

    del ds["foo/bar"]
    ds["/foo/bar/baz"] = [1, 2, 3]
```

You SHOULD use `IH5Record` as a context manager, like in the example above.
If you for some reason do not, be aware that you MUST manually call `commit()`
to finilize your changes. You can also just use `close()`, which will also call
`commit()` for you. **Consider it good practice calling `commit()`
manually after completing your updates of the record.**
"""
