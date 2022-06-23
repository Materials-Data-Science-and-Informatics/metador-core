# Design of IH5 records

This document describes the motivation and design of the "Immutable HDF5" (IH5) format.
The IH5 format is a HDF5-based format for the storage of data solving the following core
problem: **Updating an existing record without modifying the original HDF5 file**.

This requirement arises in the context where research data is stored in archival
repositories. Often, these do not allow mutation of the files, and even if they do, for
large records uploading the whole record again due to a minor change is not a satisfying
solution due to possible time and bandwidth limitations.

To solve this problem, IH5 is modelled around the idea that instead of a single HDF5-file
forming a record, a collection of HDF5 files form a record. One of these files will be
the initial record, while all other files are subsequent patches, i.e. smaller files
tracking only the changes that are applied to the data (instead of being a full updated
copy of the data).

At the same time, we want to abstract away the technical details from the user and give
them an interface such that a multi-file patch-based record can be accessed for both
reading and writing in mostly the same way as a regular single HDF5 file, while the
required patch creation and interpretation happens behind the scenes.

## IH5 file sets, abstractly

On a mathematical level, we understand IH5 files as elements of a **monoid**, i.e. a set
with a neutral element and an associative binary operation `*`. In our setting, the
neutral element is the "empty patch" doing nothing to the data, while the operation is
simply the application of the patch on the right to the previous state of data on the
left. As usual, we use concatenation for the binary operation, i.e. write `ab` for `a*b`.

From the theoretical point of view, there is no difference between the "original file" and
its patches, as the original file can be considered as a patch applied to an empty HDF5
file, which acts as our neutral element. In the following, we use the monoid
elements and their sequences to denote the data they represent, i.e. the actual
intended content, regardless of the internal representation of patches and ignoring
whether the assembled record is a physical file or a purely conceptual entity. This means
that e.g. `abc` is understood as "the record obtained by taking the data in `a`, applying
patch `b` to it and then applying patch `c` to the result".

We want to ensure that for a base HDF5 file denoted by `a` and some subsequent patch files
denoted by `b,c` the user can access the data as if he had a single file `a'` which
contains the changes that `b` does to `a` and the changes that `c` does to `ab`, i.e. `abc
== a'`. Similarly, when the user decides to apply changes to `a' = abc` leading to a new
state of the data we now call `a''`, they may do so, while under the hood a file `d` with
the changes is created such that `a'' = abcd` has the contents desired by the user. We
call the facade providing this unified view on the data that spans multiple files an
`overlay`, i.e. `a' := overlay(a,b,c)` and `a'' := overlay(a,b,c,d)`.

In some situations it might also be desirable to be able to actually apply the patches,
i.e. physically actualize a file such as `a'` in the example which accumulates changes
done by multiple patches by some operation `merge` such that `merge(a1, a2, ..., an)`
results in an actual file `a'`. From a semantic point of view, this means that the result
of both operations applied to the same arguments should be equivalent as monoid element,
i.e. `overlay(a1,...,an) == merge(a1,...,an)` for all inputs `a1, ..., an`.

## IH5 file sets, technically

While abstractly, any two HDF5 patches would be compatible when defined in a sufficiently
liberal way, in practice it is desirable to add some restrictions and validation for
working with such HDF5 file sets.

Conceptually, a user creates a **record** as an HDF5 file containing some data (this
initial file is what we call the **base container**) and might later update it by creating
**patches**. The patches are specific to this conceptual record at a specific point in
time, represented by a collection of already existing HDF5 files, and we want to prevent
the user from using an incoherent, incomplete or otherwise invalid collection of patches
that does not represent a meaningful entity. To ensure this, we do the following.

When a new record is created, it gets a unique **record UUID** that is inherited to all
future patches. This UUID links together patches that belong to the same record.
Additionally, each HDF5 file has a **patch UUID** that tags the state of the record (to
be precise, it identifies the state of the data after applying the patch, not the patch
itself) and a **patch index** that is incremented for each successive patch.
Each new patch to the record also has a **previous patch UUID**, which must be
equal to the **patch UUID** of the previous patch file. Finally, for integrity checking a
**data hashsum** of the HDF5 container is stored after a patch is declared as completed.

This information can be used to infer the correct sequence and applicability of patches
even in the case that the order or relationship is not apparent from the filenames.
A valid fileset consists of one initial file without assigned **previous patch UUID** and
files with successively increasing patch indices and correctly linking to the patch UUID
of the predecessor. The hashsums can be used to detect accidental tampering with the files
e.g. if a user would open and edit them individually (which we cannot prevent, but want to
detect).

The provided class `IH5Record` is to be used instead of `h5py.File` and has the
following main operations:

* `create(record_name)`
* `open(record_name)`
* `close()`
* `create_patch()`
* `discard_patch()`
* `commit()`
* `merge(new_record_name)`

The method `create()` will create a fresh record, which is simply wrapping a writable
HDF5 file. The user can write data to the record only when he just `create`d it or when
the method `create_patch()` has been called (which takes care of patch file creation).
After completing the writing, `commit()` must be called to finalize the update, which will
among other things add the metadata above, including the hashsum.

The IH5 specific metadata outlined above is written in the **user block** before the
actual HDF5 file contents begin. This makes it impossible for the user to accidentally
damage this information and allows to keep this administrative information in the same
file without interfering with the actual contents in any way.

Apart from the special methods just described, the `IH5Record` can be used mostly like
the usual `h5py.File` API for all operations and essentially provides the `overlay` (see
above) creating the illusion of working with a single record file, while the `merge()`
method allows to materialize the overlayed data into a single file (i.e. implements the
`merge` operation above).

## IH5 patches, abstractly

An HDF5 file is just a tree where groups (`h5py.Group`) and attribute sets
(`h5py.AttributeManager`) form inner nodes, while HDF5 datasets (`h5py.Dataset`) and
attribute values are the leaves. We do not support external, hard or symbolic links in the
container. Thus, written as an abstract datatype (in pseudo-Haskell), the HDF5 file has
the following internal structure:

```
type Attrs = Map Key Value
data HDF5Tree = Record Attrs Value | Group Attrs (Map Key HDF5Tree)
```

where `Value` denotes any supported value for storage as attribute or dataset in the HDF5
file, such as primitive types or n-dimensional arrays.

To support patching, i.e. declaring updates to the data, we need to extend this structure
with two new elements:

* a **deletion marker**

which is a special value stating that the data at this location in the previous state is
to be considered removed.

* a **virtual node**

which can be used as a non-overwriting group or dataset and is required e.g. to **add** or
**modify** elements without completely overwriting the whole subtree, i.e. attribute set
or (sub-)group.

Hence, the resulting tree is conceptually defined as:

```
data ValOrDel = Deleted | Val Value
type Attrs = Map Key ValOrDel
data HDF5Tree = Record Attrs ValOrDel | Group Attrs (Map Key HDF5Tree)
              | Virtual Attrs (Map Key HDF5Tree)
```

To **create or overwrite** an entity at some location `/a/b/c` in the record,
create a path of virtual inner nodes `/a/b` (if any node along the path does not exist)
and put the actual value as `c` (i.e. data value or new group). If the desired update is
concerned with attributes, also create `c` as a virtual inner node and add the new
desired attribute value to it.
To **delete** an entity at some location, proceed exactly as above, but assign the
deletion marker as the value.

**Read access** to a path through multiple patch files is done by looking backward for the
most recent patch containing a non-virtual node along a path, and then recursively
enumerating its children (attribute keys or sub-groups) starting from that patch, while
ignoring all earlier patches during the successive search of the children.

Each data and attribute value has a most recent patch where it was created or
overwritten which determines its **creation index** and it corresponds to its intended
value. If the stored value is the special deletion marker, the entity at this path is
considered as not existing anymore.

Similarly, each group or attribute set has a **creation index** and as children contains
the corresponding most recently created or overwritten non-virtual(!) nodes (unless these
are deletion markers). This means, a group or attribute set is "open" towards the future,
until the corresponding path has been overwritten by a non-virtual node in a future patch
In that case, from that point on the future patch takes the role of being the starting
reference point for accumulating children (or, if these are deletion markers, the path
is considered as non-existing).

## IH5 patches, technically

To represent the deletion marker, we use an opaque single byte value equal to
the non-printable ASCII character **DEL**.

For practical reasons we chose to identify normal HDF5 groups with **virtual nodes**,
while we add a special marking for **non-virtual groups** (i.e., groups intended to
completely overwrite what was previously at that location). We mark non-virtual groups by
the presence of an attribute whose name is equal to the non-printable ASCII character
**SUB**.

This imposes the following small **restrictions on "regular" HDF5 file contents**:

* there MUST NOT be an attribute with key `b'\x1a'` (ASCII-SUB) attached to a group
* there MUST NOT be a record or attribute with value `np.void(b'\x7f')` (ASCII-DEL)

We expect that this will not affect any imaginable realistic use cases, though.

## Patching over Stubs

We also provide the possibility to create an auxiliary **stub base container**.
It mimics a real record by having its metadata (with an additional `is_stub` flag set),
contain all paths and attributes that are included in the container, but no actual data.

The use of such a stub is to be a foundation for creating patches without having the
complete record locally available. The stub can be constructed from only the information
stored in the most recent user block and a **skeleton** of existing paths and attributes
inside the container.

**Patches created on top of a stub are compatible with the real record**, so the patch
can be created locally without having the full record and then be uploaded to the
same location where the record is actually stored in order to update the remote record.

The stub containers may not be used for merging the data for obvious reasons.
Also, accessing a stub-based patch will only allow to see data added in the patch - for
every non-overridden path there will be just a placeholder "stub" where the data would be
located in the real record the stub is based on.
