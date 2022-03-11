## Patch Semantics

Normal datasets and groups that are present in the patch completely overwrite
the previous entities at their location, including attached attributes.
This is used for creating and substituting leaves or subtrees in the container.

Datasets/groups/attributes are deleted by replacing the group path target or attribute
value with the special value np.void(b'\x7f') (i.e. pure removal without replacement).
If the original location did not exist, this has no effect and is ignored.

Finally, granular modification of datasets/groups/attributes is done by special
pass-through groups that have an attribute named '\x10' (DLE) (with any value attached).
The meaning of this special node is to
* modify (create/substitute/delete) attributes of the node at this location
* if previous entity at this location is a group,
  recursively interpret patches in child nodes
  (if previous entity at this location is a dataset, children are ignored!)

If the most recent location did not exist (at all or was DELeted),
the pass-through group has no effect and is ignored.

## Restrictions on HDF5 File Contents

The patching mechanism imposes these small restrictions on "regular" HDF5 file contents:
    * there MUST NOT be an attribute with key b'\x10' (ASCII-DLE) attached to a group
    * there MUST NOT be a dataset or attribute with value np.void(b'\x7f') (ASCII-DEL)

## Overlay Access Algorithm

**Input:**

* valid container sequence (base container and patches in ascending order),
* path to a dataset/group node,
* optional: attribute key.

**Output:** Patch index of container that has the most recent version of the path/attr.

The corresponding container will immediately give the desired value in case of attributes
and datasets and is the lower-bound container for search of the set of children (in case
of groups) or all attributes (in both cases).

**Procedure *findContainer*:**

```
left_boundary := 0
For each pref in path_prefixes(path): (i.e. for /foo/bar: /, /foo, /foo/bar)
    i := len(container)-1 (most recent patch index)
    While i >= left_boundary:
        If pref in container[i]:
            val = container[i][pref]
            If val is a DEL node:
                raise KeyError (most recent update is that this path is deleted!)
            If val is dataset and pref != path:
                raise KeyError (cannot go "deeper", most recent node is not a group!)
            If val is not pass-through (i.e. not a "DLE-marked" group):
                left_boundary := i
        i := i - 1
    i := i + 1
    If pref not in container[i] or container[i][pref] is pass-through:
        raise KeyError (this node does not exist, at least since last substitution)

If not requesting an attribute:
    return left_boundary
Else:
    (similar loop as above)
    find most recent value between left_boundary and most recent patch
    If not found: throw KeyError
    If attribute value is DEL: throw KeyError
    return left_boundary

**Procedure *mostRecentNode*:**
as above

**Procedure *listChildren*:**
Note: basically same for *listAttributes*

Input: path, left_boundary

Go down to left_boundary and collect non-passthrough children
without overwriting (we hit the most recent one first)

Wrapper builds overlay tree storing its own left boundary and that of its children

once we have that tree, assembly is rather simple... just create most recent
non-del leaves and attributes
