"""Metador interface to manage metadata in HDF5 containers.

Works with plain h5py.File and IH5Record subclasses and can be extended to work
with any type of archive providing the required functions in a h5py-like interface.

When assembling a container, the compliance with the Metador container specification is
ensured by using it through the MetadorContainer interface.

Technical Metador container specification (not required for users):

Metador uses only HDF5 Groups and Datasets. We call both kinds of objects Nodes.
Notice that HardLinks, SymLinks, ExternalLinks or region references cannot be used.

Users are free to lay out data in the container as they please, with one exception:
a user-defined Node MUST NOT have a name starting with "metador_".
"metador_" is a reserved prefix for Group and Dataset names used to manage
technical bookkeeping structures that are needed for providing all container features.

For each HDF5 Group or Dataset there MAY exist a corresponding
Group for Metador-compatible metadata that is prefixed with "metador_meta_".

For "/foo/bar" the metadata is to be found...
    ...in a group "/foo/metador_meta_bar", if "/foo/bar" is a dataset,
    ...in a group "/foo/bar/metador_meta_" if it is a group.
We write meta("/foo/bar") to denote that group.

Given schemas with entrypoint names X, Y and Z such that X is the parent schema of Y,
and Y is the parent schema of Z and a node "/foo/bar" annotated by a JSON object of
type Z, that JSON object MUST be stored as a newline-terminated, utf-8 encoded byte
sequence at the path meta("/foo/bar")/X/Y/Z/=UUID, where the UUID is unique in the
container.

For metadata attached to an object we expect the following to hold:

Node instance uniqueness:
Each schema MAY be instantiated explicitly for each node at most ONCE.
Collections thus must be represented on schema-level whenever needed.

Parent Validity:
Any object of a subschema MUST also be a valid instance of all its parent schemas.
The schema developers are responsible to ensure this by correct implementation
of subschemas.

Parent Consistency:
Any objects of a subtype of schema X that stored at the same node SHOULD result
in the same object when parsed as X (they agree on the "common" information).
Thus, any child object can be used to retrieve the same parent view on the data.
The container creator is responsible for ensuring this property. In case it is not
fulfilled, retrieving data for a more abstract type will yield it from ANY present subtype
instance (but always the same one, as long as the container does not change)!

If at least one metadata object it stored, a container MUST have a "/metador_toc" Group,
containing a lookup index of all metadata objects following a registered metadata schema.
This index structure MUST be in sync with the node metadata annotations.
Keeping this structure in sync is responsibility of the container interface.

This means (using the previous example) that for "/foo/bar" annotated by Z there also
exists a dataset "/metador_toc/X/Y/Z/=UUID" containing the full path to the metadata node,
i.e. "meta(/foo/bar)/X/Y/Z/=UUID". Conversely, there must not be any empty entry-point
named Groups, and all listed paths in the TOC must point to an existing node.

A valid container MUST contain a dataset /metador_version string of the form "X.Y"

A correctly implemented library supporting an older minor version MUST be able open a
container with increased minor version without problems (by ignoring unknown data),
so for a minor update of this specification only new entities may be defined.

Known technical limitations:

Due to the fact that versioning of plugins such as schemas is coupled to the versioning
of the respective Python packages, it is not (directly) possible to use two different
versions of the same schema in the same environment (with the exception of mappings, as
they may bring their own equivalent schema classes).

Minor version updates of packages providing schemas must ensure that the classes providing
schemas are backward-compatible (i.e. can parse instances of older minor versions).

Major version updates must also provide mappings migrating between the old and new schema
versions. In case that the schema did not change, the mapping is simply the identity.
"""

from .interface import MetadorContainerTOC, MetadorMeta
from .provider import ContainerProxy
from .wrappers import MetadorContainer, MetadorDataset, MetadorGroup, MetadorNode

__all__ = [
    "MetadorContainer",
    "MetadorNode",
    "MetadorGroup",
    "MetadorDataset",
    "MetadorMeta",
    "MetadorContainerTOC",
    "ContainerProxy",
]
