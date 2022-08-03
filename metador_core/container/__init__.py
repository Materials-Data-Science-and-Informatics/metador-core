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
from __future__ import annotations

from itertools import takewhile
from typing import (
    Dict,
    ItemsView,
    Iterator,
    KeysView,
    List,
    Mapping,
    Optional,
    Set,
    Type,
    TypeVar,
    Union,
    ValuesView,
    cast,
)
from uuid import UUID, uuid1

import h5py
import wrapt

from metador_core.ih5.protocols import (
    H5DatasetLike,
    H5FileLike,
    H5GroupLike,
    H5NodeLike,
)

from ..ih5.container import IH5Record
from ..ih5.overlay import H5Type, node_h5type
from ..plugins import installed
from ..schema import MetadataSchema, PGSchema
from ..schema.core import PluginPkgMeta, PluginRef
from . import utils as M


class UnsupportedOperationError(AttributeError):
    """Subclass to distinguish between actually missing attribute and unsupported one."""


class ROAttributeManager(wrapt.ObjectProxy):
    """Wrapper for AttributeManager-like objects to protect from accidental mutation.

    Returned from nodes that are marked as read_only.
    """

    __wrapped__: Mapping

    # read-only methods listed in h5py AttributeManager API:
    _self_ALLOWED = {"keys", "values", "items", "get"}
    _self_MSG = "This attribute set belongs to a node marked as read_only!"

    def __getattr__(self, key: str):
        if hasattr(self.__wrapped__, key) and key not in self._self_ALLOWED:  # RAW
            raise RuntimeError(self._self_MSG)
        return getattr(self.__wrapped__, key)  # RAW

    # this is not intercepted by getattr
    def __setitem__(self, key: str, value):
        raise RuntimeError(self._self_MSG)

    # this is not intercepted by getattr
    def __delitem__(self, key: str, value):
        raise RuntimeError(self._self_MSG)

    def __repr__(self) -> str:
        return repr(self.__wrapped__)  # RAW


class MetadorNode(wrapt.ObjectProxy):
    """Wrapper for h5py and IH5 Groups and Datasets providing Metador-specific features.

    In addition to the Metadata management, also provides helpers to reduce possible
    mistakes in implementing interfaces by allowing to mark nodes as read_only
    (regardless of the writability of the underlying opened container) and as
    local_only (restricting access to data and metadata that is not below this node).
    """

    __wrapped__: H5NodeLike

    def __init__(self, mc: MetadorContainer, node: H5NodeLike, **kwargs):
        super().__init__(node)
        self._self_container: MetadorContainer = mc

        lp = kwargs.pop("local_parent", None)
        lo = bool(kwargs.pop("local_only", False))
        ro = bool(kwargs.pop("read_only", False))
        self._self_local_parent: Optional[MetadorGroup] = lp
        self._self_local_only: bool = lo
        self._self_read_only: bool = ro

        if kwargs:
            raise ValueError(f"Unknown keyword arguments: {kwargs}")

    def _guard_path(self, path: str):
        if M.is_internal_path(path):
            msg = f"Trying to use a Metador-internal path: '{path}'"
            raise ValueError(msg)
        if self.local_only and path[0] == "/":
            msg = f"Node is marked as local_only, cannot use absolute path '{path}'!"
            raise ValueError(msg)

    @property
    def local_only(self) -> bool:
        """Return whether this node is local-only."""
        return self._self_local_only

    @property
    def read_only(self) -> bool:
        """Return whether this node is read-only.

        If True, no mutating methods should be called and will be prevented if possible.
        If this is a Group, will inherit this status to children.
        """
        return self._self_read_only

    def _guard_mut_method(self, method: str = "this method"):
        if self.read_only:
            msg = f"Cannot use method '{method}', the node is marked as read_only!"
            raise RuntimeError(msg)

    def _child_node_kwargs(self):
        """Return kwargs to be passed to a child node.

        Ensures that read_only and local_only status is passed down correctly.
        """
        return {
            "read_only": self.read_only,
            "local_only": self.local_only,
            "local_parent": self if self.local_only else None,
        }

    def _wrap_if_node(self, val):
        """Wrap value into a metador node wrapper, if it is a suitable group or dataset."""
        ntype = node_h5type(val)
        kwargs = self._child_node_kwargs()
        if ntype == H5Type.group:
            return MetadorGroup(self._self_container, val, **kwargs)
        elif ntype == H5Type.dataset:
            return MetadorDataset(self._self_container, val, **kwargs)
        else:
            return val

    def _destroy_meta(self, unlink_in_toc: bool = True):
        """Destroy all attached metadata at and below this node."""
        self.meta._destroy(unlink_in_toc=unlink_in_toc)

    # need that to add our new methods

    def __dir__(self):
        names = set.union(
            *map(
                lambda x: set(x.__dict__.keys()),
                takewhile(lambda x: issubclass(x, MetadorNode), type(self).mro()),
            )
        )
        return list(set(super().__dir__()).union(names))

    # make wrapper transparent

    def __repr__(self):
        return repr(self.__wrapped__)  # RAW

    # added features

    def restrict(self, **kwargs) -> MetadorNode:
        """Restrict this object to be local_only or read_only.

        Pass local_only=True and/or read_only=True to enable the restriction.

        local_only means that the node may not access the parent or file objects.
        read_only means that mutable actions cannot be done (even if container is mutable).
        """
        # can only set, but not unset! (unless doing it on purpose with the attributes)
        ro = bool(kwargs.pop("read_only", None))
        lo = bool(kwargs.pop("local_only", None))
        self._self_read_only = self._self_read_only or ro
        self._self_local_only = self._self_local_only or lo
        if lo:  # a child was set explicitly as local, not by inheritance
            # so we remove its ability to go to the (also localized) parent
            self._self_local_parent = None
        if kwargs:
            raise ValueError(f"Unknown keyword arguments: {kwargs}")
        return self

    @property
    def meta(self) -> MetadorMeta:
        """Access the interface to metadata attached to this node."""
        return MetadorMeta(self)

    # wrap existing methods as needed

    @property
    def name(self) -> str:
        # just for type checker not to complain
        return self.__wrapped__.name  # RAW

    @property
    def attrs(self):
        if self.read_only:
            return ROAttributeManager(self.__wrapped__.attrs)  # RAW
        return self.__wrapped__.attrs  # RAW

    @property
    def parent(self) -> MetadorGroup:
        if self.local_only:
            if self._self_local_parent is None:
                # see https://github.com/GrahamDumpleton/wrapt/issues/215
                return None  # type: ignore
            else:
                # allow child nodes of local-only nodes to go up to the marked parent
                return self._self_local_parent

        return MetadorGroup(
            self._self_container,
            self.__wrapped__.parent,  # RAW
            **self._child_node_kwargs(),
        )

    @property
    def file(self) -> MetadorContainer:
        if self.local_only:
            # see https://github.com/GrahamDumpleton/wrapt/issues/215
            return None  # type: ignore
        return self._self_container

    @property
    def container_uuid(self) -> UUID:
        uuid = self._self_container.__wrapped__[M.METADOR_UUID_PATH]
        uuid_ds = cast(H5DatasetLike, uuid)  # RAW
        return UUID(uuid_ds[()].decode("utf-8"))


# TODO: can this be done somehow with wrapt.decorator but still without boilerplate?
# problem is it wants a function, but we need to look it up by name first
# so we hand-roll the decorator for now.
def _wrap_method(method: str, is_read_only_method: bool = False):
    """Wrap a method called on a HDF5 entity.

    Prevents user from creating or deleting reserved entities/names by hand.
    Ensures that a wrapped Group/Dataset is returned.

    If is_read_only=False and the object is read_only, refuses to call the method.
    """

    def wrapped_method(obj, name, *args, **kwargs):
        # obj will be the self of the wrapper instance
        obj._guard_path(name)
        if not is_read_only_method:
            obj._guard_mut_method(method)
        ret = getattr(obj.__wrapped__, method)(name, *args, **kwargs)  # RAW
        return obj._wrap_if_node(ret)

    return wrapped_method


# classes of h5py reference-like types (we don't support that)
_H5_REF_TYPES = [h5py.HardLink, h5py.SoftLink, h5py.ExternalLink, h5py.h5r.Reference]


class MetadorGroup(MetadorNode):
    """Wrapper for a HDF5 Group."""

    __wrapped__: H5GroupLike

    def _destroy_meta(self, unlink_in_toc: bool = True):
        """Destroy all attached metadata at and below this node (recursively)."""
        # destroy metadata at this node (group itself)
        super()._destroy_meta(unlink_in_toc=unlink_in_toc)

        # recurse down the group.
        # must collect first, then delete (otherwise delete during iteration issue)
        nodes_with_meta: List[MetadorNode] = []

        def collect_meta(_, nd):
            if len(nd.meta):
                nodes_with_meta.append(nd)

        self.visititems(collect_meta)
        for nd in nodes_with_meta:
            nd.meta._destroy(unlink_in_toc=unlink_in_toc)

    # these access entities in read-only way:

    get = _wrap_method("get", is_read_only_method=True)
    __getitem__ = _wrap_method("__getitem__", is_read_only_method=True)

    # these just create new entities with no metadata attached:

    create_group = _wrap_method("create_group")
    require_group = _wrap_method("require_group")
    create_dataset = _wrap_method("create_dataset")
    require_dataset = _wrap_method("require_dataset")

    def __setitem__(self, name, value):
        if any(map(lambda x: isinstance(value, x), _H5_REF_TYPES)):
            raise ValueError(f"Unsupported reference type: {type(value).__name__}")

        return _wrap_method("__setitem__")(self, name, value)

    # following all must be filtered to hide metador-specific structures:

    # must wrap nodes passed into the callback function and filter visited names
    def visititems(self, func):
        def wrapped_func(name, node):
            if M.is_internal_path(node.name):
                return  # skip path/node
            return func(name, self._wrap_if_node(node))

        return self.__wrapped__.visititems(wrapped_func)  # RAW

    # paths passed to visit also must be filtered, so must override this one too
    def visit(self, func):
        def wrapped_func(name, _):
            return func(name)

        return self.visititems(wrapped_func)

    # following also depend on the filtered sequence, directly
    # filter the items, derive other related functions based on that

    def items(self):
        return (
            (k, self._wrap_if_node(v))
            for k, v in self.__wrapped__.items()  # RAW
            if not M.is_internal_path(v.name)
        )

    def values(self):
        return map(lambda x: x[1], self.items())

    def keys(self):
        return map(lambda x: x[0], self.items())

    def __iter__(self):
        return iter(self.keys())

    def __len__(self):
        return len(list(self.keys()))

    def __contains__(self, name: str):
        self._guard_path(name)
        if name[0] == "/" and self.name != "/":
            return name in self["/"]
        segs = name.lstrip("/").split("/")
        has_first_seg = segs[0] in self.keys()
        if len(segs) == 1:
            return has_first_seg
        else:
            return "/".join(segs[1:]) in self[segs[0]]

    # these we can take care of but are a bit more tricky to think through

    def __delitem__(self, name: str):
        self._guard_mut_method("__delitem__")
        self._guard_path(name)

        node = self[name]
        # clean up metadata (recursively, if a group)
        node._destroy_meta()
        # kill the actual data
        return _wrap_method("__delitem__")(self, name)

    def move(self, source: str, dest: str):
        self._guard_mut_method("move")
        self._guard_path(source)
        self._guard_path(dest)

        src_metadir = self[source].meta._base_dir
        # if actual data move fails, an exception will prevent the rest
        self.__wrapped__.move(source, dest)  # RAW

        # if we're here, no problems -> proceed with moving metadata
        dst_node = self[dest]
        if isinstance(dst_node, MetadorDataset):
            dst_metadir = dst_node.meta._base_dir
            # dataset has its metadata stored in parallel -> need to take care of it
            meta_base = dst_metadir
            if src_metadir in self.__wrapped__:  # RAW
                self.__wrapped__.move(src_metadir, dst_metadir)  # RAW
        else:
            # directory where to fix up metadata object TOC links
            # when a group was moved, all metadata is contained in dest -> search it
            meta_base = dst_node.name

        # re-link metadata object TOC links
        if meta_base in self.__wrapped__:  # RAW
            self._self_container.toc._find_missing_links(
                meta_base, repair=True, update=True
            )

    def copy(
        self,
        source: Union[str, MetadorGroup, MetadorDataset],
        dest: Union[str, MetadorGroup],
        **kwargs,
    ):
        self._guard_mut_method("copy")

        # get source node and its name without the path and its type
        src_node: MetadorNode
        if isinstance(source, str):
            self._guard_path(source)
            src_node = self[source]
        elif isinstance(source, MetadorNode):
            src_node = source
        else:
            raise ValueError("Copy source must be path, Group or Dataset!")
        src_is_dataset: bool = isinstance(src_node, MetadorDataset)
        src_name: str = src_node.name.split("/")[-1]
        # user can override name at target
        dst_name: str = kwargs.pop("name", src_name)

        # fix up target path
        dst_path: str
        if isinstance(dest, str):
            self._guard_path(dest)
            dst_path = dest
        elif isinstance(dest, MetadorGroup):
            dst_path = dest.name + f"/{dst_name}"
        else:
            raise ValueError("Copy dest must be path or Group!")

        # get other allowed options
        without_attrs: bool = kwargs.pop("without_attrs", False)
        without_meta: bool = kwargs.pop("without_meta", False)
        if kwargs:
            raise ValueError(f"Unknown keyword arguments: {kwargs}")

        # perform copy
        copy_kwargs = {
            "name": None,
            "shallow": False,
            "expand_soft": True,
            "expand_external": True,
            "expand_refs": True,
            "without_attrs": without_attrs,
        }
        self.__wrapped__.copy(source, dst_path, **copy_kwargs)  # RAW
        dst_node = self[dst_path]  # exists now

        if src_is_dataset and not without_meta:
            # because metadata lives in parallel group, need to copy separately
            src_meta: str = src_node.meta._base_dir
            dst_meta: str = dst_node.meta._base_dir  # will not exist yet
            self.__wrapped__.copy(src_meta, dst_meta, **copy_kwargs)  # RAW
            # register in TOC
            self._self_container.toc._find_missing_links(dst_meta, repair=True)
        if not src_is_dataset:
            if without_meta:
                # need to destroy copied metadata copied with the source group
                # but keep TOC links (they point to original copy!)
                dst_node._destroy_meta(unlink_in_toc=False)
            else:
                # register copied metadata objects under new uuids
                self._self_container.toc._find_missing_links(dst_path, repair=True)

    # anything else we simply won't support
    def __getattr__(self, key):
        if hasattr(self.__wrapped__, key):  # RAW
            raise UnsupportedOperationError(key)
        else:
            msg = f"'{type(self).__name__}' object has no attribute '{key}'"
            raise AttributeError(msg)


class MetadorDataset(MetadorNode):
    """Metador wrapper for a HDF5 Dataset."""

    __wrapped__: H5DatasetLike

    # manually assembled from public methods h5py.Dataset provides
    _self_FORBIDDEN = {"resize", "make_scale", "write_direct", "flush"}

    # prevent mutating method calls of node is marked as read_only

    def __setitem__(self, *args, **kwargs):
        self._guard_mutable_method("__setitem__")
        return self.__wrapped__.__setitem__(*args, **kwargs)  # RAW

    def __getattr__(self, key):
        if self.read_only and key in self._self_FORBIDDEN:
            self._guard_mutable_method(key)
        return getattr(self.__wrapped__, key)  # RAW


# NOTE: one could try writing a ZIP wrapper providing the minimal HDF5 like API and
# to make it work it just needs to be registered for H5Type and implement the protocol


class MetadorContainer(wrapt.ObjectProxy):
    """Wrapper class adding Metador container interface to h5py.File-like objects.

    The wrapper ensures that any actions done to IH5Records through this interface
    also work with plain h5py.Files.

    There are no guarantees about behaviour with h5py methods not supported by IH5Records.
    """

    __wrapped__: H5FileLike

    def __init__(self, obj):
        if not isinstance(obj, h5py.File) and not isinstance(obj, IH5Record):
            raise ValueError("Passed object muss be an h5py.File or a IH5(MF)Record!")
        super().__init__(obj)

        ver = self.spec_version if M.METADOR_VERSION_PATH in self.__wrapped__ else None
        if ver is None and self.mode == "r":
            msg = "Container is read-only and does not look like a Metador container! "
            msg += "Please open in writable mode to initialize Metador structures!"
            raise ValueError(msg)
        if ver is not None and ver >= [2]:
            msg = "Unsupported Metador container version: {ver}"
            raise ValueError(msg)

        if ver is None:  # writable + fresh -> mark as a metador container
            self.__wrapped__[M.METADOR_VERSION_PATH] = M.METADOR_SPEC_VERSION  # RAW

        # add UUID if container does not have one yet
        if M.METADOR_UUID_PATH not in self.__wrapped__:
            self.__wrapped__[M.METADOR_UUID_PATH] = str(uuid1())  # RAW

        # if we are here, we should have an existing metador container, or a fresh one
        self._self_toc = MetadorContainerTOC(self)

    # not clear if we want these in the public interface. keep this private for now.

    def _find_orphan_meta(self) -> List[str]:
        """Return list of paths to metadata that has no corresponding user node anymore."""
        ret: List[str] = []

        def collect_orphans(name: str):
            if M.is_meta_base_path(name):
                if M.to_data_node_path(name) not in self:
                    ret.append(name)

        self.__wrapped__.visit(collect_orphans)  # RAW
        return ret

    def _repair(self, remove_orphans: bool = False):
        """Repair container structure on best-effort basis.

        This will ensure that the TOC points to existing metadata objects
        and that all metadata objects are listed in the TOC.

        If remove_orphans is set, will erase metadata not belonging to an existing node.

        Notice that missing schema plugin dependency metadata cannot be restored.
        """
        if remove_orphans:
            for path in self._find_orphan_meta():
                del self.__wrapped__[path]  # RAW
        self.toc._find_broken_links(repair=True)
        self.toc._find_missing_links("/", repair=True)

    # ---- new container-level interface ----

    @property
    def spec_version(self) -> List[int]:
        """Return Metador container specification version for this container."""
        ver = cast(H5DatasetLike, self.__wrapped__[M.METADOR_VERSION_PATH])  # RAW
        return list(map(int, ver[()].decode("utf-8").split(".")))

    @property
    def toc(self) -> MetadorContainerTOC:
        """Access interface to Metador metadata object index."""
        return self._self_toc

    # ---- pass through HDF5 group methods to a wrapped root group instance ----

    def __enter__(self):
        self.__wrapped__.__enter__()  # RAW
        return self  # return the MetadorContainer back, not the raw thing

    # overriding method of IH5Record, but a new method for h5py.File
    # used for forwarding group methods
    def _root_group(self) -> MetadorGroup:
        return MetadorGroup(self, self.__wrapped__["/"])  # RAW

    def __getattr__(self, key: str):
        # takes care of forwarding all non-special methods
        try:
            return getattr(self._root_group(), key)  # try passing to wrapped group
        except UnsupportedOperationError as e:
            raise e  # group has that, but we chose not to support it
        except AttributeError:
            # otherwise pass to file object
            return getattr(self.__wrapped__, key)  # RAW

    # we want these also to be forwarded to the wrapped group, not the raw object:

    def __getitem__(self, key: str) -> MetadorGroup:
        return self._root_group()[key]

    def __setitem__(self, key: str, value):
        self._root_group()[key] = value

    def __delitem__(self, key: str):
        del self._root_group()[key]

    def __iter__(self):
        return iter(self._root_group())

    def __contains__(self, key: str) -> bool:
        return key in self._root_group()

    def __len__(self) -> int:
        return len(self._root_group())

    # need that to add our new methods

    def __dir__(self):
        return list(set(super().__dir__()).union(type(self).__dict__.keys()))

    # make wrapper transparent

    def __repr__(self) -> str:
        return repr(self.__wrapped__)  # RAW


# --------
# Now we come to the main part - interfacing with metadata in containers
# --------


_SCHEMAS = installed.group("schema", PGSchema)

S = TypeVar("S", bound=MetadataSchema)


class MetadorMeta:
    """Interface to Metador metadata objects stored at a single HDF5 node."""

    def __init__(self, node: MetadorNode):
        self._mc = node._self_container  # needed for read/write access
        self._read_only = node.read_only  # inherited from node marker
        # used all over the place, so precompute once:
        self._base_dir = M.to_meta_base_path(
            node.name, is_dataset=isinstance(node, MetadorDataset)
        )

    def _destroy(self, unlink_in_toc: bool = True):
        """Unregister and delete all metadata objects attached to this node."""
        for schema_name in iter(self):
            self._delitem(schema_name, unlink_in_toc)

    def _meta_schema_dir(self, schema_name: str = "") -> Optional[str]:
        """Return full hierarchical path to a schema name."""
        if not schema_name:
            return self._base_dir
        # try to resolve parent path based on found structure
        pp: str = self._mc.toc._parent_path(schema_name) or ""
        return self._base_dir + "/" + pp

    def _get_raw(self, schema_name: str):
        """Return raw dataset node for an exact schema name."""
        dir_path = self._meta_schema_dir(schema_name)
        if dir_path is None:
            return None  # unknown schema
        dir = self._mc.__wrapped__.get(dir_path)  # RAW
        if dir is None:
            return None  # non-existing metadata
        for name, node in dir.items():
            if name[0] == "=":  # metadata dataset byte objects start with =
                return node  # return first match

    def _is_unknown_schema(self, schema_name: str) -> bool:
        # we have no such schema in our plugin environment
        unknown_nonexisting: bool = schema_name not in _SCHEMAS.keys()
        # we may have such a schema with that name, but its not the right one!
        unknown_but_existing: bool = schema_name in self._mc.toc.unknown_schemas
        return unknown_nonexisting or unknown_but_existing

    def _guard_unknown_schema(self, schema_name: str):
        if self._is_unknown_schema(schema_name):
            raise ValueError(f"Unknown or incompatible schema: {schema_name}")

    def _guard_read_only(self):
        if self._read_only:
            raise ValueError("This node is marked as read_only!")

    # public interface:

    def get_bytes(self, schema_name: str) -> Optional[bytes]:
        """Get bytes for an object belonging (exactly!) to the given schema, if present.

        This also works for unknown schemas (we can retrieve, but cannot parse those).
        """
        if (ret := self._get_raw(schema_name)) is not None:
            return ret[()]
        return None

    def find(self, schema_name: str = "", **kwargs) -> Set[str]:
        """List schema names of all attached metadata objects with given schema as a base.

        This will include possibly unknown schema names.

        Only the physically embedded instances are included.
        For transitive closure, add schema names along the parent_path of the schemas.
        """
        include_parents = kwargs.pop("include_parents", False)
        if kwargs:
            raise ValueError(f"Unknown keyword arguments: {kwargs}")

        basepath = self._meta_schema_dir(schema_name)
        if not basepath or basepath not in self._mc.__wrapped__:  # RAW
            # no metadata subdirectory -> no objects (important case!)
            return set()
        grp = self._mc.__wrapped__.require_group(basepath)  # RAW

        entries: Set[str] = set()

        def collect(path):
            # path is relative to basepath
            segs = path.split("/")
            # = is marker for object, use to distinguish from a group
            # (entry points may not contain = symbols by setuptools spec)
            if segs[-1][0] == "=":
                matching_schema = segs[-2] if len(segs) > 1 else schema_name
                entries.add(matching_schema)
                if include_parents:
                    entries.update(segs[:-1])
                    if schema_name:
                        entries.add(schema_name)

        grp.visit(collect)  # RAW
        return entries

    def _delitem(self, schema_name: str, unlink_in_toc: bool = True):
        self._guard_read_only()
        node = self._get_raw(schema_name)
        if node is None:
            raise KeyError(schema_name)

        # unregister in TOC (will also clean up empty dirs and deps there
        uuid = M.split_meta_obj_path(node.name)[-1]
        if unlink_in_toc:
            self._mc.toc._unregister_link(uuid)

        # clean up empty dirs at the in the node metadata dir
        parent_node = node.parent
        del self._mc.__wrapped__[node.name]  # RAW
        while parent_node.name != self._base_dir:
            if len(parent_node):
                break  # non-empty
            empty_group_path = parent_node.name
            parent_node = parent_node.parent
            del self._mc.__wrapped__[empty_group_path]  # RAW
        # remove the base dir itself, if empty
        if parent_node.name == self._base_dir and not len(parent_node):
            del self._mc.__wrapped__[self._base_dir]  # RAW

    # start with methods having non-transitive semantics (considers exact schemas)

    def __iter__(self) -> Iterator[str]:
        """Iterate listing schema names of all actually attached metadata objects.

        This means, no transitive parent schemas are included.
        """
        return iter(self.find())

    def __len__(self) -> int:
        """Return number of all actually attached metadata objects.

        This means, transitive parent schemas are not counted.
        """
        return len(self.find())

    def keys(self) -> KeysView[str]:
        return self.find()  # type: ignore

    def values(self) -> ValuesView[MetadataSchema]:
        return set(map(lambda x: x[1], self.items()))  # type: ignore

    def items(self) -> ItemsView[str, MetadataSchema]:
        return {name: self.get[name] for name in self.find()}  # type: ignore

    def __delitem__(self, schema_name: str):
        return self._delitem(schema_name, unlink_in_toc=True)

    # all following only work with known (i.e. registered/correct) schema:

    def __setitem__(self, schema_name: str, value: MetadataSchema):
        self._guard_read_only()
        self._guard_unknown_schema(schema_name)
        if self._get_raw(schema_name):
            raise ValueError(f"Metadata object for schema {schema_name} exists!")

        schema_class: Type[MetadataSchema] = _SCHEMAS[schema_name]  # type: ignore
        if not isinstance(value, schema_class):
            raise ValueError(f"Metadata object is invalid for schema {schema_name}!")

        obj_path = self._mc.toc._register_link(self._base_dir, schema_name)
        self._mc.__wrapped__[obj_path] = bytes(value)  # RAW

        # notify dep manager to register possibly new dependency.
        # we do it here instead of in toc._register_link, because _register_link
        # is also used in cases where we might not have the schema installed,
        # e.g. when fixing the TOC.
        self._mc.toc._notify_used_schema(schema_name)

    # following have transitive semantics (i.e. logic also works with parent schemas)

    def get(
        self, schema_name: str, schema_class: Type[S] = MetadataSchema
    ) -> Optional[S]:
        """Get a parsed metadata object (if it exists) matching the given known schema.

        Will also accept a child schema object and parse it as the parent schema.
        If multiple suitable objects are found, picks the alphabetically first to parse it.
        """
        self._guard_unknown_schema(schema_name)
        # don't include parents, because we need to parse some physical instance
        candidates = self.find(schema_name)
        if not candidates:
            return None
        # NOTE: here we might get also data from an unknown child schema
        # but as the requested schema is known and compatible, we know it must work!
        dat: Optional[bytes] = self.get_bytes(next(iter(sorted(candidates))))
        assert dat is not None  # getting bytes of a found candidate!
        # parse with the correct schema and return the instance object
        return cast(S, _SCHEMAS[schema_name].parse_raw(dat))

    def __getitem__(self, schema_name: str) -> MetadataSchema:
        """Like get, but will raise KeyError on failure."""
        ret = self.get(schema_name)
        if ret is None:
            raise KeyError(schema_name)
        return ret

    def __contains__(self, schema_name: str) -> bool:
        """Check whether a suitable metadata object for given known schema exists."""
        self._guard_unknown_schema(schema_name)
        return bool(self.find(schema_name, include_parents=True))


class MetadorContainerTOC:
    """Interface to the Metador metadata index of a container."""

    def _find_broken_links(self, repair: bool = False) -> List[str]:
        """Return list of UUIDs in TOC not pointing to an existing metadata object.

        Will use loaded cache of UUIDs and check them, without scanning the container.

        If repair is set, will remove those broken links.
        """
        broken = []
        for uuid in self._toc_path_from_uuid.keys():
            target = self._resolve_link(uuid)
            if target not in self._container.__wrapped__:  # RAW
                broken.append(uuid)
        if repair:
            for uuid in broken:
                self._unregister_link(uuid)
        return broken

    def _find_missing_links(
        self, path: str, repair: bool = False, update: bool = False
    ) -> List[str]:
        """Return list of paths to metadata objects not listed in TOC.

        Path must point to a group.

        If repair is set, will create missing entries in TOC.

        If update is set, will reassign existing UUID if possible (this only makes
        sense when objects have been moved, otherwise it simply breaks the other link)
        """
        missing = []

        def collect_missing(name, node):
            if not M.is_internal_path(node.name, M.METADOR_META_PREF):
                return  # not a metador metadata path
            leaf = name.split("/")[-1]
            if not leaf[0] == "=":
                return  # metadata objects are named =SOME_UUID
            uuid = leaf[1:]

            known = uuid in self._toc_path_from_uuid
            # UUID collision - used in TOC, but points to other object
            # (requires fixing up the name of this object / new UUID)
            # but technically THIS object IS missing in the TOC!
            collision = known and self._resolve_link(uuid) != node.name

            if not known or collision:
                missing.append(node.name)  # absolute paths!

        rawcont = self._container.__wrapped__
        rawcont[path]  # raises exception if not existing  # RAW
        grp = rawcont.require_group(path)  # ensure its a group # RAW
        grp.visititems(collect_missing)  # RAW

        if not repair:
            return missing

        # Repair links (objects get new UUIDs, unless update is true)
        for path in missing:
            meta_dir, schema_path, schema_name, uuid = M.split_meta_obj_path(path)

            if self._parent_path(schema_name) is None:  # schema not in lookup table?
                # add the unknown schema parent path based on inferred info
                self._parent_paths[schema_path] = schema_name
                self._unknown_schemas.add(schema_name)

            if update and uuid in self._toc_path_from_uuid:
                self._update_link(uuid, path)  # update target for existing UUID
            else:  # link the object with a new UUID
                target_path = self._register_link(meta_dir, schema_name)
                self._container.__wrapped__.move(path, target_path)  # RAW

        return missing

    # ---- link management ----

    def _parent_path(self, schema_name: str) -> Optional[str]:
        """Like PGSchema.parent_path, but works also with unknown schemas in container.

        Only used for internal plumbing.
        """
        pp: Optional[str] = self._parent_paths.get(schema_name)
        if pp is None and schema_name in _SCHEMAS.keys():
            pp = "/".join(_SCHEMAS.parent_path(schema_name))
        return pp

    def _fresh_uuid(self) -> str:
        """Return a UUID string not used for a metadata object in the container yet."""
        fresh = False
        ret = ""
        # NOTE: here a very unlikely race condition is present if parallelized
        while not fresh:
            ret = str(uuid1())
            fresh = ret not in self._toc_path_from_uuid
        self._toc_path_from_uuid[ret] = ""  # not assigned yet, but blocked
        return ret

    def _register_link(self, node_meta_base: str, schema_name: str) -> str:
        """Create a link for a metadata object, returning its target location.

        The link points to the returned path (object must be placed there).
        """
        schema_path = self._parent_path(schema_name)
        assert schema_path is not None
        link_name = self._fresh_uuid()
        toc_path = f"{M.METADOR_TOC_PATH}/{schema_path}/={link_name}"
        target_path = f"{node_meta_base}/{schema_path}/={link_name}"
        self._toc_path_from_uuid[link_name] = toc_path
        # create (broken) link
        self._container.__wrapped__[toc_path] = target_path  # RAW
        return target_path

    def _unregister_link(self, uuid: str):
        """Unregister metadata object in TOC given its UUID.

        Will remove the object and clean up empty directories in the TOC.
        """
        # delete the link itself
        toc_link_path = self._toc_path_from_uuid[uuid]
        link_node = self._container.__wrapped__[toc_link_path]  # RAW
        parent_node = link_node.parent
        del self._container.__wrapped__[toc_link_path]  # RAW

        # clean up empty groups
        while parent_node.name != M.METADOR_TOC_PATH:
            if len(parent_node):
                break  # non-empty
            empty_group = parent_node.name
            parent_node = parent_node.parent
            del self._container.__wrapped__[empty_group]  # RAW

            # notify dep manager to prune deps if no schema they provide is used
            # do it here because the TOC tracks globally all object metadata
            # so it sees here when its not used anymore.
            schema_name = empty_group.split("/")[-1]
            self._notify_unused_schema(schema_name)

        # remove the TOC dir itself, if empty
        if parent_node.name == M.METADOR_TOC_PATH and not len(parent_node):
            del self._container.__wrapped__[M.METADOR_TOC_PATH]  # RAW
        # now the uuid counts as "free" again
        del self._toc_path_from_uuid[uuid]

    def _resolve_link(self, uuid: str) -> str:
        """Get the path a uuid in the TOC points to."""
        toc_link_path = self._toc_path_from_uuid[uuid]
        dataset = cast(H5DatasetLike, self._container.__wrapped__[toc_link_path])
        return dataset[()].decode("utf-8")  # RAW

    def _update_link(self, uuid: str, new_target: str):
        """Update target of an existing link to point to a new location."""
        toc_link_path = self._toc_path_from_uuid[uuid]
        del self._container.__wrapped__[toc_link_path]  # RAW
        self._container.__wrapped__[toc_link_path] = new_target  # RAW

    # ---- schema provider package dependency management ----

    @classmethod
    def _dep_node_path(cls, pkg_name: str) -> str:
        return f"{M.METADOR_DEPS_PATH}/{pkg_name}"

    def _notify_used_schema(self, schema_name: str):
        """Notify that a schema is used in the container (metadata object is created/updated).

        If no dependency is tracked yet, will add it. If it is, will update to the one
        from the environment.

        This assumes that the already existing schemas and the new one are compatible!
        """
        env_pkg_info = _SCHEMAS.provider(schema_name)
        pkg_name = env_pkg_info.name

        # update/create metadata entry in container
        curr_info = self._pkginfos.get(pkg_name)

        if curr_info != env_pkg_info:
            pkg_node_path = self._dep_node_path(pkg_name)
            if curr_info is not None:
                # remove old dep metadata first
                del self._container.__wrapped__[pkg_node_path]
            # add new dep metadata to container
            self._container.__wrapped__[pkg_node_path] = bytes(env_pkg_info)

        # update/create dependency in cache
        self._pkginfos[pkg_name] = env_pkg_info
        self._provider[schema_name] = pkg_name

        # make sure schema is tracked as "used"
        if curr_info is None:
            self._used[pkg_name] = set()
        self._used[pkg_name].add(schema_name)

    def _notify_unused_schema(self, schema_name: str):
        """Notify that a schema is not used at any container node anymore.

        If after that no schema of a listed dep package is used,
        this dependency will be removed from the container.
        """
        pkg_name = self._provider.get(schema_name)
        if pkg_name is None:
            # apparently container has no package info for that schema
            # this is not allowed, but we can just ignore it to handle
            # slightly broken containers
            return

        del self._provider[schema_name]
        self._used[pkg_name].remove(schema_name)

        if not self._used[pkg_name]:
            # no schemas of its providing package are used anymore.
            # -> kill this dep in cache and in container
            del self._used[pkg_name]
            del self._pkginfos[pkg_name]
            del self._container.__wrapped__[self._dep_node_path(pkg_name)]

        rawcont = self._container.__wrapped__
        deps_grp = cast(H5GroupLike, rawcont[M.METADOR_DEPS_PATH])
        if not len(deps_grp):  # RAW
            # no package metadata -> can kill dir
            del rawcont[M.METADOR_DEPS_PATH]  # RAW

    # ---- public API ----

    def __init__(self, container: MetadorContainer):
        self._container = container

        # 1. compute parent paths based on present TOC structure
        # (we need it to efficiently traverse possibly unknown schemas/objects)
        # 2. collect metadata object uuids

        # schema name -> full/registered/parent/sequence
        self._parent_paths: Dict[str, str] = {}
        # uuid -> path in dataset
        self._toc_path_from_uuid: Dict[str, str] = {}

        def collect_schemas(path):
            schema_name = path.split("/")[-1]
            if schema_name[0] == "=":  # links/metadata entries start with =
                self._toc_path_from_uuid[
                    schema_name[1:]
                ] = f"{M.METADOR_TOC_PATH}/{path}"
            else:
                self._parent_paths[schema_name] = path

        rawcont = self._container.__wrapped__
        if M.METADOR_TOC_PATH in rawcont:  # RAW
            toc_grp = rawcont.require_group(M.METADOR_TOC_PATH)  # RAW
            toc_grp.visit(collect_schemas)  # RAW

        # 3. init structure tracking schema dependencies

        # package name -> package info:
        self._pkginfos: Dict[str, PluginPkgMeta] = {}
        # schema name -> package name:
        self._provider: Dict[str, str] = {}
        # package name -> schemas used in container
        self._used: Dict[str, Set[str]] = {}

        # parse package infos if they exist
        rawcont = self._container.__wrapped__
        if M.METADOR_DEPS_PATH in rawcont:  # RAW
            deps_grp = rawcont.require_group(M.METADOR_DEPS_PATH)  # RAW
            for name, node in deps_grp.items():  # RAW
                info = PluginPkgMeta.parse_raw(cast(H5DatasetLike, node)[()])  # RAW
                self._pkginfos[name] = info
                self._used[name] = set()
                # lookup for schema -> used package
                for schema in info.plugins[_SCHEMAS.name]:
                    self._provider[schema] = name

        # initialize tracking of used schemas
        for schema_name in self.schemas:
            # we assume that each schema has a provider dep!
            pkg_name = self._provider[schema_name]
            # add schema to list of actually used schemas provided by this dep
            self._used[pkg_name].add(schema_name)

        # 3. collect the schemas that we don't know already
        self._unknown_schemas = set(self._parent_paths.keys()) - set(_SCHEMAS.keys())

        # TODO: what about plugin name collisions?
        # should check package info and correct deps!
        # for that, the container env entry must be implemented and enforced!
        # also must be careful with parsing - need to mark colliding but wrong as unknown

    @property
    def schemas(self) -> Set[str]:
        """Return names of all schemas used in the container."""
        return set(self._provider.keys())

    @property
    def deps(self) -> Set[str]:
        """Return names of all packages used to provide schemas in the container."""
        return set(self._pkginfos.keys())

    @property
    def unknown_schemas(self) -> Set[str]:
        """Return set of schema names that are unknown or incompatible.

        Here, unknown means that a plugin is missing providing that schema name,
        whereas incompatible means that the installed plugin providing that name
        is not suitable for the embedded data using a schema of the same name.
        """
        return set(self._unknown_schemas)

    def provider(self, schema_name: str) -> PluginPkgMeta:
        """Like PluginGroup.provider, but with respect to container deps."""
        pkg_name = self._provider.get(schema_name)
        if pkg_name is None:
            msg = f"Did not find metadata of package providing schema: {schema_name}"
            raise KeyError(msg)
        return self._pkginfos[pkg_name]

    def fullname(self, schema_name: str) -> PluginRef:
        """Like PluginGroup.fullname, but with respect to container deps."""
        pkginfo = self.provider(schema_name)
        return PluginRef(
            pkg=pkginfo.name,
            pkg_version=pkginfo.version,
            group=_SCHEMAS.name,
            name=schema_name,
        )

    def query(self, schema_name: str) -> Dict[MetadorNode, MetadataSchema]:
        """Return nodes that contain a metadata object valid for the given schema."""
        ret = {}

        def collect_nodes(_, node):
            if (obj := node.meta.get(schema_name)) is not None:
                ret[node] = obj

        self._container.visititems(collect_nodes)
        return ret
