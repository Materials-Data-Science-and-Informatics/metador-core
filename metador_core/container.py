"""Metador interface to manage metadata in HDF5 containers.

Works with plain h5py.File and IH5Record subclasses.

The compliance with the Metador container specification is ensured by using the provided
MetadorContainer interface.

Metador container specification:

Users are free to lay out data in the container as they please, with one exception:
a user-defined entity MUST NOT have a name starting with "metador_".
"metador_" is a reserved prefix for group, dataset and attribute names.

For each HDF5 Group or Dataset there MAY exist a corresponding
directory for Metador-compatible metadata that is prefixed with "metador_meta_".

For "/foo/bar" the metadata is to be found:
    in a group "/foo/metador_meta_bar", if "/foo/bar" is a dataset,
    in a group "/foo/bar/metador_meta_" if it is a group.
We write meta("/foo/bar") to denote that group.

Given schemas with entrypoint names X, Y and Z such that X is the parent schema of Y,
and Y is the parent schema of Z and a node "/foo/bar" annotated by a JSON object of
type Z, that JSON object MUST be stored as a newline-terminated, utf-8 encoded byte
sequence at the path meta("/foo/bar")/X/Y/Z/json.

Data inheritence / subschema consistency:
Any objects of a subtype of schema X that stored at the same node MUST result
in the same object when parsed as X (they agree on the "common" information).
Thus, any child object can be used to retrieve the same parent view on the data.

A container MUST have a "/metador_toc" group, containing a lookup index of all nodes
that are annotated by an object following a registered metadata schema. This index
structure MUST be in sync with the node metadata annotations.

This means (using the previous example) that for "/foo/bar" annotated by Z there also
exists a dataset "/metador_toc/X/Y/Z/#N" containing the full path to the data node, i.e.
"/foo/bar", where N is any string (e.g. an increasing number). There is no kind of
ordering encoded in the chosen string N. The # symbol is used to distinguish node links
from child schema entry point names (a comment in the pyproject.toml file and thus
impossible as a regular entry point name). Conversely, there must not be any empty
entry-point named groups (without paths to any node) and all listed paths must point to
an existing node.

A valid container MUST contain a dataset /metador_version string of the form "X.Y"

A correctly implemented library supporting an older minor version MUST be able open a
container with increased minor version without problems (by ignoring unknown data),
so for a minor update of this specification only new entities may be defined.

Known technical and deliberate limitations:

Each node can be annotated by at most one json object of each registered schema type.
Collections thus must be represented on schema-level whenever needed.
This is a deliberate design decision.

Due to the fact that versioning of plugins such as schemas is coupled to the versioning
of the respective Python packages, it is not (directly) possible to use two different
versions of the same schema in the same environment (with the exception of mappings, as
they may bring their own equivalent schema classes).

Minor version updates of packages providing schemas must ensure that the classes providing
schemas are backward-compatible.

Major version updates must be performed in two phases. First, the new
non-backward-compatible version is registered under a new name in parallel,
and when whe major version update happens, the migration consists of simply renaming.
"""
from __future__ import annotations

from itertools import dropwhile
from uuid import uuid1
from typing import (
    Optional,
    List,
    Iterator,
    KeysView,
    ValuesView,
    ItemsView,
    Set,
    Dict,
    Tuple
)
from typing_extensions import Final

import wrapt
import h5py

from .ih5.container import IH5Record
from .ih5.protocols import H5FileLike, H5DatasetLike, H5NodeLike
from .ih5.overlay import H5Type, node_h5type
from .schema.interface import schemas, MetadataSchema

LATEST_SPEC_VERSION: Final[str] = "1.0"
"""Version of container spec created by this module."""
# NOTE: don't forget to change it when something about the container structure changes!

METADOR_PREF: Final[str] = "metador_"
"""Reserved prefix for group and dataset names."""

METADOR_META_PREF: Final[str] = METADOR_PREF + "meta_"
"""Sub-prefix for group that stores group or dataset metadata."""

METADOR_VERSION_PATH: Final[str] = f"/{METADOR_PREF}version"
"""Path of dataset with the Metador container version of the container."""

METADOR_TOC_PATH: Final[str] = f"/{METADOR_PREF}toc"
"""Path of group with the Metador metadata index structure of the container."""


def _is_metador_path(path: str, pref: str = METADOR_PREF) -> bool:
    """Returns True iff the path of this node is Metador-internal (metador_*).

    Optional argument can set a different prefix that path segments are checked for.
    """
    # first case is for relative paths, second for later path segments and absolute paths
    return path.startswith(pref) or path.find(f"/{pref}") >= 0


class ROAttributeManager(wrapt.ObjectProxy):
    """Wrapper for AttributeManager-like objects to protect from accidental mutation.

    Returned from nodes that are marked as read_only.
    """

    # read-only methods listed in h5py AttributeManager API:
    _self_ALLOWED = {"keys", "values", "items", "get", "get_id"}
    _self_MSG = "This attribute set belongs to an entity marked as read_only!"

    def __getattr__(self, key):
        if key not in self._self_ALLOWED:
            raise ValueError(self._self_MSG)
        return getattr(self.__wrapped__, key)

    # this does not go through getattr
    def __setitem__(self, key, value):
        raise ValueError(self._self_MSG)

    # this does not go through getattr
    def __delitem__(self, key, value):
        raise ValueError(self._self_MSG)

    def __repr__(self):
        return repr(self.__wrapped__)


class MetadorNode(wrapt.ObjectProxy):
    """Wrapper for h5py and IH5 Groups and Datasets providing Metador-specific features.

    In addition to the Metadata management, also provides helpers to reduce possible
    mistakes in implementing interfaces by allowing to mark nodes as read_only
    (regardless of the writability of the underlying opened container) and as
    local_only (restricting access to data and metadata that is not below this node).
    """

    def __init__(self, mc: MetadorContainer, node, **kwargs):
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

    @property
    def local_only(self) -> bool:
        """Return whether this node is local-only."""
        return self._self_local_only

    @property
    def read_only(self) -> bool:
        """Return whether this node is read-only.

        If True, no mutable methods should be called and will be prevented if possible.
        If this is a Group, will inherit this status to children.
        """
        return self._self_read_only

    def _child_node_kwargs(self):
        """Return kwargs to be passed to a child node.

        Ensures that read_only and local_only status is passed down correctly."""
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

    # make wrapper transparent

    def __repr__(self):
        return repr(self.__wrapped__)

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
    def attrs(self):
        if self.read_only:
            return ROAttributeManager(self.__wrapped__.attrs)
        return self.__wrapped__.attrs

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
            self.__wrapped__.parent,
            **self._child_node_kwargs(),
        )

    @property
    def file(self) -> MetadorContainer:
        if self.local_only:
            # see https://github.com/GrahamDumpleton/wrapt/issues/215
            return None  # type: ignore
        return self._self_container


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

        if _is_metador_path(name):
            msg = f"Trying to use {method} on a Metador-internal path: '{name}'"
            raise ValueError(msg)

        if obj.local_only and name[0] == "/":
            msg = "This entity is marked as local_only! Cannot use an absolute path!"
            raise ValueError(msg)

        if not is_read_only_method and obj.read_only:
            msg = "This entity is marked as read_only!"
            raise ValueError(msg)

        ret = getattr(obj.__wrapped__, method)(name, *args, **kwargs)
        return obj._wrap_if_node(ret)

    return wrapped_method


class MetadorGroup(MetadorNode):
    """Wrapper for a HDF5 Group."""

    # these access entities in read-only way:

    get = _wrap_method("get", is_read_only_method=True)
    __getitem__ = _wrap_method("__getitem__", is_read_only_method=True)

    # these just create new entities with no metadata attached:

    __setitem__ = _wrap_method("__setitem__")
    create_group = _wrap_method("create_group")
    require_group = _wrap_method("require_group")
    create_dataset = _wrap_method("create_dataset")
    require_dataset = _wrap_method("require_dataset")

    # following all must be filtered to hide metador-specific structures:

    # must wrap nodes passed into the callback function and filter visited names
    def visititems(self, func):
        def wrapped_func(name, node):
            if _is_metador_path(node.name):
                return  # skip path/node
            return func(name, self._wrap_if_node(node))

        return self.__wrapped__.visititems(wrapped_func)

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
            for k, v in self.__wrapped__.items()
            if not _is_metador_path(v.name)
        )

    def values(self):
        return map(lambda x: x[1], self.items())

    def keys(self):
        return map(lambda x: x[0], self.items())

    def __iter__(self):
        return iter(self.keys())

    def __len__(self):
        return len(list(self.keys()))

    def __contains__(self, key):
        return key in self.keys()

    # these we can take care of but are a bit more tricky to think through

    def __delitem__(self, key):
        node = self[key]
        # clean up metadata (recursively, if a group)
        node.meta._destroy()
        if isinstance(node, MetadorGroup):
            node.visititems(lambda _, node: node.meta._destroy())
        # kill the actual data
        return _wrap_method("__delitem__")(self, key)

    def move(self, source: str, dest: str):
        # TODO: this needs the relinking of all moved metadata objects in the TOC
        # first do move, on success move metadata
        raise NotImplementedError

    def copy(self, source, dest, **kwargs):
        # name: Optional[str] = kwargs.pop("name", None)
        # without_attrs: bool = kwargs.pop("without_attrs", False)
        # without_meta: bool = kwargs.pop("without_meta", False)
        if kwargs:
            raise ValueError(f"Unknown keyword arguments: {kwargs}")
        # TODO: if source is a MetadorNode itself,
        # this needs creation of respective metadata TOC entries.
        # might also add without_meta flag
        # also disallow setting the expand_* and set them to true, shallow=False
        raise NotImplementedError

    # anything else we simply won't support
    # def __getattr__(self, key):

    def create_virtual_dataset(self, *args, **kwargs):
        raise AttributeError("Unsupported operation.")

    def build_virtual_dataset(self):
        raise AttributeError("Unsupported operation.")


class MetadorDataset(MetadorNode):
    """Metador wrapper for a HDF5 Dataset."""

    def __setitem__(self, *args, **kwargs):
        # prevent writing if dataset is marked as RO (regardless of file open mode)
        if self.read_only:
            raise ValueError("This dataset is marked as read_only!")
        return self.__wrapped__.__setitem__(*args, **kwargs)


# NOTE: one could try writing a ZIP wrapper providing the minimal HDF5 like API and
# to make it work it just needs to be registered for H5Type and implement the protocol


class MetadorContainer(wrapt.ObjectProxy):
    """Wrapper class adding Metador container interface to h5py.File-like objects.

    The wrapper ensures that any actions done to IH5Records through this interface
    also work with plain h5py.Files.

    There are no guarantees about behaviour with h5py methods not supported by IH5Records.
    """

    def _find_orphan_meta(self) -> List[str]:
        """Return list of paths to metadata that has no corresponding user node anymore."""
        ret: List[str] = []

        def collect_orphans(name: str):
            if _is_meta_dir(name):
                if _data_path(name) not in self:
                    ret.append(name)

        self.__wrapped__.visit(collect_orphans)
        return ret

    def repair(self, keep_orphans: bool = True):
        """Repair container structure on best-effort basis.

        This will ensure that the TOC points to existing metadata objects
        and that all metadata objects are listed in the TOC.

        If keep_orphans = False, will erase metadata not belonging to an existing node.

        Notice that missing schema plugin dependency metadata cannot be restored.
        """
        if not keep_orphans:
            for path in self._find_orphan_meta():
                del self.__wrapped__[path]
        self.toc._find_broken_links(repair=True)
        self.toc._find_missing_links(repair=True)

    def __init__(self, obj: H5FileLike):
        if not isinstance(obj, h5py.File) and not isinstance(obj, IH5Record):
            raise ValueError("Passed object muss be an h5py.File or a IH5(MF)Record!")
        super().__init__(obj)

        ver = self.spec_version
        if ver is None and self.mode == "r":
            msg = "Container is read-only and does not look like a Metador container! "
            msg += "Please open in writable mode to initialize Metador structures!"
            raise ValueError(msg)
        if ver is not None and ver >= [2]:
            msg = "Unsupported Metador container version: {ver}"
            raise ValueError(msg)

        if ver is None:  # writable -> mark as a metador container
            self.__wrapped__[METADOR_VERSION_PATH] = LATEST_SPEC_VERSION

        # if we are here, we should have an existing metador container, or a fresh one
        self._self_toc = MetadorTOC(self)

    # make wrapper transparent

    def __repr__(self):
        return repr(self.__wrapped__)

    # ---- new container-level interface ----

    @property
    def spec_version(self) -> Optional[List[int]]:
        """Return Metador container specification version for this container."""
        ver = self.__wrapped__.get(METADOR_VERSION_PATH, None)
        if ver is not None:
            return list(map(int, ver[()].decode("utf-8").split(".")))

    @property
    def toc(self) -> MetadorTOC:
        """Access interface to Metador metadata object index."""
        return self._self_toc

    @property
    def deps(self):
        """Return dict from embedded schema names to the providing package information."""
        # TODO: based on env info in the TOC
        raise NotImplementedError

    # ---- pass through HDF5 group methods to a wrapped root group instance ----

    # overriding method of IH5Record, but a new method for h5py.File
    # used for forwarding group methods
    def _root_group(self) -> MetadorGroup:
        return MetadorGroup(self, self.__wrapped__["/"])

    def __getattr__(self, key):
        # takes care of forwarding all non-special methods
        try:
            return getattr(self._root_group(), key)  # try passing to wrapped group
        except AttributeError:
            return getattr(self.__wrapped__, key)  # otherwise pass to file object

    # we want these also to be forwarded to the wrapped group, not the raw object:

    def __getitem__(self, key) -> H5NodeLike:
        return self._root_group()[key]

    def __setitem__(self, key: str, value):
        self._root_group()[key] = value

    def __delitem__(self, key: str):
        del self._root_group()[key]

    def __iter__(self):
        return iter(self._root_group())

    def __contains__(self, key) -> bool:
        return key in self._root_group()

    def __len__(self) -> int:
        return len(self._root_group())


# --------
# Now we come to the main part - interfacing with metadata in containers
# --------

# some helpers


def _is_meta_dir(path: str) -> bool:
    """Return whether the path is a metadata base dir (but not an inner path!)"""
    return path.split("/")[-1].startswith(METADOR_META_PREF)

def _meta_dir(node: MetadorNode) -> str:
    """Return path to base group containing metadata for given node."""
    segs = node.name.split("/")
    if isinstance(node, MetadorDataset):
        segs[-1] = METADOR_META_PREF + segs[-1]
    elif segs == ["", ""]:  # name was "/"
        segs[-1] = METADOR_META_PREF
    else:
        segs.append(METADOR_META_PREF)
    return "/".join(segs)


def _data_path(meta_dir_path: str) -> str:
    """Reverse for _meta_dir - given a metadata group path, give correct node path.

    Path can be relative or absolute.
    Will not check validity of the passed path, assumes it is fitting the scheme!
    """
    segs = meta_dir_path.split("/")
    pl = len(METADOR_META_PREF)
    segs[-1] = segs[-1][pl:]
    if segs[-1] == "" and (len(segs) > 2 or segs[0] != ""):
        segs.pop()
    return "/".join(segs)

def _meta_obj_to_base_path(meta_obj_path: str) -> str:
    """Given a path to a stored metadata object, return the base path prefix."""
    # do we need this? jumping up the raw node parents would be quicker
    # e.g. when going through TOC results

def _meta_obj_to_toc_path(meta_obj_path: str) -> str:
    """Return correct path in TOC for the given metadata object path."""
    # NOTE: can't be inverted, but link at the returned path points back to the arg.!
    segs = list(dropwhile(lambda x: not x.startswith(METADOR_META_PREF),
                    meta_obj_path.split("/")))
    segs[0] = METADOR_TOC_PATH
    return "/".join(segs)


_SCHEMAS = schemas()


class MetadorMeta:
    """Interface to Metador metadata objects stored at a single HDF5 node."""

    def __init__(self, node: MetadorNode):
        self._node = node  # user data node for which we manage metadata
        self._mc = self._node._self_container  # main object (for quick access)
        self._base_dir = _meta_dir(node)  # group path for metadata storage

    def _destroy(self):
        """Remove all metadata objects, unlink them in TOC and remove metadata dir."""
        for schema_name in iter(self):
            del self[schema_name]  # each delete should clean itself up

    def _meta_schema_dir(self, schema_name: str = "") -> Optional[str]:
        """Return hierarchical path to a schema name."""
        if not schema_name:
            return self._base_dir
        # try to resolve parent path based on found structure
        pp: str = self._mc.toc._parent_path(schema_name) or ""
        return self._base_dir + "/" + pp

    def _get_raw(self, schema_name: str) -> Optional[H5DatasetLike]:
        """Returns raw dataset node for an exact schema name."""
        dir_path = self._meta_schema_dir(schema_name)
        if dir_path is None:
            return None  # unknown schema
        dir = self._node.__wrapped__.get(dir_path)
        if dir is None:
            return None  # non-existing metadata
        for name, node in dir.items():
            if name[0] == "=":  # metadata dataset byte objects start with =
                return node  # return first match

    # def _parse_as(self, node: H5DatasetLike, schema_name: str) -> MetadataSchema:
    #     return schemas()[schema_name].parse_raw(node[()])  # type: ignore

    def _is_unknown_schema(self, schema_name: str) -> bool:
        # we have no such schema in our plugin environment
        unknown_nonexisting: bool = schema_name not in _SCHEMAS.keys()
        # we may have such a schema with that name, but its not the right one!
        unknown_but_existing: bool = schema_name in self._mc.toc.unknown_schemas
        return unknown_nonexisting or unknown_but_existing

    def _guard_unknown_schema(self, schema_name: str):
        if self._is_unknown_schema(schema_name):
            raise ValueError(f"Unknown or incompatible schema: {schema_name}")

    # public interface:

    # start with methods having non-transitive semantics (considers exact schemas)

    def get_bytes(self, schema_name: str) -> Optional[bytes]:
        """Get bytes for an object belonging (exactly!) to the given schema, if present.

        This also works for unknown schemas (we can retrieve, but cannot parse those).
        """
        if (ret := self._get_raw(schema_name)) is not None:
            return ret[()]

    def find(self, schema_name: str = "") -> Set[str]:
        """List schema names of all attached metadata objects with given schema as a base.

        This will include possibly unknown schema names.

        Only the physically embedded instances are included.
        For transitive closure, add schema names along the parent_path of the schemas.
        """
        basepath = self._meta_schema_dir(schema_name)
        if basepath not in self._mc.__wrapped__:
            # no metadata subdirectory -> no objects (important case!)
            return set()  

        entries: Set[str] = set()

        def collect(path):
            # path is relative to basepath
            segs = path.split("/")
            # marker for object, use to distinguish from group
            # as entry points may not contain = symbols by setuptools spec
            if segs[-1][0] == "=":
                if len(segs) > 1:
                    entries.add(segs[-2])
                elif schema_name:
                    entries.add(schema_name)

        self._node.__wrapped__[basepath].visit(collect)
        return entries

    def __iter__(self) -> Iterator[str]:
        """Iterator listing schema names of all actually attached metadata objects.

        This means, no transitive instances are included."""
        return iter(self.find())

    def __len__(self) -> int:
        """Number of all actually attached metadata objects.

        This means, transitive instances are not counted."""
        return len(self.find())

    def keys(self) -> KeysView[str]:
        return self.find()  # type: ignore

    def values(self) -> ValuesView[MetadataSchema]:
        return set(map(lambda x: x[1], self.items()))  # type: ignore

    def items(self) -> ItemsView[str, MetadataSchema]:
        return {name: self.get[name] for name in self.find()}  # type: ignore

    def __delitem__(self, schema_name: str):
        if self._node._read_only:
            raise ValueError("This entity is marked as read_only!")
        # TODO: remove from TOC
        # remove empty dirs
        # remove package info of that schema, if no instances left
        raise NotImplementedError

    # following only work with known (i.e. registered/correct) schema

    def __setitem__(self, schema_name: str, value: MetadataSchema):
        if self._node._read_only:
            raise ValueError("This entity is marked as read_only!")
        # create required dirs
        # TODO: add to TOC
        # add package info for new schema, if not already included
        raise NotImplementedError

    # following have transitive semantics (i.e. logic also works with parent schemas)

    def get(self, schema_name: str) -> Optional[MetadataSchema]:
        """Get a parsed metadata object (if it exists) matching the given known schema.

        Will also accept a child schema object and parse it as the parent schema.
        If multiple suitable objects are found, will pick any of those to parse it.
        """
        self._guard_unknown_schema(schema_name)
        if not (candidates := self.find(schema_name)):
            return None
        # NOTE: here we might get data from an unknown child schema
        # but as the requested schema is known and compatible, we know it must work!
        dat: Optional[bytes] = self.get_bytes(next(iter(candidates)))
        assert dat is not None  # getting bytes of a found candidate!
        # parse with the correct schema and return the instance object
        return _SCHEMAS[schema_name].parse_raw(dat)  # type: ignore

    def __getitem__(self, schema_name: str) -> MetadataSchema:
        """Like get, but will raise KeyError or failure."""
        ret = self.get(schema_name)
        if ret is None:
            raise KeyError
        return ret

    def __contains__(self, schema_name: str) -> bool:
        """Check whether a suitable metadata object for given known schema exists."""
        self._guard_unknown_schema(schema_name)
        physical = self.find(schema_name)
        transitive = filter(lambda x: schema_name in _SCHEMAS.parent_path(x), physical)
        return any(transitive)


def _split_meta_obj_path(path: str) -> Tuple[str, str, str, str]:
    """Splits full path to a metadata object inside metadata directory of a node.

    Returns tuple with:
        Path to metadata base directory
        schema parents (except for schema itself)
        schema name
        uuid
    
    "/some/path/to/metador_meta_something/schema/subschema/=uuid" results in:
    ('/some/path/to/metador_meta_something', 'schema/subschema', 'subschema', 'uuid')
    """
    segs = path.lstrip("/").split("/")
    uuid = segs.pop()[1:]
    node_meta_base = ""
    schema_path = ""
    schema_name = ""
    found_meta_base = False
    for seg in segs:
        if found_meta_base:
            schema_path += f"/{seg}"
            schema_name = seg
        else:
            node_meta_base += f"/{seg}"
        if seg.startswith(METADOR_META_PREF):
            found_meta_base = True
    return (node_meta_base, schema_path[1:], schema_name, uuid)

class MetadorTOC:
    """Interface to the Metador metadata index of a container."""

    def _find_broken_links(self, repair: bool=False) -> List[str]:
        """Return list of UUIDs in TOC not pointing to an existing metadata object."""
        broken = []
        for uuid in self._toc_path_from_uuid.keys():
            target = self._resolve_link(uuid)
            if target not in self._container.__wrapped__:
                broken.append(uuid)
        if repair:
            for uuid in broken:
                self._unregister_link(uuid)
        return broken

    def _find_missing_links(self, repair: bool = False) -> List[str]:
        """Return list of paths to metadata objects not listed in TOC."""
        missing = []

        def collect_missing(name, node):
            if not _is_metador_path(name, METADOR_META_PREF):
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
                missing.append(node.name)

        self._container.__wrapped__.visititems(collect_missing)

        if not repair:
            return missing

        # Repair missing links (objects get new UUIDs)
        for path in missing:
            meta_dir, schema_path, schema_name, uuid = _split_meta_obj_path(path)
            if self._parent_path(schema_name) is None: # not in schema lookup table
                # add the unknown schema parent path based on inferred info
                self._parent_paths[schema_path] = schema_name
                self._unknown_schemas.add(schema_name)
            # link the object with new UUID
            target_path = self._register_link(meta_dir, schema_name)
            self._container.__wrapped__.move(path, target_path)

        return missing

    # --------

    def _parent_path(self, schema_name: str) -> Optional[str]:
        """Like Schemas.parent_path, but works also with unknown schemas in container."""
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
        toc_path = f"{METADOR_TOC_PATH}/{schema_path}/={link_name}"
        target_path = f"{node_meta_base}/{schema_path}/={link_name}"
        self._toc_path_from_uuid[link_name] = toc_path
        self._container.__wrapped__[toc_path] = target_path # create (broken) link
        return target_path

    def _unregister_link(self, uuid: str):
        """Unregister metadata object in TOC given its UUID.

        Will remove the object and clean up empty directories in the TOC.
        """
        # delete the link itself
        toc_link_path = self._toc_path_from_uuid[uuid]
        link_node = self._container.__wrapped__[toc_link_path]
        parent_node = link_node.parent
        del self._container.__wrapped__[toc_link_path]

        # clean up empty groups
        while parent_node.name != METADOR_TOC_PATH:
            if len(parent_node):
                break  # non-empty
            empty_group = parent_node.name
            parent_node = parent_node.parent
            # NOTE: here we could plug in removing useless dependencies in stored env
            # If the schema is empty, its package is not a dependency anymore
            # if the package does not provide any used schemas
            del self._container.__wrapped__[empty_group]

        # remove the TOC dir itself, if empty
        if parent_node.name == METADOR_TOC_PATH and not len(parent_node):
            del self._container.__wrapped__[METADOR_TOC_PATH]
        # now the uuid counts as "free" again
        del self._toc_path_from_uuid[uuid]

    def _resolve_link(self, uuid: str) -> str:
        """Get the path a uuid in the TOC points to."""
        toc_link_path = self._toc_path_from_uuid[uuid]
        return self._container.__wrapped__[toc_link_path][()].decode("utf-8")

    def _update_link(self, uuid: str, new_target: str):
        """Update target of an existing link to point to a new location."""
        toc_link_path = self._toc_path_from_uuid[uuid]
        del self._container.__wrapped__[toc_link_path]
        self._container.__wrapped__[toc_link_path] = new_target

    def __init__(self, container: MetadorContainer):
        self._container = container

        # 1. compute parent paths based on present TOC structure
        # (we need it to efficiently traverse possibly unknown schemas/objects)
        # 2. collect metadata object uuids
        self._parent_paths: Dict[str, str] = {}
        self._toc_path_from_uuid: Dict[str, str] = {}

        def collect_schemas(path):
            schema_name = path.split("/")[-1]
            if schema_name[0] == "=":  # links/metadata entries start with =
                self._toc_path_from_uuid[schema_name[1:]] = f"{METADOR_TOC_PATH}/{path}"
            else:
                self._parent_paths[schema_name] = path

        if METADOR_TOC_PATH in self._container.__wrapped__:
            self._container.__wrapped__[METADOR_TOC_PATH].visit(collect_schemas)

        # 3. collect the schemas that we don't know already
        self._unknown_schemas = set(self._parent_paths.keys()) - set(_SCHEMAS.keys())

        # TODO: what about name collisions? should check package info and correct deps!
        # for that, the container env entry must be implemented and enforced!
        # also must be careful with parsing - need to mark colliding but wrong as unknown

    @property
    def unknown_schemas(self) -> Set[str]:
        """Return set of schema names that are unknown or incompatible.

        Here, unknown means that a plugin is missing providing that schema name,
        whereas incompatible means that the installed plugin providing that name
        is not suitable for the embedded data using a schema of the same name.
        """
        return set(self._unknown_schemas)


    # TODO: query API like with node meta, but now for whole container
