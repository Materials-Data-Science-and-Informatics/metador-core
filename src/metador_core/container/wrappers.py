from __future__ import annotations

from itertools import takewhile
from typing import Any, Dict, MutableMapping, Optional, Set, Type, Union

import h5py
import wrapt

from ..util.types import H5DatasetLike, H5FileLike, H5GroupLike, H5NodeLike, OpenMode
from . import utils as M
from .drivers import MetadorDriver, to_h5filelike
from .interface import MetadorContainerTOC, MetadorMeta, NodeAcl, NodeAclFlags


class UnsupportedOperationError(AttributeError):
    """Subclass to distinguish between actually missing attribute and unsupported one."""


class WrappedAttributeManager(wrapt.ObjectProxy):
    """Wrapper for AttributeManager-like objects to prevent mutation (read-only) or inspection (skel-only)."""

    __wrapped__: MutableMapping

    _self_acl: NodeAclFlags
    _self_acl_whitelist: Dict[NodeAcl, Set[str]] = {
        NodeAcl.skel_only: {"keys"},
        NodeAcl.read_only: {"keys", "values", "items", "get"},
    }
    _self_allowed: Set[str]

    def __init__(self, obj, acl: NodeAclFlags):
        super().__init__(obj)
        # initialize whitelist based on passed ACL flags
        self._self_acl = acl
        allowed: Optional[Set[str]] = None
        for flag, value in acl.items():
            if not value or flag not in self._self_acl_whitelist:
                continue
            if allowed is None:
                allowed = self._self_acl_whitelist[flag]
            else:
                allowed = allowed.intersection(self._self_acl_whitelist[flag])
        self._self_allowed = allowed or set()

    def _raise_illegal_op(self, flag_info: str):
        raise UnsupportedOperationError(
            f"This attribute set belongs to a node marked as {flag_info}!"
        )

    def __getattr__(self, key: str):
        # NOTE: this will not restrict __contains__ because its a special method
        # (which is desired behavior).
        if (
            hasattr(self.__wrapped__, key)
            and self._self_allowed
            and key not in self._self_allowed
        ):
            self._raise_illegal_op(str(self._self_acl))
        return getattr(self.__wrapped__, key)

    # this is not intercepted by getattr
    def __getitem__(self, key: str):
        if self._self_acl[NodeAcl.skel_only]:
            self._raise_illegal_op(NodeAcl.skel_only.name)
        return self.__wrapped__.__getitem__(key)

    # this is not intercepted by getattr
    def __setitem__(self, key: str, value):
        if self._self_acl[NodeAcl.read_only]:
            self._raise_illegal_op(NodeAcl.read_only.name)
        return self.__wrapped__.__setitem__(key, value)

    # this is not intercepted by getattr
    def __delitem__(self, key: str):
        if self._self_acl[NodeAcl.read_only]:
            self._raise_illegal_op(NodeAcl.read_only.name)
        return self.__wrapped__.__delitem__(key)

    def __repr__(self) -> str:
        return repr(self.__wrapped__)


class WithDefaultQueryStartNode(wrapt.ObjectProxy):
    """Cosmetic wrapper to search metadata below a H5LikeGroup.

    Used to make sure that:
        `group.metador.query(...)`
    is equivalent to:
        `container.metador.query(..., node=group)`

    (without it, the default node will be the root "/" if not specified).
    """

    __wrapped__: MetadorContainerTOC

    def __init__(self, obj: MetadorContainerTOC, def_start_node):
        super().__init__(obj)
        self._self_query_start_node = def_start_node

    def query(self, schema, version=None, *, node=None):
        node = node or self._self_query_start_node
        return self.__wrapped__.query(schema, version, node=node)


class MetadorNode(wrapt.ObjectProxy):
    """Wrapper for h5py and IH5 Groups and Datasets providing Metador-specific features.

    In addition to the Metadata management, also provides helpers to reduce possible
    mistakes in implementing interfaces by allowing to mark nodes as

    * read_only (regardless of the writability of the underlying opened container) and
    * local_only (preventing access to (meta)data above this node)

    Note that these are "soft" restrictions to prevent errors and can be bypassed.
    """

    __wrapped__: H5NodeLike

    @staticmethod
    def _parse_access_flags(kwargs) -> NodeAclFlags:
        # NOTE: mutating kwargs, removes keys that are inspected!
        return {flag: kwargs.pop(flag.name, False) for flag in iter(NodeAcl)}

    def __init__(self, mc: MetadorContainer, node: H5NodeLike, **kwargs):
        flags = self._parse_access_flags(kwargs)
        lp = kwargs.pop("local_parent", None)
        if kwargs:
            raise ValueError(f"Unknown keyword arguments: {kwargs}")

        super().__init__(node)
        self._self_container: MetadorContainer = mc

        self._self_flags: NodeAclFlags = flags
        self._self_local_parent: Optional[MetadorGroup] = lp

    def _child_node_kwargs(self):
        """Return kwargs to be passed to a child node.

        Ensures that {read,skel,local}_only status is passed down correctly.
        """
        return {
            "local_parent": self if self.acl[NodeAcl.local_only] else None,
            **{k.name: v for k, v in self.acl.items() if v},
        }

    def restrict(self, **kwargs) -> MetadorNode:
        """Restrict this object to be local_only or read_only.

        Pass local_only=True and/or read_only=True to enable the restriction.

        local_only means that the node may not access the parent or file objects.
        read_only means that mutable actions cannot be done (even if container is mutable).
        """
        added_flags = self._parse_access_flags(kwargs)
        if added_flags[NodeAcl.local_only]:
            # was set as local explicitly for this node ->
            self._self_local_parent = None  # remove its ability to go up

        # can only set, but not unset!
        self._self_flags.update({k: True for k, v in added_flags.items() if v})
        if kwargs:
            raise ValueError(f"Unknown keyword arguments: {kwargs}")

        return self

    @property
    def acl(self) -> Dict[NodeAcl, bool]:
        """Return ACL flags of current node."""
        return dict(self._self_flags)

    def _guard_path(self, path: str):
        if M.is_internal_path(path):
            msg = f"Trying to use a Metador-internal path: '{path}'"
            raise ValueError(msg)
        if self.acl[NodeAcl.local_only] and path[0] == "/":
            msg = f"Node is marked as local_only, cannot use absolute path '{path}'!"
            raise ValueError(msg)

    def _guard_acl(self, flag: NodeAcl, method: str = "this method"):
        if self.acl[flag]:
            msg = f"Cannot use {method}, the node is marked as {flag.name}!"
            raise UnsupportedOperationError(msg)

    # helpers

    def _wrap_if_node(self, val):
        """Wrap value into a metador node wrapper, if it is a suitable group or dataset."""
        if isinstance(val, H5GroupLike):
            return MetadorGroup(self._self_container, val, **self._child_node_kwargs())
        elif isinstance(val, H5DatasetLike):
            return MetadorDataset(
                self._self_container, val, **self._child_node_kwargs()
            )
        else:
            return val

    def _destroy_meta(self, _unlink: bool = True):
        """Destroy all attached metadata at and below this node."""
        self.meta._destroy(_unlink=_unlink)

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
        return repr(self.__wrapped__)

    # added features

    @property
    def meta(self) -> MetadorMeta:
        """Access the interface to metadata attached to this node."""
        return MetadorMeta(self)

    @property
    def metador(self) -> MetadorContainerTOC:
        """Access the info about the container this node belongs to."""
        return WithDefaultQueryStartNode(self._self_container.metador, self)

    # wrap existing methods as needed

    @property
    def name(self) -> str:
        return self.__wrapped__.name  # just for type checker not to complain

    @property
    def attrs(self):
        if self.acl[NodeAcl.read_only] or self.acl[NodeAcl.skel_only]:
            return WrappedAttributeManager(self.__wrapped__.attrs, self.acl)
        return self.__wrapped__.attrs

    @property
    def parent(self) -> MetadorGroup:
        if self.acl[NodeAcl.local_only]:
            # allow child nodes of local-only nodes to go up to the marked parent
            # (or it is None, if this is the local root)
            if lp := self._self_local_parent:
                return lp
            else:
                # raise exception (illegal non-local access)
                self._guard_acl(NodeAcl.local_only, "parent")

        return MetadorGroup(
            self._self_container,
            self.__wrapped__.parent,
            **self._child_node_kwargs(),
        )

    @property
    def file(self) -> MetadorContainer:
        if self.acl[NodeAcl.local_only]:
            # raise exception (illegal non-local access)
            self._guard_acl(NodeAcl.local_only, "parent")
        return self._self_container


class MetadorDataset(MetadorNode):
    """Metador wrapper for a HDF5 Dataset."""

    __wrapped__: H5DatasetLike

    # manually assembled from public methods which h5py.Dataset provides
    _self_RO_FORBIDDEN = {"resize", "make_scale", "write_direct", "flush"}

    def __getattr__(self, key):
        if self.acl[NodeAcl.read_only] and key in self._self_RO_FORBIDDEN:
            self._guard_acl(NodeAcl.read_only, key)
        if self.acl[NodeAcl.skel_only] and key == "get":
            self._guard_acl(NodeAcl.skel_only, key)

        return getattr(self.__wrapped__, key)

    # prevent getter of node if marked as skel_only
    def __getitem__(self, *args, **kwargs):
        self._guard_acl(NodeAcl.skel_only, "__getitem__")
        return self.__wrapped__.__getitem__(*args, **kwargs)

    # prevent mutating method calls of node is marked as read_only

    def __setitem__(self, *args, **kwargs):
        self._guard_acl(NodeAcl.read_only, "__setitem__")
        return self.__wrapped__.__setitem__(*args, **kwargs)


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
            obj._guard_acl(NodeAcl.read_only, method)
        ret = getattr(obj.__wrapped__, method)(name, *args, **kwargs)  # RAW
        return obj._wrap_if_node(ret)

    return wrapped_method


# classes of h5py reference-like types (we don't support that)
_H5_REF_TYPES = [h5py.HardLink, h5py.SoftLink, h5py.ExternalLink, h5py.h5r.Reference]


class MetadorGroup(MetadorNode):
    """Wrapper for a HDF5 Group."""

    __wrapped__: H5GroupLike

    def _destroy_meta(self, _unlink: bool = True):
        """Destroy all attached metadata at and below this node (recursively)."""
        super()._destroy_meta(_unlink=_unlink)  # this node
        for child in self.values():  # recurse
            child._destroy_meta(_unlink=_unlink)

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
        for k, v in self.__wrapped__.items():
            if v is None:
                # NOTE: e.g. when nodes are deleted/moved during iteration,
                # v can suddenly be None -> we need to catch this case!
                continue
            if not M.is_internal_path(v.name):
                yield (k, self._wrap_if_node(v))

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
            if nxt := self.get(segs[0]):
                return "/".join(segs[1:]) in nxt
            return False

    # these we can take care of but are a bit more tricky to think through

    def __delitem__(self, name: str):
        self._guard_acl(NodeAcl.read_only, "__delitem__")
        self._guard_path(name)

        node = self[name]
        # clean up metadata (recursively, if a group)
        node._destroy_meta()
        # kill the actual data
        return _wrap_method("__delitem__")(self, name)

    def move(self, source: str, dest: str):
        self._guard_acl(NodeAcl.read_only, "move")
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
        if meta_base_node := self.__wrapped__.get(meta_base):
            assert isinstance(meta_base_node, H5GroupLike)
            missing = self._self_container.metador._links.find_missing(meta_base_node)
            self._self_container.metador._links.repair_missing(missing, update=True)

    def copy(
        self,
        source: Union[str, MetadorGroup, MetadorDataset],
        dest: Union[str, MetadorGroup],
        **kwargs,
    ):
        self._guard_acl(NodeAcl.read_only, "copy")

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
            # because metadata lives in parallel group, need to copy separately:
            src_meta: str = src_node.meta._base_dir
            dst_meta: str = dst_node.meta._base_dir  # node will not exist yet
            self.__wrapped__.copy(src_meta, dst_meta, **copy_kwargs)  # RAW

            # register in TOC:
            dst_meta_node = self.__wrapped__[dst_meta]
            assert isinstance(dst_meta_node, H5GroupLike)
            missing = self._self_container.metador._links.find_missing(dst_meta_node)
            self._self_container.metador._links.repair_missing(missing)

        if not src_is_dataset:
            if without_meta:
                # need to destroy copied metadata copied with the source group
                # but keep TOC links (they point to original copy!)
                dst_node._destroy_meta(_unlink=False)
            else:
                # register copied metadata objects under new uuids
                missing = self._self_container.metador._links.find_missing(dst_node)
                self._self_container.metador._links.repair_missing(missing)

    def __getattr__(self, key):
        if hasattr(self.__wrapped__, key):
            raise UnsupportedOperationError(key)  # deliberately unsupported
        else:
            msg = f"'{type(self).__name__}' object has no attribute '{key}'"
            raise AttributeError(msg)


# ----


class MetadorContainer(MetadorGroup):
    """Wrapper class adding Metador container interface to h5py.File-like objects.

    The wrapper ensures that any actions done to IH5Records through this interface
    also work with plain h5py.Files.

    There are no guarantees about behaviour with h5py methods not supported by IH5Records.

    Given `old: MetadorContainer`, `MetadorContainer(old.data_source, driver=old.data_driver)`
    should be able to construct another object to access the same data (assuming it is not locked).
    """

    __wrapped__: H5FileLike

    _self_SUPPORTED: Set[str] = {"mode", "flush", "close"}

    # ---- new container-level interface ----

    _self_toc: MetadorContainerTOC

    @property
    def metador(self) -> MetadorContainerTOC:
        """Access interface to Metador metadata object index."""
        return self._self_toc

    def __init__(
        self,
        name_or_obj: Union[MetadorDriver, Any],
        mode: Optional[OpenMode] = "r",
        *,
        # NOTE: driver takes class instead of enum to also allow subclasses
        driver: Optional[Type[MetadorDriver]] = None,
    ):
        """Initialize a MetadorContainer instance from file(s) or a supported object.

        The `mode` argument is ignored when simply wrapping an object.

        If a data source such as a path is passed, will instantiate the object first,
        using the default H5File driver or the passed `driver` keyword argument.
        """
        # wrap the h5file-like object (will set self.__wrapped__)
        super().__init__(self, to_h5filelike(name_or_obj, mode, driver=driver))
        # initialize metador-specific stuff
        self._self_toc = MetadorContainerTOC(self)

    # not clear if we want these in the public interface. keep this private for now:

    # def _find_orphan_meta(self) -> List[str]:
    #     """Return list of paths to metadata that has no corresponding user node anymore."""
    #     ret: List[str] = []

    #     def collect_orphans(name: str):
    #         if M.is_meta_base_path(name):
    #             if M.to_data_node_path(name) not in self:
    #                 ret.append(name)

    #     self.__wrapped__.visit(collect_orphans)
    #     return ret

    # def _repair(self, remove_orphans: bool = False):
    #     """Repair container structure on best-effort basis.

    #     This will ensure that the TOC points to existing metadata objects
    #     and that all metadata objects are listed in the TOC.

    #     If remove_orphans is set, will erase metadata not belonging to an existing node.

    #     Notice that missing schema plugin dependency metadata cannot be restored.
    #     """
    #     if remove_orphans:
    #         for path in self._find_orphan_meta():
    #             del self.__wrapped__[path]
    #     self.toc._links.find_broken(repair=True)
    #     missing = self.toc._links._find_missing("/")
    #     self.toc._links.repair_missing(missing)

    # ---- pass through HDF5 group methods to a wrapped root group instance ----

    def __getattr__(self, key: str):
        if key in self._self_SUPPORTED:
            return getattr(self.__wrapped__, key)
        return super().__getattr__(key)  # ask group for method

    # context manager: return the wrapper back, not the raw thing:

    def __enter__(self):
        self.__wrapped__.__enter__()
        return self

    def __exit__(self, *args):
        return self.__wrapped__.__exit__(*args)

    # we want these also to be forwarded to the wrapped group, not the raw object:

    def __dir__(self):
        return list(set(super().__dir__()).union(type(self).__dict__.keys()))

    # make wrapper transparent:

    def __repr__(self) -> str:
        return repr(self.__wrapped__)  # shows that its a File, not just a Group
