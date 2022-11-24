from __future__ import annotations

import json
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    ItemsView,
    Iterator,
    KeysView,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
    ValuesView,
    cast,
    overload,
)
from uuid import UUID, uuid1

from typing_extensions import TypeAlias

from ..plugin.types import EPName, from_ep_name, plugin_args, to_ep_name
from ..plugins import schemas
from ..schema import MetadataSchema
from ..schema.plugins import PluginPkgMeta, PluginRef
from ..schema.types import SemVerTuple
from . import utils as M
from .drivers import (
    METADOR_DRIVERS,
    MetadorDriver,
    MetadorDriverEnum,
    get_driver_type,
    get_source,
)
from .types import H5DatasetLike, H5FileLike, H5GroupLike

if TYPE_CHECKING:
    from .wrappers import MetadorContainer, MetadorNode

S = TypeVar("S", bound=MetadataSchema)


@dataclass
class StoredMetadata:
    """Information about a metadata schema instance stored at a node."""

    uuid: UUID
    """UUID identifying the metadata object in the container.

    Used for bookkeeping, i.e. keeping the container TOC in sync.
    """

    schema: PluginRef
    """Schema the object is an instance of."""

    node: H5DatasetLike
    """Node with serialized metadata object."""

    def to_path(self):
        prefix = self.node.parent.name
        ep_name = to_ep_name(self.schema.name, self.schema.version)
        return f"{prefix}/{ep_name}={self.uuid}"

    @staticmethod
    def from_node(obj: H5DatasetLike) -> StoredMetadata:
        path = obj.name
        segs = path.lstrip("/").split("/")
        ep_name, uuid_str = segs.pop().split("=")
        s_name, s_vers = from_ep_name(EPName(ep_name))
        uuid = UUID(uuid_str)
        s_ref = schemas.PluginRef(name=s_name, version=s_vers)
        return StoredMetadata(uuid=uuid, schema=s_ref, node=obj)


def _schema_ref_for(ep_name: str) -> PluginRef:
    s_name, s_ver = from_ep_name(EPName(ep_name))
    return schemas.PluginRef(name=s_name, version=s_ver)


def _ep_name_for(s_ref: PluginRef) -> str:
    return to_ep_name(s_ref.name, s_ref.version)


class MetadorMeta:
    """Interface to Metador metadata objects stored at a single HDF5 node."""

    # helpers for __getitem__ and __setitem__

    @staticmethod
    def _require_schema(
        schema_name: str, schema_ver: Optional[SemVerTuple]
    ) -> Type[MetadataSchema]:
        """Return compatible installed schema class, if possible.

        Raises KeyError if no suitable schema was found.

        Raises TypeError if an auxiliary schema is requested.
        """
        schema_class = schemas._get_unsafe(
            schema_name, schema_ver
        )  # can raise KeyError
        if schema_class.Plugin.auxiliary:  # reject auxiliary schemas in container
            msg = f"Cannot attach instances of auxiliary schema '{schema_name}' to a node!"
            raise TypeError(msg)
        return schema_class

    @staticmethod
    def _parse_obj(
        schema: Type[S], obj: Union[str, bytes, Dict[str, Any], MetadataSchema]
    ) -> S:
        """Return original object if it is an instance of passed schema, or else parse it.

        Raises ValidationError if parsing fails.
        """
        if isinstance(obj, schema):
            return obj  # skip validation, already correct model!
        # try to convert/parse it:
        if isinstance(obj, (str, bytes)):
            return schema.parse_raw(obj)
        if isinstance(obj, MetadataSchema):
            return schema.parse_obj(obj.dict())
        else:  # dict
            return schema.parse_obj(obj)

    # raw getters and setters don't care about the environment,
    # they work only based on what objects are available and compatible
    # and do not perform validation etc.

    def _get_raw(
        self, schema_name: str, version: Optional[SemVerTuple] = None
    ) -> Optional[StoredMetadata]:
        """Return stored metadata for given schema at this node (or None).

        If a version is passed, the stored version must also be compatible.
        """
        # retrieve stored instance (if suitable)
        ret: Optional[StoredMetadata] = self._objs.get(schema_name)
        if not version:
            return ret  # no specified version -> anything goes
        # otherwise: only return if it is compatible
        req_ref: Optional[PluginRef] = None
        req_ref = schemas.PluginRef(name=schema_name, version=version)
        return ret if ret and req_ref.supports(ret.schema) else None

    def _set_raw(self, schema_ref: PluginRef, obj: MetadataSchema) -> None:
        """Store metadata object as instance of passed schema at this node."""
        # reserve UUID, construct dataset path and store metadata object
        obj_uuid = self._mc.metador._links.fresh_uuid()
        obj_path = f"{self._base_dir}/{_ep_name_for(schema_ref)}={str(obj_uuid)}"
        # store object
        self._mc.__wrapped__[obj_path] = bytes(obj)
        obj_node = self._mc.__wrapped__[obj_path]
        assert isinstance(obj_node, H5DatasetLike)
        stored_obj = StoredMetadata(uuid=obj_uuid, schema=schema_ref, node=obj_node)
        self._objs[schema_ref] = stored_obj
        # update TOC
        self._mc.metador._links.register(stored_obj)
        return

    def _del_raw(self, schema_name: str, *, _unlink: bool = True) -> None:
        """Delete stored metadata for given schema at this node."""
        # NOTE: _unlink is only for the destroy method
        stored_obj = self._objs[schema_name]
        # unregister in TOC (will also trigger clean up there)
        if _unlink:
            self._mc.metador._links.unregister(stored_obj.uuid)
        # remove metadata object
        del self._objs[stored_obj.schema.name]
        del self._mc.__wrapped__[stored_obj.node.name]
        # no metadata objects left -> remove metadata dir
        if not self._objs:
            del self._mc.__wrapped__[self._base_dir]
        return

    # helpers for container-level opertions (move, copy, delete etc)

    def _destroy(self, *, _unlink: bool = True):
        """Unregister and delete all metadata objects attached to this node."""
        # NOTE: _unlink is only set to false for node copy without metadata
        for schema_name in list(self.keys()):
            self._del_raw(schema_name, _unlink=_unlink)

    # ----

    def __init__(self, node: MetadorNode):
        self._mc: MetadorContainer = node._self_container
        """Underlying container (for convenience)."""

        self._node: MetadorNode = node
        """Underlying actual user node."""

        is_dataset = isinstance(node, H5DatasetLike)
        self._base_dir: str = M.to_meta_base_path(node.name, is_dataset)
        """Path of this metador metadata group node.

        Actual node exists iff any metadata is stored for the node.
        """

        self._objs: Dict[str, StoredMetadata] = {}
        """Information about available metadata objects."""

        # load available object metadata encoded in the node names
        meta_grp = cast(H5GroupLike, self._mc.__wrapped__.get(self._base_dir, {}))
        for obj_node in meta_grp.values():
            assert isinstance(obj_node, H5DatasetLike)
            obj = StoredMetadata.from_node(obj_node)
            self._objs[obj.schema.name] = obj

    # ----

    def keys(self) -> KeysView[str]:
        """Return names of explicitly attached metadata objects.

        Transitive parent schemas are not included.
        """
        return self._objs.keys()

    def values(self) -> ValuesView[StoredMetadata]:
        self._node._guard_skel_only()
        return self._objs.values()

    def items(self) -> ItemsView[str, StoredMetadata]:
        self._node._guard_skel_only()
        return self._objs.items()

    # ----

    def __len__(self) -> int:
        """Return number of explicitly attached metadata objects.

        Transitive parent schemas are not counted.
        """
        return len(self.keys())

    def __iter__(self) -> Iterator[str]:
        """Iterate listing schema names of all actually attached metadata objects.

        Transitive parent schemas are not included.
        """
        return iter(self.keys())

    # ----

    def query(
        self,
        schema: Union[str, Type[MetadataSchema]] = "",
        version: Optional[SemVerTuple] = None,
    ) -> Iterator[PluginRef]:
        """Return schema names for which objects at this node are compatible with passed schema.

        Will also consider compatible child schema instances.

        Returned iterator will yield passed schema first, if an object is available.
        Otherwise, the order is not specified.
        """
        schema_name, schema_ver = plugin_args(schema, version)
        # no schema selected -> anything goes
        if not schema_name:
            for obj in self.values():
                yield obj.schema
            return

        # try exact schema (in any compatible version, if version specified)
        if obj := self._get_raw(schema_name, schema_ver):
            yield obj.schema

        # next, try schemas compatible with any child schemas
        compat = self._mc.metador.schemas.children(schema_name, schema_ver)
        avail = {self._get_raw(s).schema for s in self.keys()}
        for s_ref in avail.intersection(compat):
            yield s_ref

    def __contains__(self, schema: Union[str, MetadataSchema]) -> bool:
        """Check whether a compatible metadata object for given schema exists.

        Will also consider compatible child schema instances.
        """
        if schema == "":
            return False
        return next(self.query(schema), None) is not None

    @overload
    def __getitem__(self, schema: str) -> MetadataSchema:
        ...

    @overload
    def __getitem__(self, schema: Type[S]) -> S:
        ...

    def __getitem__(self, schema: Union[str, Type[S]]) -> Union[S, MetadataSchema]:
        """Like get, but will raise KeyError on failure."""
        if ret := self.get(schema):
            return ret
        raise KeyError(schema)

    @overload
    def get(
        self, schema: str, version: Optional[SemVerTuple] = None
    ) -> Optional[MetadataSchema]:
        ...

    @overload
    def get(
        self, schema: Type[S], version: Optional[SemVerTuple] = None
    ) -> Optional[S]:
        ...

    def get(
        self, schema: Union[str, Type[S]], version: Optional[SemVerTuple] = None
    ) -> Optional[Union[MetadataSchema, S]]:
        """Get a parsed metadata object matching the given schema (if it exists).

        Will also consider compatible child schema instances.
        """
        self._node._guard_skel_only()
        schema_name, schema_ver = plugin_args(schema, version)

        compat_schema = next(self.query(schema_name, schema_ver), None)
        if not compat_schema:
            return None

        schema_class = self._require_schema(schema_name, schema_ver)
        if obj := self._get_raw(compat_schema.name, compat_schema.version):
            return cast(S, self._parse_obj(schema_class, obj.node[()]))
        return None

    def __setitem__(
        self, schema: Union[str, Type[S]], value: Union[Dict[str, Any], MetadataSchema]
    ) -> None:
        """Store metadata object as instance of given schema.

        Raises KeyError if passed schema is not installed in environment.

        Raises TypeError if passed schema is marked auxiliary.

        Raises ValueError if an object for the schema already exists.

        Raises ValidationError if passed object is not valid for the schema.
        """
        self._node._guard_read_only()
        schema_name, schema_ver = plugin_args(schema)

        # if self.get(schema_name, schema_ver):  # <- also subclass schemas
        # NOTE: for practical reasons let's be more lenient here and allow redundancy
        # hence only check if exact schema (modulo version) is already there
        if self._get_raw(schema_name):  # <- only same schema
            msg = f"Metadata object for schema {schema_name} already exists!"
            raise ValueError(msg)

        schema_class = self._require_schema(schema_name, schema_ver)
        checked_obj = self._parse_obj(schema_class, value)
        self._set_raw(schema_class.Plugin.ref(), checked_obj)

    def __delitem__(self, schema: Union[str, Type[MetadataSchema]]) -> None:
        """Delete metadata object explicitly stored for the passed schema.

        If a schema class is passed, its version is ignored,
        as each node may contain at most one explicit instance per schema.

        Raises KeyError if no metadata object for that schema exists.
        """
        self._node._guard_read_only()
        schema_name, _ = plugin_args(schema)

        if self._get_raw(schema_name) is None:
            raise KeyError(schema_name)  # no (explicit) metadata object

        self._del_raw(schema_name)


# ----


class TOCLinks:
    """Link management for synchronizing metadata objects and container TOC."""

    # NOTE: This is not exposed to the end-user

    @staticmethod
    def _link_path_for(schema_ref: PluginRef) -> str:
        return f"{M.METADOR_LINKS_PATH}/{_ep_name_for(schema_ref)}"

    def __init__(self, raw_cont: H5FileLike, toc_schemas: TOCSchemas):
        self._raw: H5FileLike = raw_cont
        """Raw underlying container (for quick access)."""

        self._toc_schemas = toc_schemas
        """Schemas used in container (to (un)register)."""

        self._toc_path: Dict[UUID, str] = {}
        """Maps metadata object UUIDs to paths of respective pseudo-symlink in TOC."""

        # load links into memory
        if M.METADOR_LINKS_PATH in self._raw:
            link_grp = self._raw.require_group(M.METADOR_LINKS_PATH)
            assert isinstance(link_grp, H5GroupLike)
            for schema_link_grp in link_grp.values():
                assert isinstance(schema_link_grp, H5GroupLike)
                for uuid, link_node in schema_link_grp.items():
                    assert isinstance(link_node, H5DatasetLike)
                    self._toc_path[UUID(uuid)] = link_node.name

    def fresh_uuid(self) -> UUID:
        """Return a UUID string not used for a metadata object in the container yet."""
        fresh = False
        ret: UUID
        # NOTE: here a very unlikely race condition is present if parallelized
        while not fresh:
            ret = uuid1()
            fresh = ret not in self._toc_path
        self._toc_path[ret] = None  # not assigned yet, but "reserved"
        # ----
        return ret

    def resolve(self, uuid: UUID) -> str:
        """Get the path a UUID in the TOC points to."""
        link_path = self._toc_path[uuid]
        link_node = cast(H5DatasetLike, self._raw[link_path])
        return link_node[()].decode("utf-8")

    def update(self, uuid: UUID, new_target: str):
        """Update target of an existing link to point to a new location."""
        link_path = self._toc_path[uuid]
        del self._raw[link_path]
        self._raw[link_path] = new_target

    def register(self, obj: StoredMetadata) -> None:
        """Create a link for a metadata object in container TOC.

        The link points to the metadata object.
        """
        self._toc_schemas._register(obj.schema)

        toc_path = f"{self._link_path_for(obj.schema)}/{obj.uuid}"
        self._toc_path[obj.uuid] = toc_path
        self._raw[toc_path] = str(obj.node.name)

    def unregister(self, uuid: UUID) -> None:
        """Unregister metadata object in TOC given its UUID.

        Will remove the object and clean up empty directories in the TOC.
        """
        # delete the link itself and free the UUID
        toc_path = self._toc_path[uuid]

        schema_group = self._raw[toc_path].parent
        assert isinstance(schema_group, H5GroupLike)
        link_group = schema_group.parent
        assert link_group.name == M.METADOR_LINKS_PATH

        del self._raw[toc_path]
        del self._toc_path[uuid]
        if len(schema_group):
            return  # schema still has instances

        s_name_vers: str = schema_group.name.split("/")[-1]
        # delete empty group for schema
        del self._raw[schema_group.name]
        # notify schema manager (cleans up schema + package info)
        self._toc_schemas._unregister(_schema_ref_for(s_name_vers))

        if len(link_group.keys()):
            return  # container still has metadata
        else:
            # remove the link dir itself (no known metadata in container left)
            del self._raw[link_group.name]

    # ----

    def find_broken(self, repair: bool = False) -> List[UUID]:
        """Return list of UUIDs in TOC not pointing to an existing metadata object.

        Will use loaded cache of UUIDs and check them, without scanning the container.

        If repair is set, will remove those broken links.
        """
        broken = []
        for uuid in self._toc_path.keys():
            target = self.resolve(uuid)
            if target not in self._raw:
                broken.append(uuid)
        if repair:
            for uuid in broken:
                self.unregister(uuid)
        return broken

    def find_missing(self, path: H5GroupLike) -> List[H5DatasetLike]:
        """Return list of metadata objects not listed in TOC."""
        missing = []

        def collect_missing(_, node):
            if not M.is_internal_path(node.name, M.METADOR_META_PREF):
                return  # not a metador metadata path

            obj = StoredMetadata.from_node(node)
            known = obj.uuid in self._toc_path
            # check UUID collision: i.e., used in TOC, but points elsewhere
            # (requires fixing up the name of this object / new UUID)
            # implies that THIS object IS missing in the TOC
            collision = known and self.resolve(obj.uuid) != node.name
            if not known or collision:
                missing.append(node)

        # ensure its a group and collect
        self._raw.require_group(path.name).visititems(collect_missing)
        return missing

    def repair_missing(
        self, missing: List[H5DatasetLike], update: bool = False
    ) -> None:
        """Repair links (objects get new UUIDs, unless update is true)."""
        # NOTE: needed for correct copy and move of nodes with their metadata
        for node in missing:
            obj = StoredMetadata.from_node(node)
            if update and obj.uuid in self._toc_path:
                # update target of existing link (e.g. for move)
                self.update(obj.uuid, node.name)
            else:
                # assign new UUID (e.g. for copy)
                obj.uuid = self.fresh_uuid()
                new_path = obj.to_path()
                self._raw.move(node.name, new_path)
                self.register(obj)


class TOCSchemas:
    """Schema management for schemas used in the container.

    Interface is made to mimic PGSchema wherever it makes sense.
    """

    @classmethod
    def _schema_path_for(cls, s_ref: PluginRef) -> str:
        return f"{M.METADOR_SCHEMAS_PATH}/{to_ep_name(s_ref.name, s_ref.version)}"

    @classmethod
    def _jsonschema_path_for(cls, s_ref: PluginRef) -> str:
        return f"{cls._schema_path_for(s_ref)}/jsonschema.json"

    @staticmethod
    def _load_json(node: H5DatasetLike):
        return json.loads(node[()].decode("utf-8"))

    def _update_parents_children(
        self, schema_ref: PluginRef, parents: Optional[List[PluginRef]]
    ):
        if parents is None:  # remove schema
            for parent in self._parents[schema_ref]:
                if parent in self._schemas:
                    self._children[parent].remove(schema_ref)
                elif all(
                    (child not in self._schemas for child in self._children[parent])
                ):
                    del self._parents[parent]
                    del self._children[parent]
        else:  # add schema
            for i, parent in enumerate(parents):
                if parent not in self._parents:
                    self._parents[parent] = parents[: i + 1]
                if parent not in self._children:
                    self._children[parent] = set()
                if parent != schema_ref:
                    self._children[parent].add(schema_ref)

    def _register(self, schema_ref: PluginRef):
        """Notify that a schema is used in the container (metadata object is created/updated).

        If the schema has not been used before in the container, will store metadata about it.
        """
        if schema_ref in self._schemas:
            return  # nothing to do

        # store json schema
        schema_cls = schemas.get(schema_ref.name, schema_ref.version)
        jsonschema_dat = schema_cls.schema_json().encode("utf-8")
        jsonschema_path = self._jsonschema_path_for(schema_ref)
        self._raw[jsonschema_path] = jsonschema_dat

        # store parent schema refs
        compat_path = f"{self._schema_path_for(schema_ref)}/compat"
        parents = schemas.parent_path(schema_ref.name, schema_ref.version)
        parents_dat: bytes = json.dumps(list(map(lambda x: x.dict(), parents))).encode(
            "utf-8"
        )

        self._raw[compat_path] = parents_dat
        self._schemas.add(schema_ref)
        self._update_parents_children(schema_ref, parents)

        # add providing package (if no stored package provides it)
        if not self._pkgs._providers.get(schema_ref, []):
            env_pkg_info: PluginPkgMeta = schemas.provider(schema_cls.Plugin.ref())
            pkg_name_ver = (str(env_pkg_info.name), env_pkg_info.version)
            self._pkgs._register(pkg_name_ver, env_pkg_info)
            self._used[pkg_name_ver] = set()

        # update used schemas tracker for all packages providing this schema
        for pkg in self._pkgs._providers[schema_ref]:
            self._used[pkg].add(schema_ref)

    def _unregister(self, schema_ref: PluginRef):
        """Notify that a schema is not used at any container node anymore.

        If after that no schema of a listed dep package is used,
        this dependency will be removed from the container.
        """
        del self._raw[self._schema_path_for(schema_ref)]
        self._schemas.remove(schema_ref)
        self._update_parents_children(schema_ref, None)

        providers = set(self._pkgs._providers[schema_ref])
        for pkg in providers:
            pkg_used = self._used[pkg]
            if schema_ref in pkg_used:
                # remove schema from list of used schemas of pkg
                pkg_used.remove(schema_ref)
            if not len(pkg_used):
                # package not used anymore in container -> clean up
                self._pkgs._unregister(pkg)

        # remove schemas group if it is empty (no schemas used in container)
        if not self._raw.require_group(M.METADOR_SCHEMAS_PATH).keys():
            del self._raw[M.METADOR_SCHEMAS_PATH]

    def __init__(self, raw_cont: H5FileLike, toc_packages: TOCPackages):
        self._raw: H5FileLike = raw_cont
        """Raw underlying container (for quick access)."""

        self._pkgs = toc_packages
        """TOC package metadata manager object."""

        self._schemas: Set[PluginRef] = set()
        """Stored JSON Schemas of used schemas."""

        self._parents: Dict[PluginRef, List[PluginRef]] = {}
        """Parents of a used json schema (i.e. other partially compatible schemas)."""

        self._children: Dict[PluginRef, Set[PluginRef]] = {}
        """Children of a used json schema (i.e. other fully compatible schemas)."""

        self._used: Dict[PythonDep, Set[PluginRef]] = {}
        """package name + version -> name of schemas used in container"""

        for pkg in self._pkgs.keys():
            self._used[pkg] = set()

        if M.METADOR_SCHEMAS_PATH in self._raw:
            schema_grp = self._raw.require_group(M.METADOR_SCHEMAS_PATH)
            for name, node in schema_grp.items():
                s_ref: PluginRef = _schema_ref_for(name)
                assert isinstance(node, H5GroupLike)
                compat = node["compat"]
                assert isinstance(compat, H5DatasetLike)

                reflist = json.loads(compat[()].decode("utf-8"))
                parents = list(map(PluginRef.parse_obj, reflist))

                self._schemas.add(s_ref)
                self._update_parents_children(s_ref, parents)
                for pkg in self._pkgs._providers[s_ref]:
                    self._used[pkg].add(s_ref)

    @property
    def packages(self) -> TOCPackages:
        """Like PluginGroup.packages, but with respect to schemas used in container."""
        return self._pkgs

    def provider(self, schema_ref: PluginRef) -> PluginPkgMeta:
        """Like PluginGroup.provider, but with respect to container deps."""
        pkg_name_ver = next(iter(self._pkgs._providers.get(schema_ref, [])), None)
        if pkg_name_ver is None:
            msg = f"Did not find metadata of a package providing schema: '{schema_ref}'"
            raise KeyError(msg)
        return self._pkgs[pkg_name_ver]

    def parent_path(
        self, schema, version: Optional[SemVerTuple] = None
    ) -> List[PluginRef]:
        """Like PGSchema.parent_path, but with respect to container deps."""
        name, vers = plugin_args(schema, version, require_version=True)
        s_ref = schemas.PluginRef(name=name, version=vers)
        return self._parents[s_ref]

    def children(self, schema, version: Optional[SemVerTuple] = None) -> Set[PluginRef]:
        """Like PGSchema.children, but with respect to container deps."""
        name, vers = plugin_args(schema, version)
        if vers is not None:
            s_refs = [schemas.PluginRef(name=name, version=vers)]
        else:
            # if no version is given, collect all possibilities
            s_refs = [ref for ref in self._children.keys() if ref.name == name]
        return set.union(set(), *map(self._children.__getitem__, s_refs))

    # ----

    def __len__(self):
        return len(self._schemas)

    def __iter__(self):
        return iter(self.keys())

    def __contains__(self, schema_ref: PluginRef):
        return schema_ref in self._schemas

    def __getitem__(self, schema_ref: PluginRef):
        node_path = self._jsonschema_path_for(schema_ref)
        assert node_path in self._raw
        return self._load_json(self._raw.require_dataset(node_path))

    def get(self, schema_ref: PluginRef):
        try:
            self[schema_ref]
        except KeyError:
            return None

    def keys(self):
        return set(self._schemas)

    def values(self):
        return [self[k] for k in self.keys()]

    def items(self):
        return [(k, self[k]) for k in self.keys()]


PythonDep: TypeAlias = Tuple[str, SemVerTuple]


class TOCPackages:
    """Package metadata management for schemas used in the container.

    The container will always store for each schema used in the
    information about one package providing that schema.

    If there are multiple providers of the same schema,
    the first/existing one is preferred.
    """

    @staticmethod
    def _pkginfo_path_for(pkg_name: str, pkg_version: SemVerTuple) -> str:
        return f"{M.METADOR_PACKAGES_PATH}/{to_ep_name(pkg_name, pkg_version)}"

    def _add_providers(self, pkg: PythonDep, pkginfo: PluginPkgMeta):
        # fill schema -> package lookup table for provided package
        for schema_ref in pkginfo.plugins[schemas.name]:
            if schema_ref not in self._providers:
                self._providers[schema_ref] = set()
            self._providers[schema_ref].add(pkg)

    def _register(self, pkg: PythonDep, info: PluginPkgMeta):
        pkg_path = self._pkginfo_path_for(*pkg)
        self._raw[pkg_path] = bytes(info)
        self._pkginfos[pkg] = info
        self._add_providers(pkg, info)

    def _unregister(self, pkg: PythonDep):
        pkg_path = self._pkginfo_path_for(*pkg)
        del self._raw[pkg_path]
        info = self._pkginfos.pop(pkg)
        # unregister providers
        for schema_ref in info.plugins[schemas.name]:
            providers = self._providers[schema_ref]
            providers.remove(pkg)
            if not providers:  # schema not provided by any package
                del self._providers[schema_ref]

        # remove schemas group if it is empty (no schemas used in container)
        if not self._raw.require_group(M.METADOR_PACKAGES_PATH).keys():
            del self._raw[M.METADOR_PACKAGES_PATH]

    def __init__(self, raw_container: H5FileLike):
        self._raw: H5FileLike = raw_container
        """Raw underlying container (for quick access)."""

        self._pkginfos: Dict[PythonDep, PluginPkgMeta] = {}
        """Package name + version -> package info"""

        self._providers: Dict[PluginRef, Set[PythonDep]] = {}
        """schema reference -> package name + version"""

        # parse package infos if they exist
        if M.METADOR_PACKAGES_PATH in self._raw:
            deps_grp = self._raw.require_group(M.METADOR_PACKAGES_PATH)
            for name, node in deps_grp.items():
                pkg: PythonDep = from_ep_name(EPName(name))
                info = PluginPkgMeta.parse_raw(cast(H5DatasetLike, node)[()])
                self._pkginfos[pkg] = info
                self._add_providers(pkg, info)

    # ----

    def __len__(self):
        return len(self._pkginfos)

    def __iter__(self):
        return iter(self._pkginfos)

    def __contains__(self, pkg: PythonDep):
        return pkg in self._pkginfos

    def __getitem__(self, pkg: PythonDep):
        return self._pkginfos[pkg]

    def keys(self):
        return self._pkginfos.keys()

    def values(self):
        return self._pkginfos.values()

    def items(self):
        return self._pkginfos.items()


class MetadorContainerTOC:
    """Interface to the Metador metadata index (table of contents) of a container."""

    def __init__(self, container: MetadorContainer):
        self._container = container
        self._raw = self._container.__wrapped__

        ver = self.spec_version if M.METADOR_VERSION_PATH in self._raw else None
        if ver is None and self._container.mode == "r":
            msg = "Container is read-only and does not look like a Metador container! "
            msg += "Please open in writable mode to initialize Metador structures!"
            raise ValueError(msg)
        if ver is not None and ver >= [2]:
            msg = f"Unsupported Metador container version: {ver}"
            raise ValueError(msg)

        # writable + no version = fresh (for metador), initialize it
        if ver is None:
            self._raw[M.METADOR_VERSION_PATH] = M.METADOR_SPEC_VERSION
            self._raw[M.METADOR_UUID_PATH] = str(uuid1())
        # if we're here, we have a prepared container TOC structure

        # proceed to initialize TOC
        self._driver_type: MetadorDriverEnum = get_driver_type(self._raw)

        self._packages = TOCPackages(self._raw)
        self._schemas = TOCSchemas(self._raw, self._packages)
        self._links = TOCLinks(self._raw, self._schemas)

    # ----

    @property
    def driver_type(self) -> MetadorDriverEnum:
        """Return the type of the container driver."""
        return self._driver_type

    @property
    def driver(self) -> Type[MetadorDriver]:
        """Return the container driver class used by the container."""
        return METADOR_DRIVERS[self.driver_type]

    @property
    def source(self) -> Any:
        """Return data underlying thes container (file, set of files, etc. used with the driver)."""
        return get_source(self._raw, self.driver_type)

    # ----

    @property
    def container_uuid(self) -> UUID:
        """Return UUID of the container."""
        uuid = self._raw[M.METADOR_UUID_PATH]
        uuid_ds = cast(H5DatasetLike, uuid)
        return UUID(uuid_ds[()].decode("utf-8"))

    @property
    def spec_version(self) -> List[int]:
        """Return Metador container specification version of the container."""
        ver = cast(H5DatasetLike, self._raw[M.METADOR_VERSION_PATH])
        return list(map(int, ver[()].decode("utf-8").split(".")))

    @property
    def schemas(self):
        """Information about all schemas used for metadata objects in this container."""
        return self._schemas

    @overload
    def query(
        self, schema: str, version: Optional[SemVerTuple] = None
    ) -> Dict[MetadorNode, MetadataSchema]:
        ...

    @overload
    def query(
        self, schema: Type[S], version: Optional[SemVerTuple] = None
    ) -> Dict[MetadorNode, S]:
        ...

    def query(
        self, schema: Union[str, Type[S]] = "", version: Optional[SemVerTuple] = None
    ) -> Dict[MetadorNode, Union[MetadataSchema, S]]:
        """Return nodes that contain a metadata object valid for the given schema."""
        schema_name, schema_ver = plugin_args(schema, version)
        ret: Dict[MetadorNode, Union[MetadataSchema, S]] = {}
        if obj := self._container.meta.get(schema_name, schema_ver):
            ret[self._container["/"]] = obj

        def collect_nodes(_, node: MetadorNode):
            if obj := node.meta.get(schema_name, schema_ver):
                ret[node] = obj

        self._container.visititems(collect_nodes)
        return ret
