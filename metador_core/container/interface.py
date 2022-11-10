from __future__ import annotations

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
    Type,
    TypeVar,
    Union,
    ValuesView,
    cast,
    overload,
)
from uuid import UUID, uuid1

from ..plugin.interface import _from_ep_name, _to_ep_name
from ..plugins import schemas
from ..schema import MetadataSchema
from ..schema.plugins import PluginPkgMeta, PluginRef
from . import utils as M
from .drivers import (
    METADOR_DRIVERS,
    MetadorDriver,
    MetadorDriverEnum,
    get_driver_type,
    get_source,
)
from .types import H5DatasetLike, H5GroupLike

if TYPE_CHECKING:
    from .wrappers import MetadorContainer, MetadorNode

S = TypeVar("S", bound=MetadataSchema)


class MetadorContainerInfo:
    """Initializes and provides MetadorContainer technical metadata structure."""

    def __init__(self, mc: MetadorContainer):
        self._raw: MetadorDriver = mc.__wrapped__
        self._driver_type: MetadorDriverEnum = get_driver_type(self._raw)

        ver = self.spec_version if M.METADOR_VERSION_PATH in self._raw else None
        if ver is None and mc.mode == "r":
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

    @property
    def uuid(self) -> UUID:
        """Return UUID of the container."""
        uuid = self._raw[M.METADOR_UUID_PATH]
        uuid_ds = cast(H5DatasetLike, uuid)
        return UUID(uuid_ds[()].decode("utf-8"))

    @property
    def spec_version(self) -> List[int]:
        """Return Metador container specification version of the container."""
        ver = cast(H5DatasetLike, self._raw[M.METADOR_VERSION_PATH])
        return list(map(int, ver[()].decode("utf-8").split(".")))


# ----


class MetadorMeta:
    """Interface to Metador metadata objects stored at a single HDF5 node."""

    def __init__(self, node: MetadorNode):
        self._mc = node._self_container  # needed for read/write access
        self._read_only = node.read_only  # inherited from node marker
        self._skel_only = node.skel_only  # inherited from node marker
        # used all over the place, so precompute once:
        self._base_dir = M.to_meta_base_path(
            node.name, is_dataset=isinstance(node, H5DatasetLike)
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

        assert isinstance(dir, H5GroupLike)
        for name, node in dir.items():
            if name[0] == "=":  # metadata dataset byte objects start with =
                return node  # return first match

    def _is_unknown_schema(self, schema_name: str) -> bool:
        # we have no such schema in our plugin environment
        unknown_nonexisting: bool = schema_name not in schemas.keys()
        # we may have such a schema with that name, but its not the right one!
        unknown_but_existing: bool = schema_name in self._mc.toc.unknown_schemas
        return unknown_nonexisting or unknown_but_existing

    def _guard_unknown_schema(self, schema_name: str):
        if self._is_unknown_schema(schema_name):
            raise ValueError(f"Unknown or incompatible schema: {schema_name}")

    def _guard_read_only(self):
        if self._read_only:
            raise ValueError("This node is marked as read_only!")

    def _guard_skel_only(self):
        if self._skel_only:
            raise ValueError("This node is marked as skel_only!")

    # public interface:

    def get_bytes(self, schema_name: str) -> Optional[bytes]:
        """Get bytes for an object belonging (exactly!) to the given schema, if present.

        This also works for unknown schemas (we can retrieve, but cannot parse those).
        """
        self._guard_skel_only()
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
        self._guard_skel_only()
        return set(map(lambda x: x[1], self.items()))  # type: ignore

    def items(self) -> ItemsView[str, MetadataSchema]:
        self._guard_skel_only()
        return {name: self.get[name] for name in self.find()}  # type: ignore

    def __delitem__(self, schema: Union[str, MetadataSchema]):
        schema_name: str = schema if isinstance(schema, str) else schema.Plugin.name
        return self._delitem(schema_name, unlink_in_toc=True)

    # all following only work with known (i.e. registered/correct) schema:

    def __setitem__(
        self, schema: Union[str, Type[S]], value: Union[Dict[str, Any], MetadataSchema]
    ):
        self._guard_read_only()
        schema_name: str = schema if isinstance(schema, str) else schema.Plugin.name
        self._guard_unknown_schema(schema_name)
        if self._get_raw(schema_name):
            raise ValueError(
                f"Metadata object for schema {schema_name} already exists!"
            )

        # get the correct installed plugin class (don't trust the user class!)
        schema_class: Type[MetadataSchema] = schemas._get_unsafe(schema_name)

        # reject auxiliary schemas
        if schema_class.Plugin.auxiliary:
            msg = f"Cannot attach instances of auxiliary schema '{schema_name}' to a node!"
            raise TypeError(msg)

        # handle and check the passed metadata
        if isinstance(value, schema_class):
            validated = value  # skip validation, already correct model
        else:
            # try to convert/parse it:
            val = value.dict() if isinstance(value, MetadataSchema) else value
            # let ValidationError be raised (if it happens)
            validated = schema_class.parse_obj(val)

        # all good -> store it
        obj_path = self._mc.toc._register_link(self._base_dir, schema_name)
        self._mc.__wrapped__[obj_path] = bytes(validated)  # RAW

        # notify TOC dependency tracking to register possibly new dependency.
        # we do it here instead of in toc._register_link, because _register_link
        # is also used in cases where we might not have the schema installed,
        # e.g. when fixing the TOC.
        # here, adding new metadata only works with a properly installed schema
        # so we can get all required information.
        self._mc.toc._notify_used_schema(schema_class.ref())

    # following have transitive semantics (i.e. logic also works with parent schemas)

    @overload
    def get(self, schema: str) -> Optional[MetadataSchema]:
        ...

    @overload
    def get(self, schema: Type[S]) -> Optional[S]:
        ...

    def get(self, schema: Union[str, Type[S]]) -> Optional[Union[MetadataSchema, S]]:
        """Get a parsed metadata object (if it exists) matching the given known schema.

        Will also accept a child schema object and parse it as the parent schema.
        If multiple suitable objects are found, picks the alphabetically first to parse it.
        """
        self._guard_skel_only()
        schema_name: str = schema if isinstance(schema, str) else schema.Plugin.name
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
        return cast(S, schemas[schema_name].parse_raw(dat))

    @overload
    def __getitem__(self, schema: str) -> MetadataSchema:
        ...

    @overload
    def __getitem__(self, schema: Type[S]) -> S:
        ...

    def __getitem__(self, schema: Union[str, Type[S]]) -> Union[S, MetadataSchema]:
        """Like get, but will raise KeyError on failure."""
        ret = self.get(schema)
        if ret is None:
            raise KeyError(schema)
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
        if pp is None and schema_name in schemas.keys():
            pp = "/".join(schemas.parent_path(schema_name))
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
            name, version = _from_ep_name(empty_group.split("/")[-1])
            self._notify_unused_schema(schemas.PluginRef(name=name, version=version))

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
        return f"{M.METADOR_PKGS_PATH}/{pkg_name}"

    def _ref_node_path(cls, schema_ref: PluginRef) -> str:
        return f"{M.METADOR_SCHEMAS_PATH}/{_to_ep_name(schema_ref)}"

    def _notify_used_schema(self, schema_ref: PluginRef):
        """Notify that a schema is used in the container (metadata object is created/updated).

        If no dependency is tracked yet, will add it. If it is, will update to the one
        from the environment.

        This assumes that the already existing schemas and the new one are compatible!
        """
        # store plugin ref (if not added yet)
        ref_node_path = self._ref_node_path(schema_ref)
        if ref_node_path not in self._container.__wrapped__:
            self._container.__wrapped__[ref_node_path] = (
                schemas.get(schema_ref.name, schema_ref.version)
                .schema_json()
                .encode("utf-8")
            )

        # update/create providing package
        env_pkg_info: PluginPkgMeta = schemas.provider(schema_ref)
        pkg_name = str(env_pkg_info.name)

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
        self._provider[schema_ref] = pkg_name

        # make sure schema is tracked as "used"
        if curr_info is None:
            self._used[pkg_name] = set()
        self._used[pkg_name].add(schema_ref)
        if schema_ref not in self._parent_paths:
            ppath = "/".join(schemas.parent_path(schema_ref))
            self._parent_paths[schema_ref] = ppath

    def _notify_unused_schema(self, schema_ref: PluginRef):
        """Notify that a schema is not used at any container node anymore.

        If after that no schema of a listed dep package is used,
        this dependency will be removed from the container.
        """
        rawcont = self._container.__wrapped__
        pkg_name = self._provider.get(schema_ref)
        if pkg_name is None:
            # apparently container has no package info for that schema
            # this is not allowed, but we can just ignore it to handle
            # slightly broken containers
            return

        del self._provider[schema_ref]
        self._used[pkg_name].remove(schema_ref)
        if schema_ref in self._used_schemas:
            del self._used_schemas[schema_ref]
            del rawcont[self._ref_node_path(schema_ref)]

        if not self._used[pkg_name]:
            # no schemas of its providing package are used anymore.
            # -> kill this dep in cache and in container
            del self._used[pkg_name]
            del self._pkginfos[pkg_name]
            del rawcont[self._dep_node_path(pkg_name)]

        deps_grp = cast(H5GroupLike, rawcont[M.METADOR_PKGS_PATH])
        if not len(deps_grp):  # RAW
            # no package metadata -> can kill dir
            del rawcont[M.METADOR_PKGS_PATH]  # RAW

    # ---- public API ----

    def __init__(self, container: MetadorContainer):
        self._container = container
        rawcont = self._container.__wrapped__

        # 1. compute parent paths based on present TOC structure
        # (we need it to efficiently traverse possibly unknown schemas/objects)
        # 2. collect metadata object uuids

        # refs for actually embedded schema instances
        self._used_schemas: Dict[str, PluginRef] = {}

        if M.METADOR_SCHEMAS_PATH in rawcont:  # RAW
            refs_grp = rawcont.require_group(M.METADOR_SCHEMAS_PATH)  # RAW
            for name, node in refs_grp.items():  # RAW
                ref = schemas.PluginRef.parse_raw(cast(H5DatasetLike, node)[()])  # RAW
                self._used_schemas[name] = ref

        # schema name -> full/registered/parent/sequence
        self._parent_paths: Dict[str, str] = {}
        # uuid -> path in dataset
        self._toc_path_from_uuid: Dict[str, str] = {}

        def scan_schemas(path):
            path_segs = path.split("/")
            schema_name = path_segs[-1]
            if schema_name[0] == "=":  # links/metadata entries start with =
                uuid_str = schema_name[1:]
                self._toc_path_from_uuid[uuid_str] = f"{M.METADOR_TOC_PATH}/{path}"
            else:  # a schema name -> infer parent relationship
                self._parent_paths[schema_name] = path

        if M.METADOR_TOC_PATH in rawcont:  # RAW
            toc_grp = rawcont.require_group(M.METADOR_TOC_PATH)  # RAW
            toc_grp.visit(scan_schemas)  # RAW

        # 3. init structure tracking schema dependencies

        # package name -> package info:
        self._pkginfos: Dict[str, PluginPkgMeta] = {}
        # schema name -> package name:
        self._provider: Dict[str, str] = {}
        # package name -> names of its schemas used in container
        self._used: Dict[str, Set[str]] = {}

        # parse package infos if they exist
        if M.METADOR_PKGS_PATH in rawcont:  # RAW
            deps_grp = rawcont.require_group(M.METADOR_PKGS_PATH)  # RAW
            for name, node in deps_grp.items():  # RAW
                info = PluginPkgMeta.parse_raw(cast(H5DatasetLike, node)[()])  # RAW
                self._pkginfos[name] = info
                self._used[name] = set()
                # lookup for schema -> used package
                for schema in info.plugins[schemas.name]:
                    self._provider[schema] = name

        # initialize tracking of used schemas
        for schema_name in self.schemas():
            # we assume that each schema has a provider dep!
            pkg_name = self._provider[schema_name]
            # add schema to list of actually used schemas provided by this dep
            self._used[pkg_name].add(schema_name)

        # 3. collect the schemas that we don't know already
        self._unknown_schemas = set(self._parent_paths.keys()) - set(schemas.keys())

    def schemas(self, *, include_parents: bool = True) -> Set[str]:
        """Return names of all schemas used in the container."""
        if include_parents:
            schemas = map(lambda p: set(p.split("/")), self._parent_paths.values())
            return set.union(set(), *schemas)
        else:
            return set(self._used_schemas.keys())

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
            msg = f"Did not find metadata of package providing schema: '{schema_name}'"
            raise KeyError(msg)
        return self._pkginfos[pkg_name]

    @overload
    def query(self, schema: str) -> Dict[MetadorNode, MetadataSchema]:
        ...

    @overload
    def query(self, schema: Type[S]) -> Dict[MetadorNode, S]:
        ...

    def query(
        self, schema: Union[str, Type[S]] = ""
    ) -> Dict[MetadorNode, Union[MetadataSchema, S]]:
        """Return nodes that contain a metadata object valid for the given schema."""
        schema_name = schema if isinstance(schema, str) else schema.Plugin.name
        ret = {}

        def collect_nodes(_, node):
            if (obj := node.meta.get(schema_name)) is not None:
                ret[node] = obj

        self._container.visititems(collect_nodes)
        return ret
