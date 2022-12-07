"""This module defines a metaclass that should be used for all plugin types."""

from .types import to_semver_str


class MarkerMixin:
    """Base class for Metador-internal marker mixins.

    It can be used to hand out copies of classes with markings,
    without actually modifying the original class.
    """

    @classmethod
    def _fieldname(cls):
        return f"__{cls.__name__}_unwrapped__"

    @classmethod
    def _is_marked(cls, c) -> bool:
        """Return whether `c` is proper subclass of this marker."""
        return c is not cls and issubclass(c, cls)

    @classmethod
    def _mark_class(cls, c):
        """Mark a class with this marker mixin."""
        if cls._is_marked(c):
            raise TypeError(f"{c} already marked by {cls}!")

        ret = c.__class__(c.__name__, (cls, c), {})
        setattr(ret, cls._fieldname(), c)

        # NOTE: discouraged!
        # https://docs.python.org/3/howto/annotations.html#annotations-howto
        # ----
        # anns = getattr(ret, "__annotations__", {})
        # anns[unw_field] = ClassVar[Type]
        # ret.__annotations__ = anns

        return ret

    @classmethod
    def _unwrap(cls, c):
        """Return the original class, or None if given argument is not marked."""
        if issubclass(c, cls):
            return getattr(c, cls._fieldname())
        else:
            return None


class UndefVersion(MarkerMixin):
    """Marker for a plugin class retrieved with no specified version.

    We have to do this crazy thing, because wrapt.ObjectProxy-wrapped
    classes can be transparently derived, and what is even worse,
    the derived class is not wrapped anymore.

    The mixin subclass approach therefore makes more sense here, as
    the metaclass then can check for its presence.
    """

    @classmethod
    def _mark_class(cls, c):
        # NOTE: we also want to mark nested non-plugins to prevent subclassing
        # so we do not assume that cls.Plugin is defined
        ret = super()._mark_class(c)
        # make sure that the Plugin section *is* actually inherited,
        # (normally this is prevented by the plugin metaclass)
        # that way we can use the marked class as if it was the real one
        if not ret.__dict__.get("Plugin"):
            ret.Plugin = c.Plugin

        return ret


class PluginMetaclassMixin(type):
    """Metaclass mixin to be used with plugins of any group.

    It provides an is_plugin property to classes to quickly check if they
    seem to be valid registered plugins.

    It ensures that:
    * the `Plugin` inner class is not automatically inherited
    * registered Plugin classes cannot be subclassed if loaded without a fixed version

    For the second part, this works together with the PluginGroup implementation,
    which makes sure that schemas requested without versions are not actually handed out,
    but instead users get a subclass with the `UndefVersion` mixin we can detect here.
    """

    def __repr__(self):
        # add plugin name and version to default class repr
        if c := UndefVersion._unwrap(self):
            # indicate the attached UndefVersion
            return f"{repr(c)} (version unspecified)"
        else:
            # indicate loaded plugin name and version
            pg_str = ""
            if pgi := self.__dict__.get("Plugin"):
                pgi = self.Plugin
                pg_str = f" ({pgi.name} {to_semver_str(pgi.version)})"

            return f"{super().__repr__()}{pg_str}"

    def __new__(cls, name, bases, dct):
        # prevent inheriting from a plugin accessed without stated version
        for b in bases:
            if UndefVersion._is_marked(b):
                if pgi := b.__dict__.get("Plugin"):
                    ref = f"plugin '{pgi.name}'"
                else:
                    ref = f"{UndefVersion._unwrap(b)} originating from a plugin"
                msg = f"{name}: Cannot inherit from {ref} of unspecified version!"
                raise TypeError(msg)

        # prevent inheriting inner Plugin class by setting it to None
        if "Plugin" not in dct:
            dct["Plugin"] = None

        # hide special marker base class from parent metaclass (if present)
        # so it does not have to know about any of this happening
        # (otherwise it could interfere with other checks)
        # NOTE: needed e.g. for schemas to work properly
        filt_bases = tuple(b for b in bases if b is not UndefVersion)
        ret = super().__new__(cls, name, filt_bases, dct)

        # add marker back, as if it was present all along
        if len(filt_bases) < len(bases):
            ret.__bases__ = (UndefVersion, *ret.__bases__)

        return ret
