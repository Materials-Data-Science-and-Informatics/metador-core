# from typing import ClassVar, Type


class MarkerMixin:
    """Base class for Metador-internal marker mixins.

    It can be used to hand out copies of classes with markings,
    without actually modifying the original class.
    """

    @classmethod
    def _fieldname(cls):
        return f"__{cls.__name__}_unwrapped__"

    @classmethod
    def is_marked(cls, c) -> bool:
        """Return whether `c` is proper subclass of this marker."""
        return c is not cls and issubclass(c, cls)

    @classmethod
    def mark_class(cls, c):
        """Mark a class with this marker mixin."""
        if cls.is_marked(c):
            raise TypeError(f"{c} already marked by {cls}!")

        ret = c.__class__(c.__name__, (cls, c), {})
        setattr(ret, cls._fieldname(), c)

        # anns = getattr(ret, "__annotations__", {})
        # anns[unw_field] = ClassVar[Type]
        # ret.__annotations__ = anns

        return ret

    @classmethod
    def unwrap(cls, c):
        if issubclass(c, cls):
            return getattr(c, cls._fieldname())
        else:
            return None


class UndefVersion(MarkerMixin):
    """Marker for a plugin class retrieved with no specified version."""

    @classmethod
    def mark_class(cls, c):
        # a plugin has a no non-None Plugin attribute -> wrong use
        if not getattr(c, "Plugin", None):
            raise TypeError(f"{c} does not look like a Plugin class!")

        ret = super().mark_class(c)

        # make sure that the Plugin section *is* actually inherited,
        # (normally this is prevented by the plugin metaclass)
        #
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
        if c := UndefVersion.unwrap(self):
            # indicate UndefVersion marking, if present
            return f"{repr(c)} (version unspecified)"
        else:
            # indicate loaded plugin name and version
            pg_str = ""
            if self.is_plugin:
                pgi = self.Plugin
                pg_str = f" ({pgi.name} {pgi.version_string()})"

            return f"{super().__repr__()}{pg_str}"

    @property
    def is_plugin(self):
        """Return whether this schema is a (possibly marked) installed schema plugin."""
        c = UndefVersion.unwrap(self) or self  # get real underlying class
        # check its exactly a registered plugin, if it has a Plugin section
        if info := c.__dict__.get("Plugin"):
            from ..schema.plugins import PluginBase

            if not isinstance(info, PluginBase):
                return False

            from ..plugins import plugingroups

            return plugingroups[info.group]._get_unsafe(info.name) is c
        else:
            return False

    def __new__(cls, name, bases, dct):
        # prevent inheriting from a plugin accessed without stating a version
        for b in bases:
            if UndefVersion.is_marked(b):
                msg = f"{name}: Cannot inherit from plugin '{b.Plugin.name}' of unspecified version!"
                raise TypeError(msg)

        # prevent inheriting inner Plugin class by setting it to None
        if "Plugin" not in dct:
            dct["Plugin"] = None

        # hide special marker base class from parent metaclass (if present)
        # so it does not have to know about any of this
        # (otherwise it could interfere with other checks)
        filt_bases = tuple(b for b in bases if b is not UndefVersion)
        ret = super().__new__(cls, name, filt_bases, dct)

        # add marker back, as if it was present
        if len(filt_bases) < len(bases):
            ret.__bases__ = (UndefVersion, *ret.__bases__)

        return ret
