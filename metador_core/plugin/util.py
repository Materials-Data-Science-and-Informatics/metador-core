"""General utitilies with relevance for plugins."""
from typing import TYPE_CHECKING, Any, Optional, Type, TypeVar

from ..util import eprint
from .types import PluginLike, is_pluginlike, to_ep_name

if TYPE_CHECKING:  # pragma: no cover
    from .interface import PluginGroup
else:
    PluginGroup = Any


# ----
# helpers for checking plugins (also to be used in PluginGroup subclasses):


def implements_method(plugin, base_method):
    ep_method = plugin.__dict__.get(base_method.__name__)
    return ep_method is not None and base_method != ep_method


def check_implements_method(name: str, plugin, base_method):
    """Check whether plugin overrides a method of its superclass."""
    if not implements_method(plugin, base_method):
        msg = f"{name}: {plugin} does not implement {base_method.__name__}!"
        raise TypeError(msg)


def check_is_subclass(name: str, plugin, base):
    """Check whether plugin has expected parent class (helper method)."""
    if not issubclass(plugin, base):
        msg = f"{name}: {plugin} is not subclass of {base}!"
        raise TypeError(msg)


# ----


def is_notebook() -> bool:  # pragma: no cover
    # https://stackoverflow.com/a/39662359
    try:
        # get_ipython() is defined globally in ipython-like env!
        shell = get_ipython().__class__.__name__  # type: ignore

        if shell == "ZMQInteractiveShell":
            return True  # Jupyter notebook or qtconsole
        elif shell == "TerminalInteractiveShell":
            return False  # Terminal running IPython
        else:
            return False  # Other type (?)
    except NameError:
        return False  # Probably standard Python interpreter


T = TypeVar("T", bound=PluginLike)


def register_in_group(
    pgroup: PluginGroup,
    plugin: Optional[Type[T]] = None,
    *,
    violently: bool = False,
):
    """Register and load a plugin manually, without defining an entry point."""
    if not violently and not is_notebook():
        raise RuntimeError("This is not supposed to be used outside of notebooks!")

    def manual_register(plugin: Type[T]) -> Type[T]:
        pginfo = plugin.Plugin
        ep_name = to_ep_name(pginfo.name, pginfo.version)
        pg_ref = pgroup.PluginRef(name=pginfo.name, version=pginfo.version)

        pgroup._ENTRY_POINTS[ep_name] = None
        pgroup._LOADED_PLUGINS[pg_ref] = plugin
        if pg_ref.name not in pgroup._VERSIONS:
            pgroup._VERSIONS[pg_ref.name] = []
        pgroup._VERSIONS[pg_ref.name].append(pg_ref)

        pgroup._load_plugin(ep_name, plugin)
        if not violently:
            eprint(
                f"Notebook: Plugin '{pginfo.name}' registered in '{pgroup.name}' group!"
            )  # pragma: no cover
        return plugin

    if not plugin:
        return manual_register  # used as decorator
    else:
        if not is_pluginlike(plugin, check_group=False):
            raise RuntimeError("This class has no inner Plugin class!")

        manual_register(plugin)  # used as normal function
