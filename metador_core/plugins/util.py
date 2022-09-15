"""General utitilies with relevance for plugins."""
from typing import Optional, Type, TypeVar

from .interface import IsPlugin, PluginGroup


def is_notebook() -> bool:
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


T = TypeVar("T", bound=IsPlugin)


def register_in_group(pgroup: PluginGroup[T], plugin: Optional[Type[T]] = None):
    """Register and load a plugin manually, without defining an entry point."""
    if not is_notebook():
        raise RuntimeError("This is not supposed to be used outside of notebooks!")

    def manual_register(plugin: Type[T]) -> Type[T]:
        pginfo = plugin.Plugin
        pgroup._ENTRY_POINTS[pginfo.name] = None
        pgroup._LOADED_PLUGINS[pginfo.name] = plugin
        pgroup._load_plugin(pginfo.name, plugin)
        print(f"Notebook: Plugin '{pginfo.name}' registered in '{pgroup.name}' group!")
        return plugin

    if not plugin:
        return manual_register  # used as decorator
    else:
        manual_register(plugin)  # used as normal function
