import builtins
import importlib
from sys import modules as module_registry
from fnmatch import fnmatch
from sysconfig import get_paths as sysconfig_paths


def recursive_reload(module):
    """
    Helper method to reload a module recursively. If a module imports a set of
    modules, then the modules in the set are also reloaded and so on.

    Modules that are part of the current python installation are not reloaded.
    For example, modules part of python standard library or modules installed
    through pip (or some other package manager that use distutils).

    Also note that this method may not be able to handle dynamic imports that
    only happens at runtime. For example, if a module imports another module
    only when a certain method is executed, reloading the former module does
    not guarantee that the latter module is reloaded.
    """

    original_import = builtins.__import__
    already_reloaded = set()

    def custom_import(name, globals=None, locals=None, fromlist=[], level=0):
        if name in module_registry:
            module = module_registry[name]
            if name not in already_reloaded and not is_internal_module(module):
                already_reloaded.add(name)
                importlib.reload(module)
        return original_import(name, globals, locals, fromlist, level)

    try:
        builtins.__import__ = custom_import
        return importlib.reload(module)
    finally:
        builtins.__import__ = original_import


def is_internal_module(module):
    return not hasattr(module, "__file__") or is_internal_file(module.__file__)


def is_internal_file(filename):
    """
    Helper method that determines whether the provided file is internal
    to Python, i.e., it's in the Python installation paths.
    """
    return any(
        fnmatch(filename, file_dir + "/*") for file_dir in sysconfig_paths().values()
    )
