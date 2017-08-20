from functools import wraps
import mock
import os
import re

norm_pattern = re.compile(r'[/|.]')

def patch_plugin_file(*patch_args, **patch_kwargs):
    """
    Decorator used to search for in items:
    """
    root, filename = os.path.split(patch_args[0])
    module_name, file_ext = os.path.splitext(filename)
    namespace = '_'.join([re.sub(norm_pattern, '__', root), module_name])

    import sys
    found_modules = [key for key in sys.modules.keys() if namespace in key]

    if len(found_modules) != 1:
        raise(NameError('Tried to find 1 module from file %s but found: %s' %
                        (found_modules, namespace)))

    module = sys.modules[found_modules.pop()]

    def patch_decorator(func, *patch_decorator_args):
        @wraps(func)
        @mock.patch.object(module, *patch_args[1:], **patch_kwargs)
        def wrapper(*args, **kwargs):
            return func(*(args + patch_decorator_args), **kwargs)
        return wrapper
    return patch_decorator
