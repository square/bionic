'''
Convenience functions for handling arguments based on their type.  These can be
used to provide "Pandas-like" APIs that accept (e.g.) either a string or a list
of strings.
'''


def str_or_seq_as_list(value):
    if isinstance(value, str):
        return [value]
    elif is_iterable(value):
        return list(value)
    else:
        raise TypeError(f'Expected a string or sequence; got {value!r}')


def str_or_seq_or_none_as_list(value):
    if isinstance(value, str):
        return [value]
    elif is_iterable(value):
        return list(value)
    elif value is None:
        return []
    else:
        raise TypeError(f'Expected a string or sequence or None; got {value!r}')


def is_iterable(x):
    try:
        iter(x)
        return True
    except TypeError:
        return False
