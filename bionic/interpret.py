'''
Convenience functions for handling arguments based on their type.  These can be
used to provide "Pandas-like" APIs that accept (e.g.) either a string or a list
of strings.
'''


from past.builtins import basestring


def none_or_seq_or_obj_as_list(value):
    if value is None:
        return []
    elif is_iterable(value):
        return list(value)
    else:
        return [value]


def str_or_seq_as_list(value):
    if isinstance(value, basestring):
        return [value]
    elif is_iterable(value):
        return list(value)
    else:
        raise TypeError('Expected a string or sequence; got %r' % value)


def is_iterable(x):
    try:
        iter(x)
        return True
    except TypeError:
        return False
