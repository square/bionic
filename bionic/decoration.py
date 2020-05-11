"""
Code for creating and applying Bionic decorators.

Bionic decorators are expected to be applied like this:

    @builder
    @decorator1
    @decorator2
    def func(arg1, arg2, ...):
        ...

Each decorator attaches information to the decorated function by by creating or updating
a DecorationAccumulator object, set as an attribute on the function. The assumption is
that the decorator at the top, (``@builder``) will be a FlowBuilder object which removes
this accumulator object and uses it to define a new entity.
"""

import attr

from .provider import FunctionProvider


@attr.s
class DecorationAccumulator:
    provider = attr.ib()

    def wrap_provider(self, wrapper_fn, *args, **kwargs):
        self.provider = wrapper_fn(self.provider, *args, **kwargs)


def decorator_wrapping_provider(wrapper_fn, *args, **kwargs):
    """
    Returns a decorator which transforms the accumulated provider on a decorated
    function by applying the given wrapping function and arguments.
    """

    def decorator(func):
        init_accumulator_if_not_set_on_func(func)
        acc = get_accumulator_from_func(func)
        acc.wrap_provider(wrapper_fn, *args, **kwargs)
        return func

    return decorator


ACC_ATTR_NAME = "bionic_decorator_accumulator"


def init_accumulator_if_not_set_on_func(func):
    if not hasattr(func, ACC_ATTR_NAME):
        setattr(
            func, ACC_ATTR_NAME, DecorationAccumulator(provider=FunctionProvider(func)),
        )


def get_accumulator_from_func(func):
    return getattr(func, ACC_ATTR_NAME)


def pop_accumulator_from_func(func):
    acc = get_accumulator_from_func(func)
    delattr(func, ACC_ATTR_NAME)
    return acc
