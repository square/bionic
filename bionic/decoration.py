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
import warnings

from .exception import AttributeValidationError
from .provider import FunctionProvider
from .util import oneline


@attr.s
class DecorationAccumulator:
    provider = attr.ib()

    protocol = attr.ib(default=None)
    docs = attr.ib(default=None)
    can_persist = attr.ib(default=None)
    should_memoize = attr.ib(default=None)

    def wrap_provider(self, wrapper_fn, *args, **kwargs):
        self.provider = wrapper_fn(self.provider, *args, **kwargs)

    def update_attr(
        self, attr_name, attr_value, decorator_name, raise_if_already_set=True
    ):
        old_attr_value = getattr(self, attr_name)
        if old_attr_value is not None:
            message = f"""
            Tried to use {decorator_name} with value {attr_value!r},
            but this decorator was already used with value {old_attr_value!r}
            """
            if raise_if_already_set:
                raise AttributeValidationError(oneline(message))
            else:
                preamble = """
                Applying this type of decorator multiple times is deprecated and will
                become an error condition in a future release; please remove all but
                the uppermost uses of this decorator. Details:
                """
                warnings.warn(oneline(preamble) + "\n" + oneline(message))
        setattr(self, attr_name, attr_value)


def decorator_updating_accumulator(acc_update_func):
    """
    Creates a decorator which applies a transformation to the DecorationAccumulator
    attached to the decorated function. (If no accumulator is attached, the decorator
    will initialize one.)
    """

    def decorator(func):
        init_accumulator_if_not_set_on_func(func)
        acc = get_accumulator_from_func(func)
        acc_update_func(acc)
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
