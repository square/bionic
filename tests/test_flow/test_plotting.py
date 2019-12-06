import pytest

import bionic as bn


def test_pyplot_no_parens(builder):
    @builder
    @bn.pyplot
    def plot(pyplot):
        ax = pyplot.subplot()
        ax.plot([1, 2, 3], [1, 3, 9])

    img = builder.build().get('plot')
    assert img.width > 0
    assert img.height > 0


def test_pyplot_no_args(builder):
    @builder
    @bn.pyplot()
    def plot(pyplot):
        ax = pyplot.subplot()
        ax.plot([1, 2, 3], [1, 3, 9])

    img = builder.build().get('plot')
    assert img.width > 0
    assert img.height > 0


def test_pyplot_name_arg(builder):
    @builder
    @bn.pyplot('plt')
    def plot(plt):
        ax = plt.subplot()
        ax.plot([1, 2, 3], [1, 3, 9])

    img = builder.build().get('plot')
    assert img.width > 0
    assert img.height > 0


def test_pyplot_missing_dep(builder):
    with pytest.raises(ValueError):
        @builder
        @bn.pyplot
        def plot(some_arg):
            pass
