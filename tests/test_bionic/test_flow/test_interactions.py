import pytest

import bionic as bn


@pytest.fixture(scope='function')
def preset_builder(builder):
    builder.assign('n', values=[1, 2, 3])

    @builder
    def xs(n):
        return list(range(n))

    @builder
    def ys(xs):
        return [x ** 2 for x in xs]

    return builder


def test_pyplot_then_gather(preset_builder):
    builder = preset_builder

    @builder
    @bn.pyplot('plt')
    @bn.gather('n', ['xs', 'ys'])
    def plot(gather_df, plt):
        for row in gather_df.itertuples():
            plt.plot(row.xs, row.ys)

    img = builder.build().get('plot')
    assert img.width > 0
    assert img.height > 0


def test_gather_then_pyplot(preset_builder):
    builder = preset_builder

    @builder
    @bn.gather('n', ['xs', 'ys'])
    @bn.pyplot('plt')
    def plot(gather_df, plt):
        for row in gather_df.itertuples():
            plt.plot(row.xs, row.ys)

    img = builder.build().get('plot')
    assert img.width > 0
    assert img.height > 0
