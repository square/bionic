import pytest

from ..helpers import RoundingProtocol
import bionic as bn


@pytest.fixture(scope="function")
def preset_builder(builder):
    builder.assign("n", values=[1, 2, 3])

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
    @bn.pyplot("plt")
    @bn.gather("n", ["xs", "ys"])
    def plot(gather_df, plt):
        for row in gather_df.itertuples():
            plt.plot(row.xs, row.ys)

    img = builder.build().get("plot")
    assert img.width > 0
    assert img.height > 0


def test_gather_then_pyplot(preset_builder):
    builder = preset_builder

    @builder
    @bn.gather("n", ["xs", "ys"])
    @bn.pyplot("plt")
    def plot(gather_df, plt):
        for row in gather_df.itertuples():
            plt.plot(row.xs, row.ys)

    img = builder.build().get("plot")
    assert img.width > 0
    assert img.height > 0


def test_outputs_with_multiplicity(builder):
    builder.assign("x", values=[2, 3])
    builder.assign("y", 4)

    @builder
    @bn.outputs("x_plus_y", "xy")
    def _(x, y):
        return (x + y), (x * y)

    @builder
    @bn.gather("xy")
    def sum_xy(gather_df):
        return gather_df["xy"].sum()

    @builder
    @bn.gather("x_plus_y")
    def sum_x_plus_y(gather_df):
        return gather_df["x_plus_y"].sum()

    flow = builder.build()
    assert flow.get("sum_xy") == 20
    assert flow.get("sum_x_plus_y") == 13

    flow = flow.clearing_cases("x")
    assert flow.get("sum_xy") == 0
    assert flow.get("sum_x_plus_y") == 0


def test_outputs_with_protocols(builder):
    @builder
    @RoundingProtocol()
    @bn.outputs("x", "y")
    def _():
        return 0.1, 1.9

    flow = builder.build()

    assert flow.get("x") == 0
    assert flow.get("y") == 2
