import pytest

import pandas as pd
import pandas.testing as pdt

from ..helpers import RoundingProtocol

import bionic as bn
from bionic.exception import UndefinedEntityError


@pytest.fixture(scope='function')
def preset_builder(builder):
    builder.assign('x', 2)
    builder.assign('y', 3)

    return builder


def test_output(preset_builder):
    builder = preset_builder

    @builder
    @bn.output('g')
    def f(x, y):
        return x + y

    flow = builder.build()

    assert flow.get('g') == 5

    with pytest.raises(UndefinedEntityError):
        flow.get('f')


def test_outputs(builder):
    builder.assign('numerator', 14)
    builder.assign('denominator', 3)

    @builder
    @bn.outputs('quotient', 'remainder')
    def divide(numerator, denominator):
        quotient = numerator // denominator
        remainder = numerator % denominator
        return quotient, remainder

    flow = builder.build()

    assert flow.get('quotient') == 4
    assert flow.get('remainder') == 2

    with pytest.raises(UndefinedEntityError):
        flow.get('divide')


def test_outputs_custom_protocols_first(builder):
    builder.assign('location', (37.7, -122.4))

    @builder
    @bn.outputs('lat', 'lon')
    def latlon(location):
        return location

    @builder
    @RoundingProtocol()
    @bn.outputs('rounded_lat', 'rounded_lon')
    def rounded_latlon(lat, lon):
        return lat, lon

    @builder
    @bn.outputs('other_rounded_lat', 'other_rounded_lon')
    @RoundingProtocol()
    def other_rounded_latlon(lat, lon):
        return lat, lon

    flow = builder.build()

    assert flow.get('lat') == 37.7
    assert flow.get('lon') == -122.4

    assert flow.get('rounded_lat') == 38
    assert flow.get('rounded_lon') == -122

    assert flow.get('other_rounded_lat') == 38
    assert flow.get('other_rounded_lon') == -122


# I'm not sure if there's an easy way to test that we're using the correct
# default protocol for each type, but at least we can check that nothing
# breaks.
def test_outputs_default_protocols(builder):
    expected_df = pd.DataFrame(
        columns=['x', 'y'],
        data=[
            [1, 2],
            [3, 4],
        ])

    @builder
    @bn.outputs('size', 'df')
    def f():
        df = expected_df.copy()
        return len(df), df

    flow = builder.build()

    assert flow.get('size') == 2
    pdt.assert_frame_equal(flow.get('df'), expected_df)


def test_wrong_number_of_outputs(builder):
    @builder
    @bn.outputs('a', 'b')
    def three_outputs():
        return (1, 2, 3)

    flow = builder.build()
    with pytest.raises(ValueError):
        flow.get('a')
