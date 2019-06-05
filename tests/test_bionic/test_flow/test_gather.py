import pytest

import bionic as bn


@pytest.fixture(scope='function')
def preset_builder(builder):
    builder.assign('w', values=['w', 'W'])
    builder.assign('x', values=['x', 'X'])
    builder.assign('y', values=['y', 'Y'])
    builder.assign('z', values=['z', 'Z'])

    @builder
    def wx(w, x):
        return w + x

    @builder
    def xy(x, y):
        return x + y

    @builder
    def yz(y, z):
        return y + z

    return builder


def summarize(df):
    return ' '.join(
        '.'.join(list(row))
        for _, row in df.sort_values(list(df.columns)).iterrows())


def test_x(preset_builder):
    builder = preset_builder

    @builder
    @bn.gather('x')
    def summary(gather_df):
        return summarize(gather_df)

    assert builder.build().get('summary', set) == {
        'X x',
    }


def test_x__y(preset_builder):
    builder = preset_builder

    @builder
    @bn.gather('x', 'y')
    def summary(gather_df):
        return summarize(gather_df)

    assert builder.build().get('summary', set) == {
        'X.Y x.Y',
        'X.y x.y',
    }


def test_x__xy(preset_builder):
    builder = preset_builder

    @builder
    @bn.gather('x', 'xy')
    def summary(gather_df):
        return summarize(gather_df)

    assert builder.build().get('summary', set) == {
        'X.XY x.xY',
        'X.Xy x.xy',
    }


def test_xy__yz(preset_builder):
    builder = preset_builder

    @builder
    @bn.gather('xy', 'yz')
    def summary(gather_df):
        return summarize(gather_df)

    assert builder.build().get('summary', set) == {
        'XY.YZ Xy.yZ xY.YZ xy.yZ',
        'XY.Yz Xy.yz xY.Yz xy.yz',
    }


def test_x_y__z(preset_builder):
    builder = preset_builder

    @builder
    @bn.gather(['x', 'y'], 'z')
    def summary(gather_df):
        return summarize(gather_df)

    assert builder.build().get('summary', set) == {
        'X.Y.Z X.y.Z x.Y.Z x.y.Z',
        'X.Y.z X.y.z x.Y.z x.y.z',
    }


def test_wx_xy__yz(preset_builder):
    builder = preset_builder

    @builder
    @bn.gather(['wx', 'xy'], 'yz')
    def summary(gather_df):
        return summarize(gather_df)

    assert builder.build().get('summary', set) == {
        'WX.XY.YZ WX.Xy.yZ Wx.xY.YZ Wx.xy.yZ wX.XY.YZ wX.Xy.yZ wx.xY.YZ wx.xy.yZ',
        'WX.XY.Yz WX.Xy.yz Wx.xY.Yz Wx.xy.yz wX.XY.Yz wX.Xy.yz wx.xY.Yz wx.xy.yz',
    }


def test_wx__xy_yz(preset_builder):
    builder = preset_builder

    @builder
    @bn.gather('wx', ['xy', 'yz'])
    def summary(gather_df):
        return summarize(gather_df)

    assert builder.build().get('summary', set) == {
        'WX.XY.YZ Wx.xY.YZ wX.XY.YZ wx.xY.YZ',
        'WX.XY.Yz Wx.xY.Yz wX.XY.Yz wx.xY.Yz',
        'WX.Xy.yZ Wx.xy.yZ wX.Xy.yZ wx.xy.yZ',
        'WX.Xy.yz Wx.xy.yz wX.Xy.yz wx.xy.yz',
    }


def test_wx__xy__z(preset_builder):
    builder = preset_builder

    @builder
    @bn.gather('wx', 'xy')
    def summary(gather_df, z):
        return summarize(gather_df) + ' -- ' + z

    assert builder.build().get('summary', set) == {
        'WX.XY Wx.xY wX.XY wx.xY -- Z',
        'WX.Xy Wx.xy wX.Xy wx.xy -- Z',
        'WX.XY Wx.xY wX.XY wx.xY -- z',
        'WX.Xy Wx.xy wX.Xy wx.xy -- z',
    }


def test_wx__xy__yz(preset_builder):
    builder = preset_builder

    @builder
    @bn.gather('wx', 'xy')
    def summary(gather_df, yz):
        return summarize(gather_df) + ' -- ' + yz

    assert builder.build().get('summary', set) == {
        'WX.XY Wx.xY wX.XY wx.xY -- YZ',
        'WX.Xy Wx.xy wX.Xy wx.xy -- yZ',
        'WX.XY Wx.xY wX.XY wx.xY -- Yz',
        'WX.Xy Wx.xy wX.Xy wx.xy -- yz',
    }


def test_xy__yz__wx(preset_builder):
    builder = preset_builder

    @builder
    @bn.gather('xy', 'yz')
    def summary(gather_df, wx):
        return summarize(gather_df) + ' -- ' + wx

    assert builder.build().get('summary', set) == {
        'XY.YZ Xy.yZ xY.YZ xy.yZ -- WX',
        'XY.YZ Xy.yZ xY.YZ xy.yZ -- Wx',
        'XY.YZ Xy.yZ xY.YZ xy.yZ -- wX',
        'XY.YZ Xy.yZ xY.YZ xy.yZ -- wx',
        'XY.Yz Xy.yz xY.Yz xy.yz -- WX',
        'XY.Yz Xy.yz xY.Yz xy.yz -- Wx',
        'XY.Yz Xy.yz xY.Yz xy.yz -- wX',
        'XY.Yz Xy.yz xY.Yz xy.yz -- wx',
    }


def test_rename_frame(preset_builder):
    builder = preset_builder

    @builder
    @bn.gather('xy', 'yz', 'my_df')
    def summary(my_df):
        return summarize(my_df)

    assert builder.build().get('summary', set) == {
        'XY.YZ Xy.yZ xY.YZ xy.yZ',
        'XY.Yz Xy.yz xY.Yz xy.yz',
    }


def test_multiple_gathers(preset_builder):
    builder = preset_builder

    @builder
    @bn.gather('w', 'wx', 'df1')
    @bn.gather('y', 'yz', 'df2')
    def summary(df1, df2):
        return summarize(df1) + ' -- ' + summarize(df2)

    assert builder.build().get('summary', set) == {
        'W.WX w.wX -- Y.YZ y.yZ',
        'W.WX w.wX -- Y.Yz y.yz',
        'W.Wx w.wx -- Y.YZ y.yZ',
        'W.Wx w.wx -- Y.Yz y.yz',
    }


def test_multiple_gathers_complex(preset_builder):
    builder = preset_builder

    @builder
    @bn.gather('w', 'wx', 'df1')
    @bn.gather('x', 'wx', 'df2')
    def summary(df1, df2):
        return summarize(df1) + ' -- ' + summarize(df2)

    assert builder.build().get('summary', set) == {
        'W.WX w.wX -- X.WX x.Wx',
        'W.WX w.wX -- X.wX x.wx',
        'W.Wx w.wx -- X.WX x.Wx',
        'W.Wx w.wx -- X.wX x.wx',
    }
