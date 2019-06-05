import bionic as bn


def test_gather_direct_parent_no_siblings(builder):
    builder.assign('x', values=[1, 2])

    @builder
    def x_plus_one(x):
        return x + 1

    @builder
    @bn.gather('x', 'x_plus_one', 'df')
    def sum_x_plus_one_over_all_x(df):
        return df['x_plus_one'].sum()

    assert builder.build().get('sum_x_plus_one_over_all_x') == 5


def test_gather_direct_parent_with_sibling(builder):
    builder.assign('x', values=[1, 2])
    builder.assign('y', values=[2, 3])

    @builder
    def x_plus_y(x, y):
        return x + y

    @builder
    @bn.gather('x', 'x_plus_y', 'df')
    def sum_x_plus_y_over_all_x(df):
        return df['x_plus_y'].sum()

    flow = builder.build()
    assert flow.get('sum_x_plus_y_over_all_x', set) == {7, 9}
    assert (
        list(flow.get('sum_x_plus_y_over_all_x', 'series').sort_values()) ==
        [7, 9])


def test_gather_ancestor_with_sibling(builder):
    builder.assign('x', values=[1, 2])
    builder.assign('y', values=[2, 3])

    @builder
    def x_plus_one(x):
        return x + 1

    @builder
    def x_plus_two(x_plus_one):
        return x_plus_one + 1

    @builder
    def x_plus_two_plus_y(x_plus_two, y):
        return x_plus_two + y

    @builder
    @bn.gather(
        'x_plus_one', 'x_plus_two_plus_y', 'df')
    def sum_x_plus_two_plus_y_over_all_x_plus_one(df):
        return df['x_plus_two_plus_y'].sum()

    values = builder.build()\
        .get('sum_x_plus_two_plus_y_over_all_x_plus_one', set)
    assert values == {11, 13}


def test_gather_ancestor_with_mixed_sibling(builder):
    builder.assign('x', values=[1, 2])
    builder.assign('y', values=[2, 3])
    builder.assign('z', values=[3, 4])

    @builder
    def x_plus_y(x, y):
        return x + y

    @builder
    def y_plus_z(y, z):
        return y + z

    @builder
    def x_plus_y_times_y_plus_z(x_plus_y, y_plus_z):
        return x_plus_y * y_plus_z

    @builder
    @bn.gather('x_plus_y', 'x_plus_y_times_y_plus_z', 'df')
    def sum_products_over_all_x_plus_y(df):
        return df['x_plus_y_times_y_plus_z'].sum()

    assert (
        builder.build().get('sum_products_over_all_x_plus_y', set) ==
        {89, 105})


def test_gather_non_ancestor(builder):
    builder.assign('x', values=[1, 2])
    builder.assign('y', values=[2, 3])

    @builder
    def x_plus_one(x):
        return x + 1

    @builder
    @bn.gather('y', 'x_plus_one', 'df')
    def sum_x_plus_one_over_all_y(df):
        return df['x_plus_one'].sum()

    assert builder.build().get('sum_x_plus_one_over_all_y', set) == {4, 6}

    @builder
    @bn.gather('y', 'x_plus_one', 'df')
    def y_times_sum_x_plus_one_over_all_y(y, df):
        return y * df['x_plus_one'].sum()

    assert (
        builder.build().get('y_times_sum_x_plus_one_over_all_y', set) ==
        {8, 12, 18})


def test_gather_self(builder):
    builder.assign('x', values=[1, 2])
    builder.assign('y', values=[2, 3])

    @builder
    def x_plus_y(x, y):
        return x + y

    @builder
    @bn.gather('x_plus_y', [], 'df')
    def sum_x_plus_ys(df):
        return df['x_plus_y'].sum()

    assert builder.build().get('sum_x_plus_ys') == 16


def test_gather_using_key_values(builder):
    builder.assign('x', values=[1, 2])

    @builder
    def x_plus_one(x):
        return x + 1

    @builder
    @bn.gather('x', 'x_plus_one', 'df')
    def sum_x_plus_one_times_x(df):
        return (df['x'] * df['x_plus_one']).sum()

    assert builder.build().get('sum_x_plus_one_times_x') == 8


def test_gather_multipart_key(builder):
    builder.declare('x')
    builder.declare('y')
    builder.add_case('x', 1, 'y', 2)
    builder.add_case('x', 2, 'y', 3)

    @builder
    def x_plus_y(x, y):
        return x + y

    @builder
    @bn.gather('x', 'x_plus_y', 'df')
    def sum_x_plus_y_over_all_x(df):
        return df['x_plus_y'].sum()

    assert builder.build().get('sum_x_plus_y_over_all_x') == 8


def test_gather_multiple_resources(builder):
    builder.assign('x', values=[1, 2])
    builder.assign('y', values=[2, 3])
    builder.assign('z', values=[3, 4])

    @builder
    def x_plus_y_plus_z(x, y, z):
        return x + y + z

    @builder
    @bn.gather(['x', 'y'], 'x_plus_y_plus_z', 'df')
    def sum_x_plus_y_plus_z_over_all_x_y(df):
        return df['x_plus_y_plus_z'].sum()

    assert (
        builder.build().get('sum_x_plus_y_plus_z_over_all_x_y', set) ==
        {28, 32})


def test_gather_multiple_dependent_resources(builder):
    builder.assign('x', values=[1, 2])
    builder.assign('y', values=[2, 3])
    builder.assign('z', values=[3, 4])

    @builder
    def xy(x, y):
        return x * y

    @builder
    def yz(y, z):
        return y * z

    @builder
    @bn.gather(['x', 'y'], ['xy', 'yz'], 'df')
    def prod_xy_plus_yz_over_all_x_y(df):
        return (df['xy'] + df['yz']).product()

    assert (
        builder.build().get('prod_xy_plus_yz_over_all_x_y', set) ==
        {
            (1*2+2*3)*(1*3+3*3)*(2*2+2*3)*(2*3+3*3),  # noqa: E226
            (1*2+2*4)*(1*3+3*4)*(2*2+2*4)*(2*3+3*4),  # noqa: E226
        })


def test_gather_no_name_specified(builder):
    builder.assign('x', values=[1, 2])

    @builder
    def x_plus_one(x):
        return x + 1

    @builder
    @bn.gather('x', 'x_plus_one')
    def sum_x_plus_one_over_all_x(gather_df):
        return gather_df['x_plus_one'].sum()

    assert builder.build().get('sum_x_plus_one_over_all_x') == 5


def test_multiple_gathers(builder):
    builder.assign('x', values=[1, 2])
    builder.assign('y', values=[2, 3])

    @builder
    @bn.gather('x', 'x', 'x_df')
    @bn.gather('y', 'y', 'y_df')
    def sum_xs_times_sum_ys(x_df, y_df):
        return x_df['x'].sum() * y_df['y'].sum()

    assert builder.build().get('sum_xs_times_sum_ys') == 15


def test_all_gathered_columns(builder):
    builder.assign('x', values=[1, 2])
    builder.assign('y', values=[2, 3])

    @builder
    def x_plus_y(x, y):
        return x + y

    @builder
    @bn.gather(['x', 'y'], 'x_plus_y', 'df')
    def zero(df):
        # TODO We have to rename the axis before we can sort because we're
        # reusing the names 'x' and 'y' in both the index and the columns,
        # which confuses recent versions of pandas.  This reuse is a bit
        # annoying -- it'd be nice if we could fix it.
        df = df.rename_axis(['a', 'b']).sort_values(['x', 'y'])
        assert list(df['x']) == [1, 1, 2, 2]
        assert list(df['y']) == [2, 3, 2, 3]
        assert list(df['x_plus_y']) == [3, 4, 4, 5]
        return 0

    assert builder.build().get('zero') == 0
