import bionic as bn

bn.util.init_basic_logging()

builder = bn.FlowBuilder()

builder.assign('x', values=[2, 3])
builder.assign('y', values=[5, 7])


@builder
@bn.persist
def x_plus_y(x, y):
    return x + y


flow = builder.build()

for _, row in flow.get('x_plus_y', 'series').reset_index().iterrows():
    print '%s + %s = %s' % (row['x'], row['y'], row['x_plus_y'])
