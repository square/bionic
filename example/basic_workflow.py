import bionic as bn

builder = bn.FlowBuilder('basic_workflow')

builder.assign('x', values=[2, 3])
builder.assign('y', values=[5, 7])


@builder
def x_plus_y(x, y):
    return x + y


flow = builder.build()

if __name__ == '__main__':
    bn.util.init_basic_logging()

    for _, row in flow.get('x_plus_y', 'series').reset_index().iterrows():
        print(f"{row['x']} + {row['y']} = {row['x_plus_y']}")
