from __future__ import print_function

import bionic as bn

builder = bn.FlowBuilder('hello_world')

builder.assign('greeting', 'Hello')
builder.assign('subject', 'world')


@builder
def message(greeting, subject):
    return '{0} {1}!'.format(greeting, subject)


flow = builder.build()

if __name__ == '__main__':
    print(flow.get('message'))
