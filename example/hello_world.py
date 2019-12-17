import bionic as bn

# Initialize the builder object we'll use to construct our flow.
builder = bn.FlowBuilder('hello_world')

# Define new entities "greeting" and "subject" with fixed values.
builder.assign('greeting', 'Hello')
builder.assign('subject', 'world')


# Define a "message" entity, constructed by taking the values of "greeting" and
# "subject" and combining them in a sentence.
# The `@builder` decorator tells Bionic to define a new derived entity; Bionic
# infers the name of the new entity ("message") and the names of its
# dependencies ("greeting" and "subject").
@builder
def message(greeting, subject):
    return f'{greeting} {subject}!'


# Assemble the flow object, which is capable of computing any of the entities
# we've defined.
flow = builder.build()

if __name__ == '__main__':
    # Use our flow to compute the message "Hello world!"
    print(flow.get('message'))
