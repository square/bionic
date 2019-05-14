'''
Contains "handle" interfaces which provide either mutable or immutable access
to shared objects.  Handles can be switched between mutable and immutable
access types, which allows implementations of builders while minimizing the
number of times the underlying data needs to be copied.
'''


class ImmutableHandle(object):
    '''
    Provides access to an immutable object.  The advantage of this handle over
    managing the object directly is that it can also generate mutable handles
    while minimizing the number of copy operations.

    The wrapped object must provide a copy() method such that `x.copy() == x`.
    The handle's get() method will always return a value equal to the original
    value.
    '''
    def __init__(self, immutable_value=None, value_generator=None):
        assert (immutable_value is None) != (value_generator is None)

        self._immutable_value = immutable_value
        self._generate_value = value_generator
        self._private_cached_value = None

    def get(self):
        if self._immutable_value is not None:
            pass

        elif self._private_cached_value is not None:
            self._immutable_value = self._private_cached_value
            self._private_cached_value = None
            self._generate_value = None

        else:
            self._immutable_value = self._generate_value()
            self._generate_value = None

        return self._immutable_value

    def as_mutable(self):
        '''
        Returns a mutable version of this handle.
        '''
        if self._private_cached_value is not None:
            value = self._private_cached_value
            self._private_cached_value = None
            return MutableHandle(mutable_value=value)
        elif self._immutable_value is not None:
            return MutableHandle(immutable_value=self._immutable_value)
        else:
            return MutableHandle(mutable_value=self._generate_value())

    def updating(self, update_func, eager=False):
        '''
        Returns a new handle whose value is the same as this one but with
        update_func applied to it.

        If eager is true, update_func is computed immediately; otherwise, it
        is computed just-in-time when the returned handle is dereferenced.
        '''
        def generate_value():
            value = self.as_mutable().get()
            update_func(value)
            return value
        updated_handler = ImmutableHandle(value_generator=generate_value)
        if eager:
            updated_handler._private_cached_value = generate_value()
        return updated_handler

    def __repr__(self):
        if self._immutable_value is not None:
            details = 'fixed=%r' % self._immutable_value
        elif self._private_cached_value is not None:
            details = 'private=%r, generator=...' % self._private_cached_value
        else:
            details = 'generator=...'
        return 'ImmutableHandle(%s)' % details


class MutableHandle(object):
    '''
    Provides access to a mutable object.  The advantage of this handle over
    managing the object directly is that it can generate immutable handles
    while minimizing the number of copy operations.

    The wrapped object must provide a copy() method such that `x.copy() == x`.
    The object returned by get() can be mutated, but only up until the next
    call to as_immutable(); any changes made before then will be reflected in
    the next call to get().  Before making any more changes, get() should be
    called again to make sure you have the most recent instance.
    '''
    def __init__(self, mutable_value=None, immutable_value=None):
        if mutable_value is not None:
            assert immutable_value is None
            self._value = mutable_value
            self._value_is_mutable = True
        else:
            assert immutable_value is not None
            self._value = immutable_value
            self._value_is_mutable = False

    def get(self):
        if not self._value_is_mutable:
            self._value = self._value.copy()
            self._value_is_mutable = True
        return self._value

    def as_immutable(self):
        '''
        Returns an immutable version of this handle.
        '''
        self._value_is_mutable = False
        return ImmutableHandle(immutable_value=self._value)
