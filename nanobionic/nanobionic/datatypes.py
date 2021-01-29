import attr


@attr.s
class Rule:
    dep_descs = attr.ib()
    func = attr.ib()
    present_as_function = attr.ib(default=False)
    present_as_future = attr.ib(default=False)
    print_on_compute = attr.ib(default=False)
    duration = attr.ib(default=0)

    def evolve(self, **kwargs):
        return attr.evolve(self, **kwargs)

    
@attr.s
class Artifact:
    value = attr.ib()
    
    @property
    def digest(self):
        return repr(self.value)

    
@attr.s(frozen=True)
class Provenance:
    desc = attr.ib()
    dep_digests = attr.ib()
    
    @property
    def digest(self):
        return f"{self.desc}({', '.join(self.dep_digests)})"


"""
FIXME continue here

Okay, I think I see how to proceed. We need to have a shared "clock" object,
which can be updated by functions when they do "work". (They will notify the
clock what work they're doing and how long it takes). The clock is created when
we start a computation. Each future knows about its clock, and manipulates its state
before doing any computation.
"""


@attr.s
class Clock:
    cur_time = attr.ib(default=0)

    def log_event(self, name, duration):
        print FIXME


@attr.s
class Supplier:
    get = attr.ib()
    duration = attr.ib(default=0)

    def __call__(self):
        return self.get()


# FIXME How does time management fit into this?
@attr.s
class Future:
    clock = attr.ib()
    func = attr.ib()
    dep_futures = attr.ib(factory=list)
    _value = attr.ib(default=None)
    _has_value = attr.ib(default=False)

    def map(self, func):
        return Future(
            clock=self.clock,
            func=func,
            dep_futures=[self],
        )
    
    def join(self, clock, futures):
        assert all(future.clock == clock for future in futures)
        return Future(
            clock=clock,
            func=lambda values: values,
            dep_futures=futures,
        )
    
    def get(self):
        if not self._has_value:
            # FIXME manipulate the clock around each recursive call
            dep_values = [
                dep_future.get() for dep_future in self.dep_futures
            ]
            # FIXME manipulate the clock around this call
            self._value = self.func(dep_values)
            self._has_value = True
        return self._value
    
        





@attr.s
class Future:
    value = attr.ib()
    duration = attr.ib(default=0)
    
    def get(self):
        return self.value

    def map(self, func, duration=0):
        return Future(func(self.value), duration=self.duration + duration)
    
    @classmethod
    def join(self, futures):
        return Future(
            value=tuple(future.value for future in futures),
            duration=max(future.duration for duration in durations),
        )
    
    @classmethod
    def from_supplier(self, supplier):
        return Future(
            value=supplier.get(),
            duration=supplier.duration,
        )