from enum import Enum, auto


class AipError(Exception):
    pass


class State(Enum):
    STATE_UNSPECIFIED = auto()
    QUEUED = auto()
    PREPARING = auto()
    RUNNING = auto()
    SUCCEEDED = auto()
    FAILED = auto()
    CANCELLING = auto()
    CANCELLED = auto()

    def is_executing(self):
        return self in {
            State.STATE_UNSPECIFIED,
            State.QUEUED,
            State.PREPARING,
            State.RUNNING,
        }

    def is_cancelled(self):
        return self in {State.CANCELLING, State.CANCELLED}

    def is_finished(self):
        return self in {
            State.SUCCEEDED,
            State.FAILED,
            State.CANCELLING,
            State.CANCELLED,
        }
