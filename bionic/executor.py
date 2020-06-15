"""
Contains an Executor class which wraps the Loky parallel processing
executor in a way that allows logging to work seamlessly. Also contains
Logging classes used by the Executor to make logging work in a parallel
setting.
"""

import logging
import queue
import sys
import threading
import traceback

from multiprocessing.managers import SyncManager

from .optdep import import_optional_dependency
from .util import oneline, SynchronizedSet


class Executor:
    """
    Encapsulates all objects related to parallel processing in one place.
    It wraps the Loky parallel processing executor in a way that allows
    logging to work seamlessly.
    """

    def __init__(self, worker_count):
        self.worker_count = worker_count
        self._manager = get_singleton_manager()
        self._process_pool_exec = None
        self._init_or_resize_process_pool()

    def _init_or_resize_process_pool(self):
        """
        Initializes the process pool if not initialized yet. If  already initialized,
        since the process pool is a singleton in Loky, it attempts to register it again
        which resizes the executor if the worker count changed.
        """

        # Loky uses cloudpickle by default. We should investigate further what would
        # it take to make is use pickle and wrap the functions that are non-picklable
        # using `loky.wrap_non_picklable_objects`.
        loky = import_optional_dependency("loky", purpose="parallel processing")

        # This call to resize the executor is cheap when worker count doesn't change.
        # Loky simply compares the arguments sent before and now. Since they match when
        # worker count doesn't change, it returns the underlying executor without doing
        # any kind of resizing (which might take time if the executor is running any tasks).
        self._process_pool_exec = loky.get_reusable_executor(
            max_workers=self.worker_count,
            initializer=logging_initializer,
            initargs=(self._manager.logging_queue,),
        )

    def flush_logs(self):
        self._manager.flush_logs()

    def submit(self, fn, *args, **kwargs):
        self._init_or_resize_process_pool()
        return self._process_pool_exec.submit(fn, *args, **kwargs)

    def create_synchronized_set(self):
        return self._manager.create_synchronized_set()


_manager = None


def get_singleton_manager():
    global _manager
    if _manager is None:
        _manager = ExternalProcessLoggingManager()
    return _manager


class ProcessManager(SyncManager):
    pass


ProcessManager.register("SynchronizedSet", SynchronizedSet)


class ExternalProcessLoggingManager:
    """
    Contains the process manager and logger used by the executor. This class
    should be a singleton as it spins up a new process for process manager
    and a new thread for receiving logs.
    """

    def __init__(self):
        self._process_manager = ProcessManager()
        self._process_manager.start()

        self.logging_queue = self._process_manager.Queue(-1)
        self._logging_receiver = LoggingReceiver(self.logging_queue)
        self._logging_receiver.start()

    def flush_logs(self):
        self._logging_receiver.flush()

    def create_synchronized_set(self):
        return self._process_manager.SynchronizedSet()


def logging_initializer(logging_queue):
    # NOTE when adding a new executor, revisit this logger logic since
    # logging might behave differently in a different process pool like
    # multiprocessing.ProcessPoolExecutor.
    logger = logging.getLogger()

    # https://stackoverflow.com/questions/58837076/logging-works-with-multiprocessing-but-not-with-loky
    # For loky, there should be no handlers since it uses fork + exec.
    # Let's remove any handlers that might have been added from previous
    # runs in this subprocess and add the new handler.
    warning = None
    if len(logger.handlers) > 1:
        warning = """
        Root logger in the subprocess seems to be modified and have
        multiple logging handlers. Make any logger changes outside
        Bionic functions for logging to work correctly when processing
        in parallel.
        """
    orig_handlers = logger.handlers
    for orig_handler in orig_handlers:
        logger.removeHandler(orig_handler)
    logger.addHandler(WorkerProcessLogHandler(logging_queue))

    # We want the subprocess to forward all messages to the queue and
    # let the receiver in main process take care of logging at the
    # appropriate logging level.
    logger.setLevel(logging.DEBUG)

    if warning is not None:
        logger.warn(oneline(warning))


class WorkerProcessLogHandler(logging.Handler):
    def __init__(self, queue):
        super(WorkerProcessLogHandler, self).__init__()
        self.queue = queue

    def emit(self, record):
        try:
            self.queue.put_nowait(record)
        except Exception:
            self.handleError(record)


class LoggingReceiver:
    def __init__(self, queue):
        self.queue = queue
        self._receive_thread = threading.Thread(target=self._receive)
        self._receive_thread.daemon = True
        self._queue_is_empty = threading.Event()

    def start(self):
        self._receive_thread.start()

    def flush(self):
        lock_acquired = self._queue_is_empty.wait(timeout=5.0)
        if not lock_acquired:
            logger = logging.getLogger()
            logger.warn("Logger flush timed out. There might be missing log messages.")

    def _receive(self):
        while True:
            if self.queue.empty():
                self._queue_is_empty.set()
            else:
                self._queue_is_empty.clear()

            try:
                record = self.queue.get(timeout=0.05)
            except queue.Empty:  # Nothing to receive from the queue.
                continue

            logger = logging.getLogger(record.name)
            try:
                if logger.isEnabledFor(record.levelno):
                    logger.handle(record)
            except (BrokenPipeError, EOFError):
                break
            except Exception as e:
                logger = logging.getLogger()
                try:
                    logger.warn("exception while logging ", e)
                except (BrokenPipeError, EOFError):
                    break
                except Exception:
                    traceback.print_exc(file=sys.stderr)
