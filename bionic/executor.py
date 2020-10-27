"""
Contains an Executor class which wraps the Loky process pool
executor in a way that allows logging to work seamlessly. Also
contains Logging classes used by the Executor to make logging
work in a parallel setting.
"""

import copy
from functools import partial
import logging
from multiprocessing.managers import SyncManager
import queue
import sys
import threading
import traceback
from uuid import uuid4

from .aip.task import Task
from .deps.optdep import import_optional_dependency
from .utils.misc import oneline, SynchronizedSet


class AipExecutor:
    """
    Encapsulates all objects related to remote execution on Google AIP
    in one place.
    """

    def __init__(self, aip_config, gcs_fs):
        self._aip_config = aip_config
        self._gcs_fs = gcs_fs

    def submit(self, task_config, fn, *args, **kwargs):
        return Task(
            # TODO: Use a better identifiable name, maybe a combination
            # of entity name and case key.
            # This is a temporary workaround. We are changing the random
            # UUID in such a manner because AIP names have to start with
            # a letter and only accepts alphanumeric and underscore
            # characters.
            name="a" + str(uuid4()).replace("-", ""),
            gcs_fs=self._gcs_fs,
            config=self._aip_config,
            task_config=task_config,
            function=partial(fn, *args, **kwargs),
        ).submit()


class ProcessExecutor:
    """
    Encapsulates all objects related to parallel execution in one place.
    It wraps the Loky process pool executor in a way that allows logging
    to work seamlessly.
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
        loky = import_optional_dependency("loky", purpose="parallel execution")

        # This call to resize the executor is cheap when worker count doesn't change.
        # Loky simply compares the arguments sent before and now. Since they match when
        # worker count doesn't change, it returns the underlying executor without doing
        # any kind of resizing (which might take time if the executor is running any tasks).
        self._process_pool_exec = loky.get_reusable_executor(
            max_workers=self.worker_count,
            initializer=logging_initializer,
            initargs=(self._manager.logging_queue,),
        )

    def submit(self, fn, *args, **kwargs):
        self._init_or_resize_process_pool()
        return self._process_pool_exec.submit(fn, *args, **kwargs)

    def create_synchronized_set(self):
        return self._manager.create_synchronized_set()

    def start_logging(self):
        self._manager.add_logging_listener()

    def stop_logging(self):
        self._manager.remove_logging_listener()


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

    def add_logging_listener(self):
        self._logging_receiver.add_listener()

    def remove_logging_listener(self):
        self._logging_receiver.flush_logs_and_remove_listener()

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
        Bionic functions for logging to work correctly when executing
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
        self._queue = queue

    def emit(self, record):
        # TODO We send all log records to the main process for logging,
        # even when logging is disabled for their log levels. This is
        # expensive as we pay IPC cost for the log records and pay the
        # cost of formatting the log record, which can be costly for
        # cases where users have a ton of disabled debug logs.
        # We should investigate if we can either cache the log levels
        # for each logger or communicate with main process to get the
        # log level before we decide to send the logs.
        try:
            # By adding the LogRecord to the queue, we send it to the
            # process spawned by the multiprocessing manager. The
            # communication between processes requires every argument
            # to be pickleable using cloudpickle. Users can log
            # unpickleable objects like clients or handlers and expect
            # the logger to work. To avoid breaking logging, we eagerly
            # format the log message the way logging does
            # (check LogRecord.getMessage). This has a side effect of
            # changing the template and arguments for the log in case
            # users intercepts the LogRecord in their custom handlers.
            record = copy.copy(record)
            record.msg = record.getMessage()
            record.args = ()
            self._queue.put_nowait(record)
        except Exception:
            self.handleError(record)


class LoggingReceiver:
    def __init__(self, queue):
        self._queue = queue
        self._event_queue_is_empty = threading.Event()

        self._listener_count = 0
        self._listener_count_lock = threading.Lock()
        self._event_has_listeners = threading.Event()

        self._receive_thread = threading.Thread(
            target=self._receive,
            name="Log-Receiver",
            daemon=True,
        )
        self._receive_thread.start()

    def add_listener(self):
        self._add_to_listener_count(1)

    def flush_logs_and_remove_listener(self):
        lock_acquired = self._event_queue_is_empty.wait(timeout=5.0)
        if not lock_acquired:
            logger = logging.getLogger()
            logger.warn("Logger flush timed out. There might be missing log messages.")

        self._add_to_listener_count(-1)

    def _add_to_listener_count(self, delta):
        with self._listener_count_lock:
            self._listener_count += delta

            if self._listener_count > 0:
                self._event_has_listeners.set()
            else:
                self._event_has_listeners.clear()

    def _receive(self):
        while True:
            # If we don't have any listeners, we don't want to be checking
            # the queue. In particular, since this is a daemon thread, it
            # will keep running up until the process exits, and at that time
            # the queue object can become unreliable because it's managed by
            # a SyncManager that depends on a separate process. This listener
            # check makes sure we're only using the queue during an actual
            # Flow.get call.
            # See here for what happens if we don't have this check:
            # https://github.com/square/bionic/issues/161
            self._event_has_listeners.wait()

            if self._queue.empty():
                self._event_queue_is_empty.set()
            else:
                self._event_queue_is_empty.clear()

            try:
                record = self._queue.get(timeout=0.05)
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
