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
import warnings

from multiprocessing.managers import SyncManager

from .optdep import import_optional_dependency
from .util import oneline, SynchronizedSet

_executor = None


def get_reusable_executor(worker_count):
    global _executor

    if _executor is None:
        _executor = Executor(worker_count)
    else:
        prev_worker_count = _executor.worker_count
        process_pool_resized = _executor.init_or_resize_process_pool(worker_count)
        if process_pool_resized:
            warning = f"""
            The number of workers in the global Loky process pool has
            changed from {_format_worker_count(prev_worker_count)} to
            {_format_worker_count(worker_count)}. Because this pool is
            global, all parallel jobs will now run with the new number
            of workers, even in Bionic flows that were configured with
            the old number.
            """
            warnings.warn(oneline(warning))

    return _executor


def _format_worker_count(worker_count):
    return worker_count if worker_count is not None else "the default value"


class Executor:
    """
    Encapsulates all objects related to parallel processing in one place.
    It wraps the Loky parallel processing executor in a way that allows
    logging to work seamlessly.
    """

    def __init__(self, worker_count):
        self.worker_count = worker_count

        class MyManager(SyncManager):
            pass

        MyManager.register("SynchronizedSet", SynchronizedSet)
        self.process_manager = MyManager()
        self.process_manager.start()

        self._logging_queue = self.process_manager.Queue(-1)
        self._logging_receiver = LoggingReceiver(self._logging_queue)
        self._logging_receiver.start()

        self._process_pool_exec = None
        self.init_or_resize_process_pool(worker_count)

    def init_or_resize_process_pool(self, worker_count):
        """
        If the process pool is not initialized yet, initializes it and
        returns True.

        If already initialized, resizes the process pool if the
        worker_count is different than the worker count used previously
        and returns whether the process pool was resized.
        """

        # When already initialized and worker count didn't change,
        # no need to resize the process pool.
        if self._process_pool_exec is not None and worker_count == self.worker_count:
            return False

        self.worker_count = worker_count

        # Loky uses cloudpickle by default. We should investigate further what would
        # it take to make is use pickle and wrap the functions that are non-picklable
        # using `loky.wrap_non_picklable_objects`.
        loky = import_optional_dependency("loky", purpose="parallel processing")

        self._process_pool_exec = loky.get_reusable_executor(
            max_workers=worker_count,
            initializer=logging_initializer,
            initargs=(self._logging_queue,),
        )
        return True

    def flush_logs(self):
        self._logging_receiver.flush()

    def submit(self, fn, *args, **kwargs):
        return self._process_pool_exec.submit(fn, *args, **kwargs)

    def create_synchronized_set(self):
        return self.process_manager.SynchronizedSet()


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
