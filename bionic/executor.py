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

from .optdep import import_optional_dependency
from .util import oneline

_executor = None


def get_reusable_executor(process_manager):
    global _executor

    # TODO: For now, executor is always the same. But once the worker
    # count is configurable, we should try and update the loky executor
    # and log a warn if the executor changed.
    if _executor is None:
        _executor = Executor(process_manager)

    return _executor


class Executor:
    """
    Encapsulates all objects related to parallel processing sans manager
    in one place. It wraps the Loky parallel processing executor in a way
    that allows logging to work seamlessly.
    """

    # TODO move process manager into the executor class.
    def __init__(self, process_manager):
        self._logging_queue = process_manager.Queue(-1)
        self._logging_receiver = LoggingReceiver(self._logging_queue)
        self._logging_receiver.start()

        # Loky uses cloudpickle by default. We should investigate further what would
        # it take to make is use pickle and wrap the functions that are non-picklable
        # using `loky.wrap_non_picklable_objects`.
        loky = import_optional_dependency("loky", purpose="parallel processing")

        # TODO: Use a config / cpu cores instead of using a hardcoded value.
        # Same applies to the test executor.
        self._process_pool_exec = loky.get_reusable_executor(
            max_workers=2,
            initializer=logging_initializer,
            initargs=(self._logging_queue,),
        )

    def flush_logs(self):
        self._logging_receiver.flush()

    def submit(self, fn, *args, **kwargs):
        return self._process_pool_exec.submit(fn, *args, **kwargs)


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
