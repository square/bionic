"""
Contains the WorkerProcessLogHandler and LoggingReceiver classes, that
handles logging for multiprocess execution.
"""

import logging
import queue
import sys
import threading
import traceback


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
        self._is_closed = False
        self._queue_empty_lock = None

    def start(self):
        self._receive_thread.start()

    def stop(self):
        if not self._is_closed:
            self._is_closed = True
            self.flush()

    def flush(self):
        self._queue_empty_lock = threading.Lock()
        lock_acquired = self._queue_empty_lock.acquire(blocking=True, timeout=5.0)
        if not lock_acquired:
            logger = logging.getLogger()
            logger.warn("Logger flush timed out. There might be missing log messages.")
        self._queue_empty_lock = None

    def _receive(self):
        while True:
            if self._is_closed and self.queue.empty():
                break

            try:
                record = self.queue.get(timeout=0.05)
                logger = logging.getLogger(record.name)
                if logger.isEnabledFor(record.levelno):
                    logger.handle(record)
            except (BrokenPipeError, EOFError):
                break
            except queue.Empty:  # Nothing to receive from the queue.
                if self._queue_empty_lock is not None:
                    self._queue_empty_lock.release()
            except Exception as e:
                logger = logging.getLogger()
                try:
                    logger.warn("exception while logging ", e)
                except (BrokenPipeError, EOFError):
                    break
                except Exception:
                    traceback.print_exc(file=sys.stderr)
