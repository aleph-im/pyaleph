import time


class Timer:
    """
    A context manager to measure the time of any operation.

    Usage:
    >>> with Timer() as timer:
    >>>     do_something()
    >>> print(f"Did something in {timer.elapsed()} seconds.")
    """

    def __enter__(self):
        self.start_time = time.time()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end_time = time.time()

    def elapsed(self) -> float:
        return self.end_time - self.start_time
