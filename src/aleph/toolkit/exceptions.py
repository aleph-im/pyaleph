from contextlib import contextmanager
from typing import Callable, Optional, Type


@contextmanager
def ignore_exceptions(
    *exceptions: Type[BaseException],
    on_error: Optional[Callable[[BaseException], None]] = None
):
    try:
        yield
    except exceptions as e:
        if on_error:
            on_error(e)
        pass
