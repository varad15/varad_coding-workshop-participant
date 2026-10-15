"""
Retry with exponential backoff and jitter for transient PostgreSQL / S3 failures.

Implements the 'Database Connection Timeout' row of the workshop's error
handling table: retry with exponential backoff, log every attempt, and raise
after the final attempt so the job exits non-zero.
"""

from __future__ import annotations

import functools
import random
import time
from typing import Callable, ParamSpec, TypeVar

from common.logger import get_logger

P = ParamSpec("P")
R = TypeVar("R")

log = get_logger("retry")


def backoff_delays(attempts: int, base: float, cap: float) -> list[float]:
    """Return the sleep (seconds) before each retry: base * 2^n, capped."""
    return [min(cap, base * (2**n)) for n in range(max(attempts - 1, 0))]


def with_retry(
    exceptions: tuple[type[BaseException], ...],
    attempts: int = 5,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    sleep: Callable[[float], None] = time.sleep,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """
    Decorate a function so it is retried on the given exception types.

    Args:
        exceptions: Exception types considered transient.
        attempts: Total attempts including the first call.
        base_delay: First backoff delay in seconds.
        max_delay: Upper bound for a single delay.
        sleep: Injectable sleep function (tests pass a no-op).
    """

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            delays = backoff_delays(attempts, base_delay, max_delay)
            for attempt in range(1, attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as exc:
                    if attempt == attempts:
                        log.error(
                            "retry_exhausted",
                            extra={"operation": func.__qualname__, "attempt": attempt, "error": repr(exc)},
                        )
                        raise
                    delay = delays[attempt - 1] * random.uniform(0.8, 1.2)
                    log.warning(
                        "retrying",
                        extra={
                            "operation": func.__qualname__,
                            "attempt": attempt,
                            "next_delay_s": round(delay, 2),
                            "error": repr(exc),
                        },
                    )
                    sleep(delay)
            raise AssertionError("unreachable")  # pragma: no cover

        return wrapper

    return decorator
