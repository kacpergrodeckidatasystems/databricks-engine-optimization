import time
import logging
import functools
from typing import Any, Callable

logger = logging.getLogger("APM.Tracer")


def trace_execution(func: Callable) -> Callable:
    """Decorator tracking entry, exit, and operation duration (Telemetry Tracer)."""

    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        start_time = time.time()
        logger.debug(f"[TRACE] Starting method: {func.__module__}.{func.__name__}")
        try:
            result = func(*args, **kwargs)
            duration = time.time() - start_time
            logger.info(f"[TRACE] Completed {func.__name__} in {duration:.4f}s successfully.")
            return result
        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"[TRACE] Failure in {func.__name__} after {duration:.4f}s.")
            raise e

    return wrapper


def safe_execution(default_factory: Callable[[], Any]) -> Callable:
    """Decorator isolating errors (Fault Barrier). Ensures pipeline continuity."""

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logger.critical(
                    f"[BARRIER] Exception in {func.__name__}: {str(e)}. "
                    f"Returning safe default object.",
                    exc_info=True,
                )
                return default_factory()

        return wrapper

    return decorator
