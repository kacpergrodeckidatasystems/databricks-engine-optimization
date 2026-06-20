import time
import logging
import functools
from typing import Any, Callable

logger = logging.getLogger("APM.Tracer")

def trace_execution(func: Callable) -> Callable:
    """Dekorator śledzący wejście, wyjście oraz czas trwania operacji (Telemetry Tracer)."""
    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        start_time = time.time()
        logger.debug(f"[TRACE] Uruchamianie metody: {func.__module__}.{func.__name__}")
        try:
            result = func(*args, **kwargs)
            duration = time.time() - start_time
            logger.info(f"[TRACE] Zakończono {func.__name__} w {duration:.4f}s z sukcesem.")
            return result
        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"[TRACE] Awaria w {func.__name__} po {duration:.4f}s.")
            raise e
    return wrapper

def safe_execution(default_factory: Callable[[], Any]) -> Callable:
    """Dekorator izolujący błędy (Fault Barrier). Zapewnia ciągłość działania rurociągu."""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logger.critical(
                    f"[BARRIER] Wyjątek w {func.__name__}: {str(e)}. "
                    f"Zwracanie bezpiecznego obiektu domyślnego.", 
                    exc_info=True
                )
                return default_factory()
        return wrapper
    return decorator