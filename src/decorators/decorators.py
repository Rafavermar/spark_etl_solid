import functools
import logging
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def log_decorator(func):
    """Decorator to log function calls."""

    @functools.wraps(func)
    def wrapper_decorator(*args, **kwargs):
        logging.info(f"Starting {func.__name__}...")
        result = func(*args, **kwargs)
        logging.info(f"Completed {func.__name__} successfully.")
        return result

    return wrapper_decorator


def timing_decorator(func):
    """Decorator to measure the execution time of functions."""

    @functools.wraps(func)
    def wrapper_decorator(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        logging.info(f"{func.__name__} executed in {end_time - start_time:.2f} seconds")
        return result

    return wrapper_decorator