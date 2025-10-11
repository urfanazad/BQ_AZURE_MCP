import functools
import logging

def handle_errors(func):
    """
    A decorator to catch and log exceptions in MCP handlers.
    This prevents the server from crashing due to unexpected errors.
    """
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except ConnectionError as e:
            logging.error(f"Connection error in {func.__name__}: {e}")
            # In a real application, you might want to return a specific error message
            # to the client, but for now, we'll just log it.
            raise
        except ValueError as e:
            logging.error(f"Value error in {func.__name__}: {e}")
            raise
        except Exception as e:
            logging.error(f"An unexpected error occurred in {func.__name__}: {e}", exc_info=True)
            raise
    return wrapper