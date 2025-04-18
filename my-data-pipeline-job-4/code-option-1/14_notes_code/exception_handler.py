import functools
import traceback
from typing import Callable, Any, TypeVar, cast

from src.utils.logger import get_logger

logger = get_logger(__name__)

# Define a generic type variable for the return type
T = TypeVar('T')


def handle_exceptions(func: Callable[..., T]) -> Callable[..., T]:
    """Decorator to handle exceptions in a consistent way."""
    
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> T:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            # Log the exception
            logger.error(f"Exception in {func.__name__}: {str(e)}")
            logger.debug(f"Traceback: {traceback.format_exc()}")
            
            # Re-raise the exception
            raise
    
    return cast(Callable[..., T], wrapper)


class ExceptionHandler:
    """Class to handle exceptions in a consistent way."""
    
    @staticmethod
    def handle(func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """Handle exceptions for the given function."""
        try:
            return func(*args, **kwargs)
        except Exception as e:
            # Log the exception
            logger.error(f"Exception in {func.__name__}: {str(e)}")
            logger.debug(f"Traceback: {traceback.format_exc()}")
            
            # Re-raise the exception
            raise
    
    @staticmethod
    def handle_with_fallback(func: Callable[..., T], fallback: T, *args: Any, **kwargs: Any) -> T:
        """Handle exceptions for the given function and return a fallback value if an exception occurs."""
        try:
            return func(*args, **kwargs)
        except Exception as e:
            # Log the exception
            logger.error(f"Exception in {func.__name__}: {str(e)}")
            logger.debug(f"Traceback: {traceback.format_exc()}")
            
            # Return the fallback value
            return fallback