import time
from typing import Optional, Dict, Any
from contextlib import contextmanager

from src.utils.logger import get_logger

logger = get_logger(__name__)


class Timer:
    """Utility class for timing operations."""
    
    def __init__(self, name: Optional[str] = None):
        self.name = name
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None
        self.elapsed_time: Optional[float] = None
    
    def start(self) -> None:
        """Start the timer."""
        self.start_time = time.time()
        self.end_time = None
        self.elapsed_time = None
        
        if self.name:
            logger.debug(f"Timer '{self.name}' started")
    
    def stop(self) -> float:
        """Stop the timer and return the elapsed time in seconds."""
        if self.start_time is None:
            raise ValueError("Timer has not been started")
        
        self.end_time = time.time()
        self.elapsed_time = self.end_time - self.start_time
        
        if self.name:
            logger.debug(f"Timer '{self.name}' stopped. Elapsed time: {self.elapsed_time:.6f} seconds")
        
        return self.elapsed_time
    
    def reset(self) -> None:
        """Reset the timer."""
        self.start_time = None
        self.end_time = None
        self.elapsed_time = None
        
        if self.name:
            logger.debug(f"Timer '{self.name}' reset")
    
    @contextmanager
    def time_it(self):
        """Context manager for timing a block of code."""
        self.start()
        try:
            yield
        finally:
            self.stop()


class TimerRegistry:
    """Registry for managing multiple timers."""
    
    def __init__(self):
        self.timers: Dict[str, Timer] = {}
    
    def get_timer(self, name: str) -> Timer:
        """Get a timer with the specified name, creating it if it doesn't exist."""
        if name not in self.timers:
            self.timers[name] = Timer(name)
        
        return self.timers[name]
    
    def start_timer(self, name: str) -> None:
        """Start a timer with the specified name."""
        timer = self.get_timer(name)
        timer.start()
    
    def stop_timer(self, name: str) -> float:
        """Stop a timer with the specified name and return the elapsed time."""
        if name not in self.timers:
            raise ValueError(f"Timer '{name}' does not exist")
        
        timer = self.timers[name]
        return timer.stop()
    
    def get_elapsed_time(self, name: str) -> Optional[float]:
        """Get the elapsed time for a timer with the specified name."""
        if name not in self.timers:
            return None
        
        timer = self.timers[name]
        return timer.elapsed_time
    
    def get_all_elapsed_times(self) -> Dict[str, Optional[float]]:
        """Get the elapsed times for all timers."""
        return {name: timer.elapsed_time for name, timer in self.timers.items()}
    
    def reset_all(self) -> None:
        """Reset all timers."""
        for timer in self.timers.values():
            timer.reset()