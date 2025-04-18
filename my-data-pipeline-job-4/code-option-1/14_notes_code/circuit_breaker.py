import time
from enum import Enum
from typing import Callable, Any, TypeVar, Optional, List
import threading

from src.utils.logger import get_logger

logger = get_logger(__name__)

# Define a generic type variable for the return type
T = TypeVar('T')


class CircuitState(Enum):
    """Enum representing the state of a circuit breaker."""
    CLOSED = 1  # Normal operation, requests are allowed
    OPEN = 2    # Circuit is open, requests are not allowed
    HALF_OPEN = 3  # Testing if the circuit can be closed again


class CircuitBreaker:
    """Implementation of the Circuit Breaker pattern."""
    
    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: int = 60,
        name: Optional[str] = None
    ):
        self.name = name or "circuit_breaker"
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.last_failure_time = 0
        self.lock = threading.RLock()
    
    def execute(self, func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """Execute the function with circuit breaker protection."""
        with self.lock:
            if self.state == CircuitState.OPEN:
                # Check if recovery timeout has elapsed
                if time.time() - self.last_failure_time >= self.recovery_timeout:
                    logger.info(f"Circuit breaker '{self.name}' transitioning from OPEN to HALF_OPEN")
                    self.state = CircuitState.HALF_OPEN
                else:
                    logger.warning(f"Circuit breaker '{self.name}' is OPEN, request rejected")
                    raise CircuitBreakerOpenError(f"Circuit breaker '{self.name}' is open")
        
        try:
            result = func(*args, **kwargs)
            
            with self.lock:
                if self.state == CircuitState.HALF_OPEN:
                    logger.info(f"Circuit breaker '{self.name}' transitioning from HALF_OPEN to CLOSED")
                    self.state = CircuitState.CLOSED
                    self.failure_count = 0
            
            return result
        
        except Exception as e:
            with self.lock:
                self.failure_count += 1
                self.last_failure_time = time.time()
                
                if self.state == CircuitState.CLOSED and self.failure_count >= self.failure_threshold:
                    logger.warning(f"Circuit breaker '{self.name}' transitioning from CLOSED to OPEN")
                    self.state = CircuitState.OPEN
                
                if self.state == CircuitState.HALF_OPEN:
                    logger.warning(f"Circuit breaker '{self.name}' transitioning from HALF_OPEN to OPEN")
                    self.state = CircuitState.OPEN
            
            raise


class CircuitBreakerOpenError(Exception):
    """Exception raised when a circuit breaker is open."""
    pass


class CircuitBreakerRegistry:
    """Registry for managing multiple circuit breakers."""
    
    def __init__(self):
        self.circuit_breakers: Dict[str, CircuitBreaker] = {}
        self.lock = threading.RLock()
    
    def get_circuit_breaker(self, name: str) -> CircuitBreaker:
        """Get a circuit breaker with the specified name, creating it if it doesn't