"""
Retry logic and fault tolerance utilities.
Provides decorators and classes for handling transient failures.
"""
import time
import logging
from functools import wraps
from typing import Callable, Type, Tuple, Optional, Any
from datetime import datetime
from enum import Enum

logger = logging.getLogger(__name__)


class RetryableError(Exception):
    # Base exception for errors that should trigger a retry.
    pass


class NonRetryableError(Exception):
    # Base exception for errors that should NOT trigger a retry.
    pass


def retry_with_backoff(
    max_retries: int = 3,
    initial_delay: float = 1.0,
    backoff_factor: float = 2.0,
    max_delay: float = 60.0,
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
    on_retry: Optional[Callable[[Exception, int], None]] = None
) -> Callable:
    
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            delay = initial_delay
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                    
                except NonRetryableError:
                    # Don't retry non-retryable errors
                    raise
                    
                except exceptions as e:
                    last_exception = e
                    
                    if attempt < max_retries:
                        # Log the retry attempt
                        logger.warning(
                            f"[{func.__name__}] Attempt {attempt + 1}/{max_retries + 1} failed: {e}. "
                            f"Retrying in {delay:.1f}s..."
                        )
                        
                        # Call retry callback if provided
                        if on_retry:
                            on_retry(e, attempt + 1)
                        
                        # Wait before retry
                        time.sleep(delay)
                        
                        # Increase delay for next retry (with cap)
                        delay = min(delay * backoff_factor, max_delay)
                    else:
                        logger.error(
                            f"[{func.__name__}] All {max_retries + 1} attempts failed. "
                            f"Last error: {e}"
                        )
            
            # All retries exhausted, raise the last exception
            raise last_exception
        
        return wrapper
    return decorator


class CircuitState(Enum):
    # States for the circuit breaker.
    CLOSED = "closed"      # Normal operation, requests go through
    OPEN = "open"          # Failing, requests are rejected immediately
    HALF_OPEN = "half_open"  # Testing if service recovered


class CircuitBreakerOpenError(Exception):
    # Raised when circuit breaker is open and rejecting requests.
    
    def __init__(self, message: str, time_until_retry: float):
        super().__init__(message)
        self.time_until_retry = time_until_retry


class CircuitBreaker:
    
    
    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        expected_exceptions: Tuple[Type[Exception], ...] = (Exception,),
        name: str = "default"
    ):
        
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exceptions = expected_exceptions
        self.name = name
        
        # State tracking
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._last_failure_time: Optional[float] = None
        self._success_count_in_half_open = 0
        
        # Statistics
        self._total_calls = 0
        self._total_failures = 0
        self._total_successes = 0
        self._times_opened = 0
    
    @property
    def state(self) -> CircuitState:
        # Get current circuit state (may transition if timeout expired).
        if self._state == CircuitState.OPEN:
            if self._should_attempt_recovery():
                logger.info(f"[CircuitBreaker:{self.name}] Transitioning to HALF_OPEN")
                self._state = CircuitState.HALF_OPEN
        return self._state
    
    def _should_attempt_recovery(self) -> bool:
        # Check if enough time has passed to attempt recovery.
        if self._last_failure_time is None:
            return True
        return time.time() - self._last_failure_time > self.recovery_timeout
    
    def call(self, func: Callable, *args, **kwargs) -> Any:
        # Call a function with circuit breaker protection.
        self._total_calls += 1
        current_state = self.state
        
        # If circuit is open, reject immediately
        if current_state == CircuitState.OPEN:
            time_until_retry = self.recovery_timeout - (
                time.time() - (self._last_failure_time or 0)
            )
            raise CircuitBreakerOpenError(
                f"Circuit breaker '{self.name}' is OPEN. "
                f"Try again in {time_until_retry:.1f}s",
                time_until_retry=max(0, time_until_retry)
            )
        
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
            
        except self.expected_exceptions as e:
            self._on_failure(e)
            raise
    
    def _on_success(self):
        # Handle successful call.
        self._total_successes += 1
        
        if self._state == CircuitState.HALF_OPEN:
            self._success_count_in_half_open += 1
            # After 3 successes in half-open, close the circuit
            if self._success_count_in_half_open >= 3:
                logger.info(
                    f"[CircuitBreaker:{self.name}] Recovery successful, "
                    f"transitioning to CLOSED"
                )
                self._state = CircuitState.CLOSED
                self._failure_count = 0
                self._success_count_in_half_open = 0
        else:
            # Reset failure count on success in closed state
            self._failure_count = 0
    
    def _on_failure(self, exception: Exception):
        # Handle failed call.
        self._total_failures += 1
        self._failure_count += 1
        self._last_failure_time = time.time()
        
        logger.warning(
            f"[CircuitBreaker:{self.name}] Failure {self._failure_count}/"
            f"{self.failure_threshold}: {exception}"
        )
        
        if self._state == CircuitState.HALF_OPEN:
            # Any failure in half-open immediately opens the circuit
            logger.warning(
                f"[CircuitBreaker:{self.name}] Failure in HALF_OPEN state, "
                f"returning to OPEN"
            )
            self._state = CircuitState.OPEN
            self._success_count_in_half_open = 0
            
        elif self._failure_count >= self.failure_threshold:
            # Threshold reached, open the circuit
            logger.error(
                f"[CircuitBreaker:{self.name}] Failure threshold reached, "
                f"opening circuit"
            )
            self._state = CircuitState.OPEN
            self._times_opened += 1
    
    def reset(self):
        # Manually reset the circuit breaker to closed state.
        logger.info(f"[CircuitBreaker:{self.name}] Manually reset")
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._last_failure_time = None
        self._success_count_in_half_open = 0
    
    def get_stats(self) -> dict:
        # Get circuit breaker statistics.
        return {
            "name": self.name,
            "state": self.state.value,
            "failure_count": self._failure_count,
            "failure_threshold": self.failure_threshold,
            "total_calls": self._total_calls,
            "total_successes": self._total_successes,
            "total_failures": self._total_failures,
            "times_opened": self._times_opened,
            "success_rate": (
                self._total_successes / self._total_calls * 100 
                if self._total_calls > 0 else 100
            ),
        }


class RetryPolicy:
    
    
    def __init__(
        self,
        max_retries: int = 3,
        initial_delay: float = 1.0,
        backoff_factor: float = 2.0,
        max_delay: float = 60.0,
        retryable_exceptions: Tuple[Type[Exception], ...] = (Exception,),
        circuit_breaker: Optional[CircuitBreaker] = None
    ):
        self.max_retries = max_retries
        self.initial_delay = initial_delay
        self.backoff_factor = backoff_factor
        self.max_delay = max_delay
        self.retryable_exceptions = retryable_exceptions
        self.circuit_breaker = circuit_breaker
    
    def execute(self, func: Callable, *args, **kwargs) -> Any:
        # Execute a function with the defined retry policy.
        @retry_with_backoff(
            max_retries=self.max_retries,
            initial_delay=self.initial_delay,
            backoff_factor=self.backoff_factor,
            max_delay=self.max_delay,
            exceptions=self.retryable_exceptions
        )
        def wrapped():
            if self.circuit_breaker:
                return self.circuit_breaker.call(func, *args, **kwargs)
            return func(*args, **kwargs)
        
        return wrapped()


# Pre-configured retry policies for common use cases
database_retry_policy = RetryPolicy(
    max_retries=3,
    initial_delay=1.0,
    backoff_factor=2.0,
    retryable_exceptions=(ConnectionError, TimeoutError, IOError)
)

network_retry_policy = RetryPolicy(
    max_retries=5,
    initial_delay=0.5,
    backoff_factor=1.5,
    retryable_exceptions=(ConnectionError, TimeoutError)
)

file_retry_policy = RetryPolicy(
    max_retries=2,
    initial_delay=0.5,
    backoff_factor=2.0,
    retryable_exceptions=(IOError, PermissionError)
)
