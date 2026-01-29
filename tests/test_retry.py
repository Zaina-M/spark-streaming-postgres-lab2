"""
Unit tests for retry logic and circuit breaker.
Run with: pytest tests/test_retry.py -v
"""
import os
import sys
import time
import pytest

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'spark'))

from utils.retry import (
    retry_with_backoff,
    CircuitBreaker,
    CircuitBreakerOpenError,
    CircuitState,
    RetryPolicy,
    NonRetryableError,
)


class TestRetryWithBackoff:
    #Tests for the retry_with_backoff decorator.
    
    def test_successful_call_no_retry(self):
        # Successful calls should not retry.
        call_count = 0
        
        @retry_with_backoff(max_retries=3)
        def always_succeeds():
            nonlocal call_count
            call_count += 1
            return "success"
        
        result = always_succeeds()
        assert result == "success"
        assert call_count == 1
    
    def test_retries_on_failure(self):
        # Should retry on failure.
        call_count = 0
        
        @retry_with_backoff(max_retries=3, initial_delay=0.01)
        def fails_twice():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("Connection failed")
            return "success"
        
        result = fails_twice()
        assert result == "success"
        assert call_count == 3
    
    def test_max_retries_exceeded(self):
        # Should raise after max retries.
        call_count = 0
        
        @retry_with_backoff(max_retries=2, initial_delay=0.01)
        def always_fails():
            nonlocal call_count
            call_count += 1
            raise ConnectionError("Always fails")
        
        with pytest.raises(ConnectionError):
            always_fails()
        
        assert call_count == 3  # 1 initial + 2 retries
    
    def test_only_retries_specified_exceptions(self):
        # Should only retry specified exception types.
        call_count = 0
        
        @retry_with_backoff(
            max_retries=3, 
            initial_delay=0.01,
            exceptions=(ConnectionError,)
        )
        def raises_value_error():
            nonlocal call_count
            call_count += 1
            raise ValueError("Not retryable")
        
        with pytest.raises(ValueError):
            raises_value_error()
        
        assert call_count == 1  # No retries for ValueError
    
    def test_non_retryable_error_not_retried(self):
        # NonRetryableError should not be retried.  
        call_count = 0
        
        @retry_with_backoff(max_retries=3, initial_delay=0.01)
        def raises_non_retryable():
            nonlocal call_count
            call_count += 1
            raise NonRetryableError("Do not retry this")
        
        with pytest.raises(NonRetryableError):
            raises_non_retryable()
        
        assert call_count == 1
    
    def test_callback_called_on_retry(self):
        # Callback should be called on each retry.
        retry_attempts = []
        
        def on_retry(exception, attempt):
            retry_attempts.append(attempt)
        
        @retry_with_backoff(
            max_retries=2, 
            initial_delay=0.01,
            on_retry=on_retry
        )
        def fails_then_succeeds():
            if len(retry_attempts) < 2:
                raise ConnectionError("Failing")
            return "success"
        
        result = fails_then_succeeds()
        assert result == "success"
        assert retry_attempts == [1, 2]


class TestCircuitBreaker:
    # Tests for the circuit breaker.
    
    def test_starts_closed(self):
        # Circuit breaker should start in closed state.
        cb = CircuitBreaker(failure_threshold=3)
        assert cb.state == CircuitState.CLOSED
    
    def test_successful_calls_keep_closed(self):
        # Successful calls should keep circuit closed.
        cb = CircuitBreaker(failure_threshold=3)
        
        def always_succeeds():
            return "success"
        
        for _ in range(10):
            result = cb.call(always_succeeds)
            assert result == "success"
        
        assert cb.state == CircuitState.CLOSED
    
    def test_opens_after_threshold_failures(self):
        # Circuit should open after failure threshold.
        cb = CircuitBreaker(failure_threshold=3, expected_exceptions=(ValueError,))
        
        def always_fails():
            raise ValueError("Failure")
        
        # Fail 3 times
        for _ in range(3):
            with pytest.raises(ValueError):
                cb.call(always_fails)
        
        assert cb.state == CircuitState.OPEN
    
    def test_open_circuit_rejects_immediately(self):
        # Open circuit should reject without calling function.
        cb = CircuitBreaker(
            failure_threshold=2, 
            recovery_timeout=10,
            expected_exceptions=(ValueError,)
        )
        
        def always_fails():
            raise ValueError("Failure")
        
        # Open the circuit
        for _ in range(2):
            with pytest.raises(ValueError):
                cb.call(always_fails)
        
        # Now should reject immediately
        call_count = 0
        def track_calls():
            nonlocal call_count
            call_count += 1
            return "success"
        
        with pytest.raises(CircuitBreakerOpenError):
            cb.call(track_calls)
        
        assert call_count == 0  # Function was never called
    
    def test_half_open_after_timeout(self):
        # Circuit should go half-open after recovery timeout.
        cb = CircuitBreaker(
            failure_threshold=2,
            recovery_timeout=0.1,  # 100ms
            expected_exceptions=(ValueError,)
        )
        
        def always_fails():
            raise ValueError("Failure")
        
        # Open the circuit
        for _ in range(2):
            with pytest.raises(ValueError):
                cb.call(always_fails)
        
        assert cb.state == CircuitState.OPEN
        
        # Wait for recovery timeout
        time.sleep(0.15)
        
        # Should now be half-open
        assert cb.state == CircuitState.HALF_OPEN
    
    def test_closes_after_success_in_half_open(self):
        # Circuit should close after successes in half-open.
        cb = CircuitBreaker(
            failure_threshold=2,
            recovery_timeout=0.05,
            expected_exceptions=(ValueError,)
        )
        
        fail_count = 0
        def fails_twice_then_succeeds():
            nonlocal fail_count
            fail_count += 1
            if fail_count <= 2:
                raise ValueError("Failure")
            return "success"
        
        # Open the circuit
        for _ in range(2):
            with pytest.raises(ValueError):
                cb.call(fails_twice_then_succeeds)
        
        # Wait for recovery
        time.sleep(0.1)
        
        # Successful calls in half-open
        for _ in range(3):
            cb.call(fails_twice_then_succeeds)
        
        assert cb.state == CircuitState.CLOSED
    
    def test_reopens_on_failure_in_half_open(self):
        # Circuit should reopen on failure in half-open.
        cb = CircuitBreaker(
            failure_threshold=2,
            recovery_timeout=0.05,
            expected_exceptions=(ValueError,)
        )
        
        def always_fails():
            raise ValueError("Failure")
        
        # Open the circuit
        for _ in range(2):
            with pytest.raises(ValueError):
                cb.call(always_fails)
        
        # Wait for recovery
        time.sleep(0.1)
        
        # Fail in half-open
        with pytest.raises(ValueError):
            cb.call(always_fails)
        
        assert cb.state == CircuitState.OPEN
    
    def test_manual_reset(self):
        # Should be able to manually reset circuit.
        cb = CircuitBreaker(failure_threshold=2, expected_exceptions=(ValueError,))
        
        def always_fails():
            raise ValueError("Failure")
        
        # Open the circuit
        for _ in range(2):
            with pytest.raises(ValueError):
                cb.call(always_fails)
        
        assert cb.state == CircuitState.OPEN
        
        cb.reset()
        
        assert cb.state == CircuitState.CLOSED
    
    def test_stats_tracking(self):
        # Should track call statistics.
        cb = CircuitBreaker(failure_threshold=5, expected_exceptions=(ValueError,))
        
        def succeeds():
            return "success"
        
        def fails():
            raise ValueError("Failure")
        
        # 5 successes
        for _ in range(5):
            cb.call(succeeds)
        
        # 3 failures
        for _ in range(3):
            with pytest.raises(ValueError):
                cb.call(fails)
        
        stats = cb.get_stats()
        assert stats["total_calls"] == 8
        assert stats["total_successes"] == 5
        assert stats["total_failures"] == 3


class TestRetryPolicy:
    # Tests for the RetryPolicy class.
    
    def test_execute_with_success(self):
        # Should execute function successfully.
        policy = RetryPolicy(max_retries=3)
        
        result = policy.execute(lambda: "success")
        assert result == "success"
    
    def test_execute_with_retries(self):
        # Should retry on failure.
        policy = RetryPolicy(
            max_retries=3,
            initial_delay=0.01,
            retryable_exceptions=(ValueError,)
        )
        
        call_count = 0
        def fails_once():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ValueError("First call fails")
            return "success"
        
        result = policy.execute(fails_once)
        assert result == "success"
        assert call_count == 2
    
    def test_execute_with_circuit_breaker(self):
        # Should integrate with circuit breaker.
        cb = CircuitBreaker(
            failure_threshold=2,
            expected_exceptions=(ValueError,)
        )
        policy = RetryPolicy(
            max_retries=1,
            initial_delay=0.01,
            retryable_exceptions=(ValueError, CircuitBreakerOpenError),
            circuit_breaker=cb
        )
        
        def always_fails():
            raise ValueError("Failure")
        
        # Exhaust retries and open circuit
        with pytest.raises(ValueError):
            policy.execute(always_fails)
        
        # Second call should open the circuit
        with pytest.raises((ValueError, CircuitBreakerOpenError)):
            policy.execute(always_fails)
        
        # Circuit should now be open
        assert cb.state == CircuitState.OPEN


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
