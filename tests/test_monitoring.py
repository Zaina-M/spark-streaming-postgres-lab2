"""
Unit tests for monitoring and alerting.
Run with: pytest tests/test_monitoring.py -v
"""
import os
import sys
import time
import pytest
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'spark'))

from monitoring.metrics import (
    PipelineMonitor,
    BatchMetrics,
    Alert,
    AlertLevel,
    AlertHandler,
    ConsoleAlertHandler,
    get_monitor,
    reset_monitor,
)


class MockAlertHandler(AlertHandler):
    # Mock handler that records alerts for testing.
    
    def __init__(self):
        self.alerts = []
    
    def send(self, alert: Alert) -> bool:
        self.alerts.append(alert)
        return True
    
    def clear(self):
        self.alerts.clear()


class TestBatchMetrics:
    # Tests for BatchMetrics dataclass.
    
    def test_validity_rate_calculation(self):
        # Should calculate validity rate correctly.
        metrics = BatchMetrics(
            batch_id=1,
            start_time=datetime.now(),
            records_read=100,
            records_valid=95,
            records_invalid=5
        )
        assert metrics.validity_rate == 95.0
    
    def test_validity_rate_empty_batch(self):
        # Empty batch should have 100% validity.
        metrics = BatchMetrics(
            batch_id=1,
            start_time=datetime.now(),
            records_read=0,
            records_valid=0
        )
        assert metrics.validity_rate == 100.0
    
    def test_throughput_calculation(self):
        # Should calculate throughput correctly.
        metrics = BatchMetrics(
            batch_id=1,
            start_time=datetime.now(),
            records_read=1000,
            processing_time_ms=1000  # 1 second
        )
        assert metrics.throughput == 1000.0  # 1000 records/second
    
    def test_throughput_zero_time(self):
        # Zero processing time should return 0 throughput.
        metrics = BatchMetrics(
            batch_id=1,
            start_time=datetime.now(),
            records_read=100,
            processing_time_ms=0
        )
        assert metrics.throughput == 0.0
    
    def test_to_dict(self):
        # Should convert to dictionary correctly.
        metrics = BatchMetrics(
            batch_id=1,
            start_time=datetime.now(),
            records_read=100,
            records_valid=95
        )
        data = metrics.to_dict()
        assert data["batch_id"] == 1
        assert data["records_read"] == 100
        assert "validity_rate" in data


class TestPipelineMonitor:
    # Tests for PipelineMonitor class.
    
    @pytest.fixture
    def monitor(self):
        # Create a fresh monitor for each test.
        return PipelineMonitor(
            validity_threshold=95.0,
            latency_threshold_ms=1000,
            throughput_threshold=100.0
        )
    
    @pytest.fixture
    def mock_handler(self):
        # Create a mock alert handler.
        return MockAlertHandler()
    
    def test_track_batch_context_manager(self, monitor):
        # Context manager should record batch metrics.
        with monitor.track_batch(batch_id=1) as metrics:
            metrics.records_read = 100
            metrics.records_valid = 95
        
        summary = monitor.get_summary()
        assert summary["total_batches"] == 1
        assert summary["total_records_processed"] == 100
    
    def test_validity_alert_triggered(self, monitor, mock_handler):
        # Should trigger alert when validity drops below threshold.
        monitor.add_handler(mock_handler)
        
        with monitor.track_batch(batch_id=1) as metrics:
            metrics.records_read = 100
            metrics.records_valid = 80  # 80% < 95% threshold
            metrics.records_invalid = 20
        
        assert len(mock_handler.alerts) >= 1
        assert any(a.title == "Low Data Quality" for a in mock_handler.alerts)
    
    def test_no_alert_when_above_threshold(self, monitor, mock_handler):
        # Should not alert when metrics are good.
        monitor.add_handler(mock_handler)
        
        with monitor.track_batch(batch_id=1) as metrics:
            metrics.records_read = 100
            metrics.records_valid = 98
            metrics.records_invalid = 2
            metrics.processing_time_ms = 100
        
        # Only check for quality-related alerts
        quality_alerts = [
            a for a in mock_handler.alerts 
            if "Quality" in a.title or "Latency" in a.title
        ]
        assert len(quality_alerts) == 0
    
    def test_latency_alert_triggered(self, monitor, mock_handler):
        # Should trigger alert when latency exceeds threshold.
        monitor.add_handler(mock_handler)
        
        # Simulate a slow batch
        metrics = BatchMetrics(
            batch_id=1,
            start_time=datetime.now(),
            end_time=datetime.now(),
            records_read=100,
            records_valid=100,
            processing_time_ms=5000  # 5 seconds > 1 second threshold
        )
        monitor.record_batch(metrics)
        
        assert any(a.title == "High Processing Latency" for a in mock_handler.alerts)
    
    def test_consecutive_failures_escalate(self, monitor, mock_handler):
        # Consecutive failures should escalate alert level.
        monitor.add_handler(mock_handler)
        monitor.consecutive_failures_threshold = 2
        
        # Two consecutive low validity batches
        for i in range(2):
            with monitor.track_batch(batch_id=i) as metrics:
                metrics.records_read = 100
                metrics.records_valid = 80
                metrics.records_invalid = 20
        
        # Should have at least one ERROR level alert
        error_alerts = [a for a in mock_handler.alerts if a.level == AlertLevel.ERROR]
        assert len(error_alerts) >= 1
    
    def test_get_summary(self, monitor):
        # Should return accurate summary.
        for i in range(5):
            with monitor.track_batch(batch_id=i) as metrics:
                metrics.records_read = 100
                metrics.records_valid = 95
                metrics.records_invalid = 5
                metrics.processing_time_ms = 100
        
        summary = monitor.get_summary()
        assert summary["status"] == "HEALTHY"
        assert summary["total_batches"] == 5
        assert summary["total_records_processed"] == 500
        assert summary["avg_validity_rate"] == 95.0
    
    def test_degraded_status(self, monitor):
        # Should report degraded status when quality is low.
        for i in range(5):
            with monitor.track_batch(batch_id=i) as metrics:
                metrics.records_read = 100
                metrics.records_valid = 90  # Below 95% threshold
                metrics.records_invalid = 10
        
        summary = monitor.get_summary()
        assert summary["status"] == "DEGRADED"
    
    def test_get_recent_batches(self, monitor):
        # Should return recent batch metrics.
        for i in range(10):
            with monitor.track_batch(batch_id=i) as metrics:
                metrics.records_read = 100
        
        recent = monitor.get_recent_batches(count=5)
        assert len(recent) == 5
        # Should be the last 5 batches
        assert recent[-1]["batch_id"] == 9
    
    def test_reset_stats(self, monitor):
        # Should reset all statistics.
        for i in range(5):
            with monitor.track_batch(batch_id=i) as metrics:
                metrics.records_read = 100
        
        monitor.reset_stats()
        
        summary = monitor.get_summary()
        assert summary["status"] == "NO_DATA"
    
    def test_error_pattern_detection(self, monitor, mock_handler):
        # Should detect high concentration of specific errors.
        monitor.add_handler(mock_handler)
        
        with monitor.track_batch(batch_id=1) as metrics:
            metrics.records_read = 100
            metrics.records_valid = 85
            metrics.records_invalid = 15
            metrics.validation_errors = {"negative_price": 12}  # 12% error rate
        
        error_alerts = [
            a for a in mock_handler.alerts 
            if "Error Concentration" in a.title
        ]
        assert len(error_alerts) >= 1


class TestAlertHandlers:
    # Tests for alert handlers.
    
    def test_console_handler(self, capsys):
        # Console handler should log alerts.
        handler = ConsoleAlertHandler()
        alert = Alert(
            level=AlertLevel.WARNING,
            title="Test Alert",
            message="This is a test",
            timestamp=datetime.now()
        )
        
        result = handler.send(alert)
        assert result is True
    
    def test_mock_handler_records_alerts(self):
        # Mock handler should record all alerts.
        handler = MockAlertHandler()
        
        for i in range(3):
            alert = Alert(
                level=AlertLevel.INFO,
                title=f"Alert {i}",
                message="Test",
                timestamp=datetime.now()
            )
            handler.send(alert)
        
        assert len(handler.alerts) == 3


class TestGlobalMonitor:
    # Tests for global monitor functions.
    
    def test_get_monitor_creates_instance(self):
        # get_monitor should create a monitor instance.
        reset_monitor()
        monitor = get_monitor()
        assert isinstance(monitor, PipelineMonitor)
    
    def test_get_monitor_returns_same_instance(self):
        # get_monitor should return the same instance.
        reset_monitor()
        monitor1 = get_monitor()
        monitor2 = get_monitor()
        assert monitor1 is monitor2
    
    def test_reset_monitor(self):
        # reset_monitor should clear the global instance.
        reset_monitor()
        monitor1 = get_monitor()
        reset_monitor()
        monitor2 = get_monitor()
        assert monitor1 is not monitor2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
