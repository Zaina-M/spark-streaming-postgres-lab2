# Monitoring and alerting system for the streaming pipeline.

import time
import logging
import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Any
from collections import deque
from enum import Enum
import threading

logger = logging.getLogger(__name__)


class AlertLevel(Enum):
    # Severity levels for alerts.
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class BatchMetrics:
    # Metrics for a single processing batch.
    batch_id: int
    start_time: datetime
    end_time: Optional[datetime] = None
    
    # Record counts
    records_read: int = 0
    records_valid: int = 0
    records_invalid: int = 0
    records_written: int = 0
    records_deduplicated: int = 0
    
    # Processing metrics
    processing_time_ms: float = 0.0
    
    # Error tracking
    errors: List[str] = field(default_factory=list)
    validation_errors: Dict[str, int] = field(default_factory=dict)
    
    @property
    def validity_rate(self) -> float:
        # Percentage of valid records.
        if self.records_read == 0:
            return 100.0
        return (self.records_valid / self.records_read) * 100
    
    @property
    def throughput(self) -> float:
        # Records processed per second.
        if self.processing_time_ms == 0:
            return 0.0
        return self.records_read / (self.processing_time_ms / 1000)
    
    @property
    def error_rate(self) -> float:
        # Percentage of records with errors.
        if self.records_read == 0:
            return 0.0
        return (self.records_invalid / self.records_read) * 100
    
    def to_dict(self) -> dict:
        # Convert to dictionary for logging/storage.
        return {
            "batch_id": self.batch_id,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "records_read": self.records_read,
            "records_valid": self.records_valid,
            "records_invalid": self.records_invalid,
            "records_written": self.records_written,
            "records_deduplicated": self.records_deduplicated,
            "processing_time_ms": self.processing_time_ms,
            "validity_rate": round(self.validity_rate, 2),
            "throughput": round(self.throughput, 2),
            "error_rate": round(self.error_rate, 2),
            "validation_errors": self.validation_errors,
        }


@dataclass
class Alert:
    # Represents an alert triggered by the monitoring system.
    level: AlertLevel
    title: str
    message: str
    timestamp: datetime
    metrics: Optional[BatchMetrics] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> dict:
        # Convert to dictionary for logging.
        return {
            "level": self.level.value,
            "title": self.title,
            "message": self.message,
            "timestamp": self.timestamp.isoformat(),
            "metadata": self.metadata,
        }


class AlertHandler:
    # Base class for alert handlers.
    
    def send(self, alert: Alert) -> bool:
        # Send an alert. Returns True if successful.
        raise NotImplementedError


class ConsoleAlertHandler(AlertHandler):
    # Handler that logs alerts to console/log file."""
    
    def send(self, alert: Alert) -> bool:
        log_func = {
            AlertLevel.INFO: logger.info,
            AlertLevel.WARNING: logger.warning,
            AlertLevel.ERROR: logger.error,
            AlertLevel.CRITICAL: logger.critical,
        }
        
        log_func[alert.level](
            f"ALERT [{alert.level.value.upper()}] [{alert.title}]: {alert.message}"
        )
        return True


class PipelineMonitor:
   
    
    def __init__(
        self,
        validity_threshold: float = 95.0,
        latency_threshold_ms: float = 10000.0,
        throughput_threshold: float = 100.0,
        window_size: int = 100,
        consecutive_failures_threshold: int = 3
    ):
        self.validity_threshold = validity_threshold
        self.latency_threshold_ms = latency_threshold_ms
        self.throughput_threshold = throughput_threshold
        self.window_size = window_size
        self.consecutive_failures_threshold = consecutive_failures_threshold
        
        # Alert handlers (just log to file by default)
        self._handlers: List[AlertHandler] = []
        
        # Rolling window of recent batches
        self._recent_batches: deque = deque(maxlen=window_size)
        
        # Counters
        self._total_records_processed = 0
        self._total_records_failed = 0
        self._total_batches = 0
        self._alerts_triggered = 0
        
        # Consecutive failure tracking
        self._consecutive_low_validity = 0
        self._consecutive_high_latency = 0
        
        # Thread safety
        self._lock = threading.Lock()
        
        # Start time
        self._start_time = datetime.now()
    
    def add_handler(self, handler: AlertHandler) -> None:
        # Add an alert handler.
        self._handlers.append(handler)
    
    def remove_handler(self, handler: AlertHandler) -> None:
        # Remove an alert handler.
        if handler in self._handlers:
            self._handlers.remove(handler)
    
    def track_batch(self, batch_id: int) -> 'BatchTracker':
       
        return BatchTracker(self, batch_id)
    
    def record_batch(self, metrics: BatchMetrics) -> None:
        # Record completed batch metrics and check thresholds.
        with self._lock:
            self._recent_batches.append(metrics)
            self._total_batches += 1
            self._total_records_processed += metrics.records_read
            self._total_records_failed += metrics.records_invalid
        
        # Check thresholds and potentially alert
        self._check_validity_threshold(metrics)
        self._check_latency_threshold(metrics)
        self._check_error_patterns(metrics)
        
        # Log metrics
        self._log_batch_metrics(metrics)
    
    def _check_validity_threshold(self, metrics: BatchMetrics) -> None:
        # Check if validity rate is below threshold.
        if metrics.validity_rate < self.validity_threshold:
            self._consecutive_low_validity += 1
            
            level = (
                AlertLevel.ERROR 
                if self._consecutive_low_validity >= self.consecutive_failures_threshold 
                else AlertLevel.WARNING
            )
            
            self._trigger_alert(Alert(
                level=level,
                title="Low Data Quality",
                message=(
                    f"Batch {metrics.batch_id} validity rate: {metrics.validity_rate:.1f}% "
                    f"(threshold: {self.validity_threshold}%)"
                ),
                timestamp=datetime.now(),
                metrics=metrics
            ))
        else:
            self._consecutive_low_validity = 0
    
    def _check_latency_threshold(self, metrics: BatchMetrics) -> None:
        # Check if processing latency exceeds threshold.
        if metrics.processing_time_ms > self.latency_threshold_ms:
            self._consecutive_high_latency += 1
            
            level = (
                AlertLevel.ERROR 
                if self._consecutive_high_latency >= self.consecutive_failures_threshold 
                else AlertLevel.WARNING
            )
            
            self._trigger_alert(Alert(
                level=level,
                title="High Processing Latency",
                message=(
                    f"Batch {metrics.batch_id} took {metrics.processing_time_ms:.0f}ms "
                    f"(threshold: {self.latency_threshold_ms}ms)"
                ),
                timestamp=datetime.now(),
                metrics=metrics
            ))
        else:
            self._consecutive_high_latency = 0
    
    def _check_error_patterns(self, metrics: BatchMetrics) -> None:
        # Check for concentrated error patterns (e.g., too many of one error type).
        if not metrics.validation_errors or metrics.records_read == 0:
            return
        
        # Check if any single error type exceeds 10% of total records
        for error_type, count in metrics.validation_errors.items():
            error_rate = (count / metrics.records_read) * 100
            if error_rate >= 10.0:  # 10% threshold for error concentration
                self._trigger_alert(Alert(
                    level=AlertLevel.WARNING,
                    title="Error Concentration",
                    message=(
                        f"Batch {metrics.batch_id}: '{error_type}' errors at {error_rate:.1f}% "
                        f"({count}/{metrics.records_read} records)"
                    ),
                    timestamp=datetime.now(),
                    metrics=metrics
                ))
    
    def _trigger_alert(self, alert: Alert) -> None:
        # Send alert to all handlers.
        self._alerts_triggered += 1
        
        for handler in self._handlers:
            try:
                handler.send(alert)
            except Exception as e:
                logger.error(f"Failed to send alert: {e}")
    
    def _log_batch_metrics(self, metrics: BatchMetrics) -> None:
        # Log batch metrics summary.
        logger.info(
            f"Batch {metrics.batch_id}: "
            f"read={metrics.records_read}, "
            f"valid={metrics.records_valid} ({metrics.validity_rate:.1f}%), "
            f"time={metrics.processing_time_ms:.0f}ms"
        )
    
    def get_health_summary(self) -> dict:
        # Get overall pipeline health summary.
        with self._lock:
            total_processed = self._total_records_processed
            total_failed = self._total_records_failed
            
            if total_processed == 0:
                status = "NO_DATA"
                overall_success = 100.0
                avg_validity = 0.0
            else:
                overall_success = (
                    (total_processed - total_failed) / total_processed * 100
                )
                status = "HEALTHY" if overall_success >= self.validity_threshold else "DEGRADED"
                
                # Calculate average validity rate from recent batches
                if self._recent_batches:
                    validity_rates = [b.validity_rate for b in self._recent_batches]
                    avg_validity = sum(validity_rates) / len(validity_rates)
                else:
                    avg_validity = overall_success
            
            return {
                "status": status,
                "total_batches": self._total_batches,
                "total_records_processed": total_processed,
                "total_records_failed": total_failed,
                "overall_success_rate": round(overall_success, 2),
                "avg_validity_rate": round(avg_validity, 2),
                "alerts_triggered": self._alerts_triggered,
                "uptime_seconds": (datetime.now() - self._start_time).total_seconds(),
            }
    
    def get_summary(self) -> dict:
        # Get pipeline monitoring summary (alias for get_health_summary).
        return self.get_health_summary()
    
    def get_recent_batches(self, count: int = 10) -> List[dict]:
        # Get the most recent batch metrics as dictionaries.
        with self._lock:
            batches = list(self._recent_batches)
            recent = batches[-count:] if count < len(batches) else batches
            return [b.to_dict() for b in recent]
    
    def reset_stats(self) -> None:
        # Reset all monitoring statistics.
        with self._lock:
            self._recent_batches.clear()
            self._total_records_processed = 0
            self._total_records_failed = 0
            self._total_batches = 0
            self._alerts_triggered = 0
            self._consecutive_low_validity = 0
            self._consecutive_high_latency = 0
            self._start_time = datetime.now()


class BatchTracker:
    # Context manager for tracking batch metrics.
    
    def __init__(self, monitor: PipelineMonitor, batch_id: int):
        self.monitor = monitor
        self.metrics = BatchMetrics(
            batch_id=batch_id,
            start_time=datetime.now()
        )
        self._start_perf = None
    
    def __enter__(self) -> BatchMetrics:
        self._start_perf = time.perf_counter()
        return self.metrics
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.metrics.end_time = datetime.now()
        self.metrics.processing_time_ms = (
            (time.perf_counter() - self._start_perf) * 1000
        )
        
        if exc_type:
            self.metrics.errors.append(f"{exc_type.__name__}: {exc_val}")
        
        self.monitor.record_batch(self.metrics)
        return False  # Don't suppress exceptions


# Global monitor instance
_pipeline_monitor: Optional[PipelineMonitor] = None


def get_monitor(
    validity_threshold: float = 95.0,
    latency_threshold_ms: float = 10000.0,
    throughput_threshold: float = 100.0
) -> PipelineMonitor:
    # Get or create the global pipeline monitor.
    global _pipeline_monitor
    if _pipeline_monitor is None:
        _pipeline_monitor = PipelineMonitor(
            validity_threshold=validity_threshold,
            latency_threshold_ms=latency_threshold_ms,
            throughput_threshold=throughput_threshold
        )
        # Add default console handler (logs to file)
        _pipeline_monitor.add_handler(ConsoleAlertHandler())
    return _pipeline_monitor


def reset_monitor() -> None:
    # Reset the global monitor (for testing).
    global _pipeline_monitor
    _pipeline_monitor = None
