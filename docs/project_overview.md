# E-Commerce Streaming Pipeline - Project Overview

## Executive Summary

This project implements a **real-time data pipeline** for processing e-commerce events. It features schema versioning, fault tolerance with retry logic and circuit breakers, comprehensive monitoring, and 124+ unit tests.

## System Components

### 1. Data Generator (`data_generator/data_generator.py`)

**Purpose**: Generates realistic synthetic e-commerce event data with configurable anomalies.

**Features**:
- **Event Types**: view, search, add_to_cart, remove_from_cart, wishlist, purchase
- **Session Tracking**: User sessions with realistic behavior patterns
- **User Segments**: new, returning, premium, inactive, at-risk
- **Categories**: 10+ product categories with price ranges
- **Anomaly Injection**: Configurable rate for testing validation (2% default)
- **Business Logic**: Anonymous users (user_id=0) can only view/search

**Output**: CSV files (100 events/file, every 5 seconds) → `data/input/`

**Technology**: Python 3.10+, pandas, uuid

---

### 2. Configuration Management (`config/settings.py`)

**Purpose**: Centralized configuration with environment variable support.

**Features**:
- **Dataclass-based**: Type-safe configuration with validation
- **Environment Variables**: All settings configurable via `.env`
- **Config Categories**:
  - `DatabaseConfig`: PostgreSQL connection settings
  - `StreamingConfig`: Spark streaming parameters
  - `DataQualityConfig`: Validation thresholds
  - `GeneratorConfig`: Data generator settings
  - `RetryConfig`: Retry and circuit breaker settings
  - `MonitoringConfig`: Alerting thresholds

**Technology**: Python dataclasses, dotenv

---

### 3. Schema Registry (`spark/schema/registry.py`)

**Purpose**: Version control for data schemas with migration support.

**Features**:
- **Schema Versions**: v1, v2, v3 with backwards compatibility
- **Auto-Migration**: Automatically upgrade data to latest schema
- **Validation**: Validate data against registered schemas
- **Extensible**: Easy to add new schema versions

**Schemas**:
| Version | Fields Added |
|---------|--------------|
| v1 | event_id, user_id, event_type, product_id, price, event_time |
| v2 | + session_id, category, quantity, user_segment |
| v3 | + device_type, browser, geo_country, geo_city, referrer, campaign_id |

---

### 4. Apache Spark Streaming (`spark/spark_streaming_to_postgres.py`)

**Purpose**: Processes streaming CSV data with validation, enrichment, and routing.

**Features**:
- **Real-time Processing**: 10-second trigger intervals
- **Data Validation**: 10+ validation rules
- **Enrichment**: Add calculated fields (total_amount, late_arrival_flag)
- **Deduplication**: Prevent duplicate event processing
- **Dead Letter Queue**: Route invalid records for analysis
- **Quality Metrics**: Track validation statistics

**Technology**: PySpark 3.5.7, Structured Streaming, JDBC

---

### 5. Retry Logic (`spark/utils/retry.py`)

**Purpose**: Fault tolerance utilities for handling transient failures.

**Features**:
- **Exponential Backoff**: Configurable initial delay and multiplier
- **Retry Decorator**: `@retry_with_backoff` for any function
- **Circuit Breaker**: Prevent cascade failures with states:
  - CLOSED: Normal operation
  - OPEN: Rejecting requests (after 5 failures)
  - HALF-OPEN: Testing recovery (after 30s timeout)
- **Retry Policy**: Configurable per-operation policies

---

### 6. Monitoring & Alerting (`spark/monitoring/metrics.py`)

**Purpose**: Real-time pipeline health monitoring via log files.

**Features**:
- **Batch Metrics**: Track records processed, validity rates, latency
- **Log-Based Alerts**: All alerts written to log files with clear level indicators
- **Thresholds**: Configurable alert triggers (validity < 95%, latency > 10s)
- **Consecutive Failures**: Escalate on repeated issues (ERROR after 3 failures)
- **Health Summary**: `get_summary()` returns pipeline status (HEALTHY/DEGRADED/NO_DATA)
- **Error Pattern Detection**: Alerts when single error type exceeds 10% of records

**Log Files**:
- Data Generator: `data/logs/data_generator_YYYYMMDD.log`
- Spark Streaming: `data/logs/spark_streaming_YYYYMMDD.log`

**Log Format**:
```
2026-01-29 10:30:20 | INFO     |  [Batch 0] COMPLETED SUCCESSFULLY
2026-01-29 10:30:20 | WARNING  |  Data Quality: ACCEPTABLE (92.0%)
2026-01-29 10:30:20 | ERROR    |  [Batch 5] FAILED - Connection refused
```

---

### 7. PostgreSQL Database (`docker/postgres/`)

**Purpose**: Stores processed events, invalid records, and quality metrics.

**Tables**:
| Table | Purpose |
|-------|---------|
| `ecommerce_events` | Valid processed events |
| `dead_letter_events` | Invalid events with error details |
| `data_quality_metrics` | Batch quality statistics |

**Views**:
- `v_hourly_event_summary` - Hourly aggregations
- `v_category_performance` - Category analytics
- `v_user_segment_analysis` - Segment breakdown
- `v_data_quality_dashboard` - Quality trends

**Constraints**:
- CHECK: valid event types
- CHECK: price >= 0
- CHECK: quantity >= 0
- UNIQUE: event_id (prevent duplicates)

---

### 8. Docker Compose (`docker/docker-compose.yml`)

**Purpose**: Orchestrates multi-container application.

**Services**:
- `postgres`: PostgreSQL 15 on port 5433
- `spark`: Spark 3.5.7 with streaming job

---

### 9. Test Suite (`tests/`)

**Purpose**: Comprehensive unit tests for all components.

| Test File | Tests | Coverage |
|-----------|-------|----------|
| `test_data_generator.py` | 40 | Event generation, business logic |
| `test_transformations.py` | 25 | Validation, enrichment |
| `test_config.py` | 17 | Configuration management |
| `test_retry.py` | 22 | Retry logic, circuit breaker |
| `test_monitoring.py` | 20 | Metrics, alerting |
| **Total** | **124+** | All components |

---

## Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              DATA FLOW                                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────────────────┐   │
│  │    Python    │    │  CSV Files   │    │       Apache Spark           │   │
│  │  Generator   │───▶│  data/input  │───▶│   Structured Streaming       │   │
│  │  100 evt/5s  │    │              │    │                              │   │
│  └──────────────┘    └──────────────┘    │  ┌─────────┐   ┌─────────┐   │   │
│                                          │  │ Validate │──▶│ Enrich  │   │   │
│                                          │  └─────────┘   └────┬────┘   │   │
│                                          │                     │        │   │
│                                          │         ┌───────────┴────┐   │   │
│                                          │         ▼                ▼   │   │
│                                          └─────────┬────────────────┬───┘   │
│                                                    │                │       │
│  ┌──────────────────────────────────────────────┐  │                │       │
│  │              PostgreSQL                       │  │                │       │
│  │  ┌─────────────────┐  ┌───────────────────┐  │◀─┘                │       │
│  │  │ ecommerce_events│  │ dead_letter_events│◀─────────────────────┘       │
│  │  │   (valid data)  │  │  (invalid data)   │  │                           │
│  │  └─────────────────┘  └───────────────────┘  │                           │
│  │  ┌─────────────────────────────────────────┐ │                           │
│  │  │      data_quality_metrics               │ │                           │
│  │  └─────────────────────────────────────────┘ │                           │
│  └──────────────────────────────────────────────┘                           │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Production Features

### Fault Tolerance

| Feature | Description |
|---------|-------------|
| Checkpointing | Spark checkpoints for exactly-once processing |
| Retry with Backoff | Automatic retry on transient failures |
| Circuit Breaker | Prevent cascade failures |
| Dead Letter Queue | Preserve invalid records for analysis |

### Observability

| Feature | Description |
|---------|-------------|
| Batch Metrics | Per-batch statistics with SUCCESS/FAILED status |
| Quality Metrics | Validation rate tracking with clear status indicators |
| File-Based Logging | All output saved to `data/logs/` directory |
| Searchable Logs | Easy to find errors with `Select-String -Pattern "ERROR"` |

### Data Quality

| Feature | Description |
|---------|-------------|
| Schema Validation | Enforce data types and constraints |
| Business Rules | Event-specific validation |
| Deduplication | Prevent duplicate processing |
| Late Arrival Handling | Watermark-based detection |

---

## Quick Start

```powershell
# 1. Start PostgreSQL
cd docker && docker compose up -d postgres

# 2. Start Spark streaming
docker compose up -d spark

# 3. Run data generator (new terminal)
cd data_generator && python data_generator.py

# 4. Query results
docker exec ecommerce_postgres psql -U spark -d ecommerce -c "SELECT event_type, COUNT(*) FROM ecommerce_events GROUP BY event_type;"
```

---

## References

- [README.md](../README.md) - Main documentation with Mermaid diagrams
- [User Guide](user_guide.md) - Detailed setup instructions
- [Test Cases](test_cases.md) - Test documentation
- [Performance Metrics](performance_metrics.md) - Benchmarks