# User Guide: E-Commerce Streaming Pipeline

This comprehensive guide provides step-by-step instructions to set up, run, and monitor the e-commerce streaming pipeline.

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Project Structure](#project-structure)
3. [Configuration](#configuration)
4. [Step-by-Step Setup](#step-by-step-setup)
5. [Monitoring the Pipeline](#monitoring-the-pipeline)
6. [Running Tests](#running-tests)
7. [Troubleshooting](#troubleshooting)
8. [Advanced Usage](#advanced-usage)

---

## Prerequisites

### Required Software

| Software | Version | Purpose |
|----------|---------|---------|
| Docker Desktop | 4.0+ | Container orchestration |
| Python | 3.10+ | Data generator and tests |
| Git | 2.0+ | Version control |

### System Requirements

- **RAM**: 4GB+ allocated to Docker
- **Disk**: 2GB+ free space
- **Ports**: 5433 (PostgreSQL) must be available

### Verify Installation

```powershell
# Check Docker
docker --version
docker compose version

# Check Python
python --version
pip --version

# Check Git
git --version
```

---

## Project Structure

Before starting, ensure your project directory looks like this:

```
ecommerce-streaming-pipeline/
├── config/                         #  Configuration management
│   ├── __init__.py
│   └── settings.py                 # Centralized settings
├── data/
│   ├── checkpoints/                # Spark fault tolerance
│   ├── input/                      # Generated CSV files
│   └── logs/                       #  Application log files
│       ├── data_generator_YYYYMMDD.log
│       └── spark_streaming_YYYYMMDD.log
├── data_generator/
│   ├── data_generator.py           # Event generator
│   └── requirements.txt            # Python dependencies
├── docker/
│   ├── docker-compose.yml          # Service orchestration
│   ├── .env                        # Environment variables
│   └── postgres/
│       └── postgres_setup.sql      # Database schema
├── spark/
│   ├── spark_streaming_to_postgres.py  # Main streaming job
│   ├── schema/                     # Schema registry
│   │   └── registry.py
│   ├── utils/                      # Utilities
│   │   └── retry.py                # Retry & circuit breaker
│   └── monitoring/                 # Observability
│       └── metrics.py              # Metrics & log-based alerts
├── tests/                          #  124+ unit tests
│   ├── test_data_generator.py
│   ├── test_transformations.py
│   ├── test_config.py
│   ├── test_retry.py
│   └── test_monitoring.py
└── docs/
    ├── project_overview.md
    ├── user_guide.md               
    ├── detailed_explanation.md     
    └── test_cases.md
```

---

## Configuration

### Environment Variables

Create a `.env` file in the project root to customize settings:

```env
# Database Configuration
POSTGRES_HOST=localhost
POSTGRES_PORT=5433
POSTGRES_DB=ecommerce
POSTGRES_USER=spark
POSTGRES_PASSWORD=

# Data Generator Settings
BATCH_SIZE=100
INTERVAL_SECONDS=5.0
ANOMALY_RATE=0.02

# Streaming Settings
TRIGGER_INTERVAL=10 seconds
WATERMARK_DELAY=10 minutes

# Data Quality
MIN_VALIDITY_RATE=95.0
MAX_PRICE=10000.0

# Retry Settings
MAX_RETRIES=3
INITIAL_RETRY_DELAY=1.0
CIRCUIT_BREAKER_THRESHOLD=5

# Monitoring
LOG_LEVEL=INFO
```

### Default Configuration

If no `.env` file exists, defaults are used. See `config/settings.py` for all options.

---

## Step-by-Step Setup

### Step 1: Clone and Navigate

```powershell
# Clone the repository (if needed)
git clone https://github.com/Zaina-M/ecommerce-streaming-pipeline.git

# Navigate to project
cd ecommerce-streaming-pipeline
```

### Step 2: Install Python Dependencies

```powershell
cd data_generator
pip install -r requirements.txt
cd ..
```

### Step 3: Start PostgreSQL Database

```powershell
cd docker
docker compose up -d postgres
```

**What happens**:
- Starts PostgreSQL 15 container on port 5433
- Creates `ecommerce` database
- Creates tables: `ecommerce_events`, `dead_letter_events`, `data_quality_metrics`
- Creates analytics views

**Verify**:

```powershell
# Check container is running
docker compose ps

# Should show: ecommerce_postgres ... Up

# Test database connection
docker exec ecommerce_postgres pg_isready -U spark

# Should show: accepting connections

# List tables
docker exec ecommerce_postgres psql -U spark -d ecommerce -c "\dt"
```

### Step 4: Start Apache Spark Streaming

```powershell
docker compose up -d spark
```

**What happens**:
- Starts Spark 3.5.7 container
- Mounts `data/input/` directory for file monitoring
- Runs streaming job with 10-second trigger intervals
- Connects to PostgreSQL for data storage

**Verify**:

```powershell
# Check containers
docker compose ps

# Should show: ecommerce_postgres ... Up
#              spark_master ... Up

# View Spark logs
docker logs spark_master --tail 20
```

### Step 5: Run Data Generator

Open a **new terminal window**:

```powershell
cd ecommerce-streaming-pipeline/data_generator
python data_generator.py
```

**Expected output**:

```

2026-01-29 10:30:00 | INFO     |  E-COMMERCE EVENT GENERATOR STARTED
2026-01-29 10:30:00 | INFO     |  Output directory: data/input
2026-01-29 10:30:00 | INFO     |  Log file: data/logs/data_generator_20260129.log
2026-01-29 10:30:05 | INFO     |  [Batch 1] SUCCESS
2026-01-29 10:30:05 | INFO     |    File: events_20260129_103005_abc123.csv
2026-01-29 10:30:05 | INFO     |     Events: 100
```

**Note**: Keep this terminal open. The generator runs continuously. All logs are also saved to `data/logs/data_generator_YYYYMMDD.log`.

---

## Monitoring the Pipeline

### Check Event Counts

```powershell
# Total events processed
docker exec ecommerce_postgres psql -U spark -d ecommerce -c "
SELECT 
    (SELECT COUNT(*) FROM ecommerce_events) as valid_events,
    (SELECT COUNT(*) FROM dead_letter_events) as invalid_events;
"
```

### View Event Distribution

```powershell
docker exec ecommerce_postgres psql -U spark -d ecommerce -c "
SELECT event_type, COUNT(*) as count,
       ROUND(SUM(total_amount)::numeric, 2) as revenue
FROM ecommerce_events 
GROUP BY event_type 
ORDER BY count DESC;
"
```

### Check Data Quality

```powershell
docker exec ecommerce_postgres psql -U spark -d ecommerce -c "
SELECT * FROM v_data_quality_summary;
"
```

### View Category Performance

```powershell
docker exec ecommerce_postgres psql -U spark -d ecommerce -c "
SELECT * FROM v_category_performance;
"
```

### View Invalid Events

```powershell
docker exec ecommerce_postgres psql -U spark -d ecommerce -c "
SELECT validation_errors, COUNT(*) as count 
FROM dead_letter_events 
GROUP BY validation_errors 
ORDER BY count DESC;
"
```

### Monitor Spark Logs

```powershell
# View recent Docker logs
docker logs spark_master --tail 50

# Follow Docker logs in real-time
docker logs -f spark_master
```

### View Application Log Files

All pipeline logs are saved to the `data/logs/` directory:

```powershell
# List all log files
dir data\logs

# View data generator log
Get-Content data\logs\data_generator_20260129.log -Tail 30

# View Spark streaming log
Get-Content data\logs\spark_streaming_20260129.log -Tail 30

# Watch logs in real-time
Get-Content data\logs\data_generator_20260129.log -Tail 10 -Wait

# Search for errors across all logs
Select-String -Path "data\logs\*.log" -Pattern "ERROR"

# Search for warnings
Select-String -Path "data\logs\*.log" -Pattern "WARNING"

# Find failed batches
Select-String -Path "data\logs\*.log" -Pattern "FAILED"
```

**Log levels:**
- INFO - Normal operation, batch completed successfully
- WARNING - Potential issue (e.g., low data quality)
- ERROR - Operation failed, requires attention

### Interactive PostgreSQL Shell

```powershell
docker exec -it ecommerce_postgres psql -U spark -d ecommerce

# Then run SQL queries:
# \dt                    -- List tables
# \dv                    -- List views
# SELECT * FROM ...      -- Query data
# \q                     -- Exit
```

---

## Running Tests

### Run All Tests

```powershell
# From project root
cd ecommerce-streaming-pipeline

# Run all unit tests (fast, no Spark required)
python -m pytest tests/ -v --ignore=tests/test_schema_registry.py

# Expected: 124 tests passed
```

### Run Specific Test Files

```powershell
# Data generator tests
python -m pytest tests/test_data_generator.py -v

# Transformation tests
python -m pytest tests/test_transformations.py -v

# Configuration tests
python -m pytest tests/test_config.py -v

# Retry logic tests
python -m pytest tests/test_retry.py -v

# Monitoring tests
python -m pytest tests/test_monitoring.py -v
```

### Run with Coverage

```powershell
pip install pytest-cov
python -m pytest tests/ -v --cov=. --cov-report=html

# Open htmlcov/index.html for coverage report
```

---

## Troubleshooting

### PostgreSQL Connection Issues

**Symptom**: "Connection refused" or "role does not exist"

```powershell
# Check if container is running
docker compose ps

# Restart PostgreSQL
docker compose down postgres
docker compose up -d postgres

# Wait 10 seconds and retry
Start-Sleep -Seconds 10
```

### Spark Not Processing Files

**Symptom**: Events not appearing in database

```powershell
# Check Spark logs for errors
docker logs spark_master --tail 100

# Verify data files exist
dir data/input

# Restart Spark
docker compose restart spark
```

### Data Generator Errors

**Symptom**: Import errors or script crashes

```powershell
# Reinstall dependencies
pip install --upgrade -r data_generator/requirements.txt

# Verify Python version
python --version  # Should be 3.10+
```

### Port Conflicts

**Symptom**: "Port 5433 already in use"

```powershell
# Find process using port
netstat -ano | findstr 5433

# Stop conflicting service, or change port in docker-compose.yml
```

### Tests Failing

```powershell
# Run with verbose output
python -m pytest tests/test_data_generator.py -v --tb=long

# Check for missing dependencies
pip install pytest pandas pyspark
```

---

## Stopping the Pipeline

### Stop All Services

```powershell
# Stop data generator: Press Ctrl+C in that terminal

# Stop Docker services
cd docker
docker compose down
```

### Stop and Reset (Fresh Start)

```powershell
# Stop and remove all data
docker compose down -v

# Clear generated files
Remove-Item -Recurse -Force ../data/input/*
Remove-Item -Recurse -Force ../data/checkpoints/*
```

---

## Advanced Usage

### Custom Event Generation

Modify `data_generator/data_generator.py` to:
- Add new event types
- Change category distributions
- Adjust anomaly rates

### Schema Migrations

Use the schema registry for evolving data:

```python
from spark.schema.registry import SchemaRegistry

registry = SchemaRegistry()
df_migrated = registry.auto_migrate(df)  # Upgrade to latest schema
```



## Support

- **README**: See [README.md](../README.md) for Mermaid architecture diagrams
- **Overview**: See [project_overview.md](project_overview.md) for component details
- **Tests**: See [test_cases.md](test_cases.md) for test documentation

