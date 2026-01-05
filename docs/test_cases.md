# Test Cases: Ecommerce Streaming Pipeline

This document outlines a manual test plan for the ecommerce streaming pipeline. Each test case includes steps to execute, expected outcomes, and space for actual results. Tests should be run in sequence where dependencies exist.

## Test Environment Setup

- **Hardware/Software**: Windows machine with Docker Desktop installed and running.
- **Data**: Start with empty `data/input/` and `data/checkpoints/` directories.
- **Database**: Ensure PostgreSQL is not running initially (run `docker compose down` if needed).

## Test Cases

### Test Case 1: PostgreSQL Database Initialization
**Description**: Verify that PostgreSQL starts correctly and initializes the database schema.

**Preconditions**: Docker is running.

**Steps**:
1. Navigate to `docker/` directory.
2. Run `docker compose up -d postgres`.
3. Wait 10 seconds for initialization.
4. Run `docker compose ps` to check status.
5. Connect to database: `docker compose exec postgres psql -U spark -d ecommerce -c "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"`

**Expected Result**:
- Container `ecommerce_postgres` is "Up".
- Output shows `ecommerce_events` table exists.
- No connection errors.

**Actual Result**:

**Pass/Fail**:

---

### Test Case 2: Data Generator Execution
**Description**: Verify the data generator creates CSV files correctly.

**Preconditions**: None.

**Steps**:
1. Navigate to `data_generator/` directory.
2. Run `python data_generator.py`.
3. Let it run for 10-15 seconds.
4. Interrupt with Ctrl+C.
5. Check `../data/input/` for new CSV files.
6. Open one CSV file and inspect the first few rows.

**Expected Result**:
- At least 2 CSV files created (e.g., `events_20260105_XXXXXX.csv`).
- Each file contains 100 rows.
- Columns: event_id, user_id, event_type, product_id, price, event_time.
- Data types: strings for IDs/types, numeric for price, ISO timestamp for event_time.
- No errors in script output (warnings ok).

**Actual Result**:

**Pass/Fail**:

---

### Test Case 3: Spark Streaming Startup
**Description**: Verify Spark container starts and initializes the streaming job.

**Preconditions**: PostgreSQL is running.

**Steps**:
1. Ensure PostgreSQL is up from Test Case 1.
2. Run `docker compose up -d spark`.
3. Wait 30 seconds.
4. Run `docker compose ps` to check status.
5. Check logs: `docker compose logs spark | tail -20`

**Expected Result**:
- Container `spark_master` is "Up".
- Logs show: "Script started", "Imports successful", "Spark session created", "Stream defined", "Query started".
- No fatal errors in logs.

**Actual Result**:

**Pass/Fail**:

---

### Test Case 4: Initial Data Processing
**Description**: Verify Spark processes existing CSV files on startup.

**Preconditions**: Tests 1-3 completed, CSV files exist from Test 2.

**Steps**:
1. Ensure Spark is running from Test Case 3.
2. Wait 30 seconds for processing.
3. Check database count: `docker compose exec postgres psql -U spark -d ecommerce -c "SELECT COUNT(*) FROM ecommerce_events;"`
4. Check Spark logs for processing messages: `docker compose logs spark | grep "Writing batch"`

**Expected Result**:
- Database count matches total events in CSV files (e.g., 200 if 2 files).
- Logs show "Writing batch 0 with 100 rows" (or similar for each file).
- No JDBC errors in logs.

**Actual Result**:

**Pass/Fail**:

---

### Test Case 5: Continuous Streaming
**Description**: Verify the pipeline processes new data continuously.

**Preconditions**: Tests 1-4 completed.

**Steps**:
1. Start data generator in a new terminal: `cd data_generator && python data_generator.py`
2. Let it run for 30 seconds (should create ~6 new files).
3. Check database count periodically: `docker compose exec postgres psql -U spark -d ecommerce -c "SELECT COUNT(*) FROM ecommerce_events;"`
4. Check Spark logs for new batches: `docker compose logs spark | grep "Writing batch" | tail -5`
5. Stop data generator with Ctrl+C.

**Expected Result**:
- Database count increases by ~600 (6 files × 100 events).
- Logs show multiple "Writing batch X with 100 rows" messages.
- Processing occurs every ~10 seconds.

**Actual Result**:

**Pass/Fail**:

---

### Test Case 6: Data Integrity and Deduplication
**Description**: Verify data is correctly transformed and deduplicated.

**Preconditions**: Database has data from previous tests.

**Steps**:
1. Query sample data: `docker compose exec postgres psql -U spark -d ecommerce -c "SELECT * FROM ecommerce_events LIMIT 10;"`
2. Check for duplicates: `docker compose exec postgres psql -U spark -d ecommerce -c "SELECT event_id, COUNT(*) FROM ecommerce_events GROUP BY event_id HAVING COUNT(*) > 1;"`
3. Verify data types: `docker compose exec postgres psql -U spark -d ecommerce -c "SELECT pg_typeof(event_id), pg_typeof(user_id), pg_typeof(price), pg_typeof(event_time) FROM ecommerce_events LIMIT 1;"`

**Expected Result**:
- event_id: VARCHAR(36), user_id: INTEGER, price: NUMERIC, event_time: TIMESTAMP.
- event_type is 'view' or 'purchase'.
- No duplicate event_ids.
- event_time is valid timestamp.

**Actual Result**:

**Pass/Fail**:

---

### Test Case 7: Error Handling - Database Unavailable
**Description**: Verify system behavior when PostgreSQL is unavailable.

**Preconditions**: Pipeline is running with data generator.

**Steps**:
1. Ensure pipeline is running (data generator and Spark).
2. Stop PostgreSQL: `docker compose stop postgres`
3. Let run for 30 seconds.
4. Check Spark logs: `docker compose logs spark | tail -10`
5. Restart PostgreSQL: `docker compose start postgres`
6. Check if processing resumes: `docker compose logs spark | tail -5`

**Expected Result**:
- Spark logs show JDBC connection errors while DB is down.
- Processing resumes after DB restart.
- No data loss (checkpointing recovers state).

**Actual Result**:

**Pass/Fail**:

---

### Test Case 8: Performance - High Volume
**Description**: Verify performance with increased data volume.

**Preconditions**: Clean setup.

**Steps**:
1. Modify data generator to create 1000 events per file (change `NUM_EVENTS = 1000`).
2. Run full pipeline for 1 minute.
3. Monitor processing time in Spark logs.
4. Check final database count.

**Expected Result**:
- Processing completes within reasonable time (< 30s per batch).
- Database count matches generated events.
- No out-of-memory errors.

**Actual Result**:

**Pass/Fail**:

---

### Test Case 9: Shutdown and Restart
**Description**: Verify graceful shutdown and state recovery.

**Preconditions**: Pipeline running with data in database.

**Steps**:
1. Stop all services: `docker compose down`
2. Restart: `docker compose up -d postgres && sleep 5 && docker compose up -d spark`
3. Check database still has data.
4. Start data generator and verify new processing.

**Expected Result**:
- Data persists across restarts.
- Spark resumes from checkpoint (no reprocessing of old data).
- New data is processed correctly.

**Actual Result**:

**Pass/Fail**:

---

### Test Case 10: Cleanup
**Description**: Verify proper cleanup of resources.

**Steps**:
1. Stop all services: `docker compose down -v`
2. Check directories: `data/input/`, `data/checkpoints/`
3. Run `docker compose ps` to confirm no containers running.

**Expected Result**:
- All containers stopped and removed.
- Data directories may be empty or contain only new files.
- No orphaned containers or volumes.

**Actual Result**:

**Pass/Fail**:

## Test Summary

**Total Tests**: 10

**Passed**: 

**Failed**: 

**Notes**: 

## Recommendations

- Run tests in order.
- Document any deviations from expected results.
- For automated testing, consider adding unit tests for data generator and Spark transformations.