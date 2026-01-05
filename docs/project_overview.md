# Ecommerce Streaming Pipeline Overview

## System Components

### 1. Data Generator (`data_generator/data_generator.py`)
- **Purpose**: Generates synthetic ecommerce event data for testing the streaming pipeline.
- **Functionality**:
  - Creates CSV files containing events like user views and purchases.
  - Each event includes: event_id, user_id, event_type, product_id, price, event_time.
  - Generates 100 events per file every 5 seconds.
  - Files are saved in `data/input/` with timestamps in filenames.
- **Technology**: Python with standard libraries (csv, datetime, random).

### 2. Apache Spark Streaming (`spark/spark_streaming_to_postgres.py`)
- **Purpose**: Processes streaming data from CSV files and loads it into PostgreSQL.
- **Functionality**:
  - Reads CSV files from `data/input/` using Spark Structured Streaming.
  - Applies schema enforcement and data transformations (e.g., timestamp conversion, deduplication).
  - Writes processed data to PostgreSQL using JDBC in append mode.
  - Uses checkpointing for fault tolerance.
  - Triggers processing every 10 seconds for continuous streaming.
- **Technology**: PySpark (Spark 3.5.7), PostgreSQL JDBC driver.

### 3. PostgreSQL Database (`docker/postgres/`)
- **Purpose**: Stores the processed ecommerce events.
- **Schema**:
  - Table: `ecommerce_events`
  - Columns: event_id (VARCHAR), user_id (INT), event_type (VARCHAR), product_id (INT), price (NUMERIC), event_time (TIMESTAMP)
  - Indexes on event_time and user_id for query performance.
- **Technology**: PostgreSQL 15, initialized via Docker with custom SQL script.

### 4. Docker Compose (`docker/docker-compose.yml`)
- **Purpose**: Orchestrates the multi-container application.
- **Services**:
  - `postgres`: Runs PostgreSQL with persistent volume for data.
  - `spark`: Runs Apache Spark with mounted scripts and data directories.
- **Networking**: Uses a custom bridge network for inter-container communication.
- **Volumes**: Shares data and scripts between host and containers.

## Data Flow

1. **Data Generation**:
   - Python script runs continuously, creating CSV files in `data/input/`.

2. **Data Ingestion**:
   - Spark streaming monitors `data/input/` for new CSV files.
   - Files are read with predefined schema and processed in micro-batches.

3. **Data Processing**:
   - Events are filtered, transformed (e.g., timestamp parsing), and deduplicated.
   - Watermarking is applied for late data handling.

4. **Data Storage**:
   - Processed events are appended to the `ecommerce_events` table in PostgreSQL via JDBC.

5. **Continuous Operation**:
   - The pipeline runs indefinitely, processing new files as they arrive.
   - Checkpointing ensures exactly-once processing and recovery from failures.

## Architecture Benefits

- **Scalability**: Spark can handle large volumes of data and scale horizontally.
- **Fault Tolerance**: Checkpointing and watermarking ensure reliable processing.
- **Real-time Processing**: Streaming allows near-real-time data ingestion.
- **Containerization**: Docker ensures consistent environments across development and deployment.

## Running the Pipeline

1. Start PostgreSQL: `docker compose up -d postgres`
2. Run data generator: `python data_generator/data_generator.py`
3. Start Spark: `docker compose up -d spark`
4. Monitor via logs and database queries.