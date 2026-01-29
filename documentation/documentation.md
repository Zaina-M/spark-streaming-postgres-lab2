# Docker Setup & Streaming Pipeline – Simple Conceptual Guide

## 1. Big Picture: What This Project Is Doing

This project simulates a **real‑time data pipeline**:

1. Fake e‑commerce events are generated as CSV files
2. Spark continuously watches a folder for new files
3. Spark processes the data and sends it to PostgreSQL
4. PostgreSQL stores the data permanently

Docker is used so that:

* Everyone runs the **same environment**
* Nothing depends on what is installed on a laptop
* The system can be started with **one command**

---

## 2. Docker & Docker Compose 

### Docker Image

* An **image** is a *ready‑made environment*
* Example: `postgres:15` already contains PostgreSQL 15 installed
* Example: `apache/spark:3.5.7-python3` already contains Spark + Python

You are **not installing software manually** — you are reusing trusted images.

### Docker Container

* A **container** is a *running instance* of an image
* Image = blueprint for one service
* Container = the actual running service

### Docker Compose

Docker Compose is a **blueprint / recipe / manual** that says:

> “I want these services, built from these images, connected this way, started in this order, with this data persisted.”

It does not store data. It only **describes** how things should run.

---

## 3. Why PostgreSQL and Spark Are in Docker

You are working with:

* PostgreSQL (database)
* Spark (stream processing engine)

Because they are **external systems**, you containerize them so:

* Versions are controlled
* Behavior is predictable
* Results are reproducible

If you also used MySQL, Kafka, Redis, etc., **each would be another service** in Docker Compose.

---

## 4. PostgreSQL Service (Why Each Part Exists)

### Image

```
image: postgres:15
```

* This locks PostgreSQL to version 15
* Prevents version mismatch issues
* Ensures everyone uses the same database behavior

### Environment Variables

```
POSTGRES_DB
POSTGRES_USER
POSTGRES_PASSWORD
```

* These are required by the Postgres image
* Avoid hardcoding credentials in code
* Can be replaced later with `.env` files or secrets

### Volumes

```
- postgres_data:/var/lib/postgresql/data
```

* This is **critical**
* It makes data persistent
* Even if the container restarts, data remains
* Data is deleted **only if the volume is deleted manually**

This is why your row count continues increasing over time.

### SQL Init Script

```
/docker-entrypoint-initdb.d/01_init.sql
```

* Runs **only the first time** the database is created
* Creates tables and indexes
* Will NOT rerun if the volume already exists

---

## 5. Spark Service (Why Each Part Exists)

### Image

```
apache/spark:3.5.7-python3
```

* Includes Spark + Python
* Matches the Spark version expected by your code

### `depends_on`

```
depends_on:
  - postgres
```

* Ensures Postgres container starts first
* Does NOT guarantee Postgres is fully ready
* But sufficient for lab‑level pipelines

### Volumes

```
- ../data:/data
```

Spark needs access to:

* Input CSV files
* Checkpoints

Mounting the folder allows:

* Streaming state to persist
* Exactly‑once style processing

### Command (`spark-submit`)

```
spark-submit --master local[*] ...
```

This means:

* Spark runs in **local mode**
* No Spark Master or Worker cluster needed
* Perfect for learning and labs

Spark starts automatically when the container starts.

---

## 6. Why There Is No Spark Master Service

You are using:

```
--master local[*]
```

This means:

* Spark runs as a single local process
* No cluster
* No separate Spark Master

So:

* You do NOT start Spark manually
* `docker compose up` is enough

---

## 7. Networking (How Containers Talk)

```
networks:
  ecommerce_net
```

Docker creates an **internal network** where:

* Containers can talk using service names
* `postgres` becomes a hostname
* Spark connects via `jdbc:postgresql://postgres:5432/...`

No IP addresses are needed.

---

## 8. Persistence & Why Data Is Not Lost

### PostgreSQL Volume

* Stores database data
* Survives restarts
* Count keeps increasing

### Spark Checkpoint Directory

* Stores streaming progress
* Prevents reprocessing old files
* Enables fault tolerance

Together, they make the pipeline **stateful and reliable**.

---

## 9. Can You Split This Into Multiple Docker Compose Files?

Yes.

Example:

* `docker-compose.generator.yml` → data generator only
* `docker-compose.pipeline.yml` → Spark + Postgres

Why you didn’t:

* Simpler for a lab
* One command to start everything

In real systems, splitting is common.


This is **exactly** what the lab is testing.

---

## 11. One‑Paragraph Summary (Memory Refresh)

Docker Compose acts as a blueprint that defines PostgreSQL and Spark as services, using fixed images to ensure consistent versions. PostgreSQL persists data through volumes, while Spark processes streaming CSV files using local mode and stores its progress via checkpoints. Environment variables prevent hardcoded secrets, networks allow services to communicate, and the entire pipeline can be started reliably with a single command.


## 1. What the Data Generator Is

The data generator is a **simulation tool**.

It pretends to be a real e-commerce system (like a website or mobile app) that continuously produces events such as:

* Product views
* Purchases

Since we do not have a real application producing live data, we **fake the stream** by repeatedly creating CSV files.

Each CSV file represents a small batch of new events that just happened.

---

## 2. Why the Data Generator Is Separate from Spark

The generator is intentionally **not part of Spark**.

This separation mirrors real-world systems:

* Data producers (websites, apps) are independent
* Data consumers (Spark) only react to incoming data

Benefits:

* Spark can crash and restart without stopping data creation
* The generator can stop while Spark continues waiting
* Each component has a single responsibility

This is good pipeline design.

---

## 3. Why CSV Files Are Used

CSV files are used because:

* They are simple and readable
* Spark Structured Streaming supports file-based streaming
* They are sufficient to demonstrate streaming concepts

Although real systems might use Kafka or message queues, **file streaming uses the same core ideas**:

* Incremental data arrival
* Tracking what has already been processed
* Fault tolerance

So CSV files act as a **stand-in for real-time event streams**.

---

## 4. Output Folder Design

The generator writes files to:

```
/data/input
```

Spark is configured to **monitor the same folder**.

Because Docker volumes are used:

* Both the generator and Spark see the same directory
* New files instantly become visible to Spark

This shared folder is what enables streaming.

---

## 5. Why Events Are Written in Batches

Each CSV file contains multiple events (e.g., 100 rows).

Reasons:

* Real systems batch events for efficiency
* Writing one event per file would be wasteful
* Spark processes data in micro-batches, not row-by-row

Each file corresponds to **one micro-batch** in Spark.

---

## 6. Why There Is a Time Delay Between Files

The generator pauses between file creations.

This simulates:

* Time passing
* Events occurring gradually
* Continuous data arrival

Without the delay, all data would appear at once and Spark would behave like a batch job instead of a streaming job.

---

## 7. Event Structure and Why It Looks This Way

Each event includes:

* `event_id`: a unique identifier
* `user_id`: identifies the user
* `event_type`: view or purchase
* `product_id`: identifies the product
* `price`: non-zero only for purchases
* `event_time`: time the event occurred

This structure is:

* Simple
* Realistic
* Sufficient to demonstrate streaming logic

---

## 8. Importance of `event_id`

The `event_id` is critical because:

* It uniquely identifies each event
* Spark can use it for deduplication
* It enables idempotent writes downstream

Without a unique event ID, exactly-once processing would not be possible.

---

## 9. Why `event_time` Is Written as a String

The generator writes timestamps as strings.

This reflects real-world pipelines where:

* Producers send raw data
* Consumers decide how to parse and interpret it

Spark later converts this string into a proper timestamp.

This separation of responsibilities is intentional.

---

## 10. Why Filenames Must Be Unique

Spark tracks which files it has already processed.

If a filename is reused:

* Spark assumes it has already been processed
* The file is ignored

By including timestamps and UUIDs in filenames:

* Every file is guaranteed to be new
* No data is accidentally skipped

---

## 11. Temporary File Pattern (Very Important)

Files are written in two steps:

1. Write to a temporary hidden file
2. Atomically rename it to the final filename

Why this is needed:

* Spark might detect a file while it is still being written
* This could cause partial or corrupted reads

This pattern is similar to downloading a file:

* While downloading, the file has a temporary name
* Once complete, it is renamed to the final name

Spark only sees the file **after it is fully written**.

---

## 12. Why Pandas Is Used

Pandas is used only to:

* Create small CSV files
* Keep the generator simple

The generator is not performance-critical.

In real systems, this role could be filled by:

* Backend services
* APIs
* Event producers

The concept remains the same.

---


## 13. One-Sentence Summary

The data generator simulates a real-time event source by periodically writing uniquely named, fully written CSV files to a shared folder, allowing Spark Structured Streaming to process new data incrementally and safely.

---


## 1. Overview: What this streaming job does

This Spark Structured Streaming job:

* Continuously watches a folder for new CSV files
* Treats arriving files as a **data stream**
* Cleans and validates the data
* Deduplicates events safely
* Writes the data incrementally into PostgreSQL
* Remembers progress so data is not reprocessed on restart

Even though the source is files, Spark processes them in a **streaming manner** using **micro-batches**.

---

## 2. Why this is called “streaming” (even though it’s files)

Spark Structured Streaming is streaming because:

* It uses `readStream`, not `read`
* It runs continuously
* It processes only **new data**
* It uses checkpoints to track progress

> Streaming here does **not** mean row-by-row.
> It means **incremental, continuous processing**.

---

## 3. Environment variables and configuration

```python
from dotenv import load_dotenv
import os
```

### Why environment variables are used

* Avoid hard-coding credentials
* Keep secrets out of GitHub
* Allow the same code to run locally, in Docker, or in production
This allows Spark to connect to PostgreSQL **securely**.


## 4. Spark Session

* Spark cannot run without a SparkSession
* `getOrCreate()` prevents duplicate Spark engines

```python
spark.sparkContext.setLogLevel("WARN")
```

### Why log level is set to WARN

Spark is very verbose by default.
Setting the level to `WARN`:

* Reduces noise
* Keeps important warnings and errors visible

---

## 5. Explicit Schema (Very Important)

```python
schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("user_id", IntegerType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("event_time", StringType(), True),
])
```

### Why schema is explicitly defined

* Streaming does **not** safely support schema inference
* Schema inference can change across files
* Explicit schema prevents runtime failures

### Why `event_time` starts as String

CSV files store everything as text.
It is converted later to a timestamp safely.

---

## 6. Reading the stream from files

```python
raw_df = (
    spark.readStream
    .schema(schema)
    .option("header", "true")
    .option("maxFilesPerTrigger", 1)
    .csv(INPUT_PATH)
)
```

### What Spark is doing here

* Watching the input directory continuously
* Reading **only new files**
* Treating each trigger as a micro-batch

### `header = true`

The first row of the CSV contains column names.

---

## 7. What `maxFilesPerTrigger = 1` really means

This does **not** mean:
 One row at a time
Real-time per record streaming

It means:

> **Spark will ingest at most ONE new file per micro-batch**

### Why this is useful

* Controls ingestion rate
* Prevents sudden data spikes
* Protects downstream systems like PostgreSQL
* Helps simulate real streaming behavior using files

> `maxFilesPerTrigger` controls **how much data Spark ingests**, not whether it is streaming.

---

## 8. Transformations (Cleaning and validation)

```python
clean_df = (
    raw_df
    .filter(col("event_type").isin("view", "purchase"))
```

### Why filter here

* Enforces business rules
* Removes invalid data early

---

```python
.withColumn("event_time", to_timestamp(col("event_time")))
```

### Why convert to timestamp

* Enables time-based operations
* Required for watermarking
* Allows Spark to reason about time correctly

---

## 9. Watermark (Very Important Concept)

```python
.withWatermark("event_time", "5 minutes")
```

### Why watermark is needed

Spark streaming keeps **state** in memory for operations like:

* Deduplication
* Aggregations

Without a watermark:

* Spark would have to remember **every event forever**
* Memory would grow endlessly
* The streaming job would eventually crash

### What watermark means (simple)

> “Spark will accept late data up to 5 minutes late, then forget older data.”

Watermark works on **event time**, not file arrival time.

---

## 10. Deduplication and watermark together

```python
.dropDuplicates(["event_id"])
```

### Why watermark is required for deduplication

* Deduplication is stateful
* Spark must know **when it can forget old IDs**
* Watermark defines that boundary

### Correct behavior

* Duplicate events within watermark window → dropped
* Very late duplicates → ignored
* Memory remains bounded

---

## 11. JDBC properties (Database writing behavior)

```python
jdbc_properties = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver",
    "batchsize": "5000",
    "isolationLevel": "READ_COMMITTED"
}
```

### Why `batchsize = 5000`

This controls **database insert batching**, NOT Spark ingestion.

* Spark may process 1 file at a time
* But writing rows one-by-one is slow
* Batch inserts reduce network and DB overhead

### Important clarification

| Setting              | Controls                 |
| -------------------- | ------------------------ |
| `maxFilesPerTrigger` | Spark ingestion          |
| `batchsize`          | Database insert batching |

They are **not related** and operate at different layers.

---

## 12. Writing with `foreachBatch`

```python
def write_to_postgres(batch_df, batch_id):
```

### Why `foreachBatch` is used

* Full control over how each micro-batch is written
* Easier error handling
* Works well with JDBC sinks

---

```python
if batch_df.rdd.isEmpty():
    return
```

### Why this check exists

* Avoids unnecessary DB calls
* Prevents empty commits

---

```python
print(f"[Batch {batch_id}] Writing {batch_df.count()} rows")
```

### What this print does

* Logs to the console
* Confirms the streaming job is running
* Helps with debugging and monitoring

This is **not file logging** — the actual data is written to PostgreSQL.

---

## 13. Starting the streaming query

```python
query = (
    clean_df.writeStream
    .foreachBatch(write_to_postgres)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .outputMode("append")
    .trigger(processingTime="10 seconds")
    .start()
)
```

### Checkpointing

* Stores progress metadata
* Prevents reprocessing after restart
* Enables fault tolerance

Without checkpoints:
 Duplicate data
 No recovery

---

### Trigger interval

```python
trigger(processingTime="10 seconds")
```

Spark checks for new files every 10 seconds.

This controls **how often Spark looks for new data**, not how fast data is generated.

---

## 14. Keeping Spark alive

```python
query.awaitTermination()
```

Without this:

* Spark exits
* Streaming stops

This keeps the job running indefinitely.

---

# Spark Structured Streaming to PostgreSQL — Documentation

## 1. Overview

This project implements a **file-based Spark Structured Streaming pipeline** that ingests ecommerce events from CSV files, cleans and deduplicates the data, and writes it reliably into a PostgreSQL database. The design follows **idempotent, fault-tolerant, and production-style streaming principles**, even though the source is file-based.

---

## 2. High-Level Architecture

**Flow:**

Data Generator → CSV files → Spark Structured Streaming → PostgreSQL

**Key components:**

* File source (CSV files generated over time)
* Spark Structured Streaming (ETL engine)
* PostgreSQL (final system of record)
* Docker (environment consistency)

---

## 3. Why File-Based Streaming Still Counts as Streaming

Although CSV files are static, Spark treats **newly arriving files** as a stream.

Streaming behavior is achieved using:

* `readStream`
* `maxFilesPerTrigger`
* `processingTime` triggers

This setup mimics real-time ingestion where data arrives **incrementally**, not all at once.

---

## 4. Spark Session Initialization

### Purpose

* Initializes Spark and connects PySpark to the JVM
* Enables Structured Streaming execution

### Key Configurations

* **JVM bridge:** Allows PySpark to communicate with Spark’s Java backend
* **Log level set to WARN:**

  * Spark is very verbose by default
  * `WARN` keeps only important system messages

This improves readability and debugging clarity.

---

## 5. Schema Definition (Why Explicit Schema Matters)

An explicit schema is defined instead of using `inferSchema`.

### Reasons:

* Prevents schema drift
* Improves performance
* Avoids inconsistencies across restarts
* Ensures Spark and PostgreSQL types align

The schema defines:

* Data types
* Nullability rules
* Expected structure of incoming data

This guarantees predictable downstream processing.

---

## 6. Reading the Stream (Extraction Phase)

### CSV Options Used

* `header = true` → First row contains column names
* `maxFilesPerTrigger = 1`

### What `maxFilesPerTrigger` Does

* Limits ingestion to **one file per micro-batch**
* Controls ingestion rate
* Mimics real-time arrival of data

Without this option, Spark would ingest all available files at once, behaving like batch processing.

> **Important:** `maxFilesPerTrigger` controls *file ingestion*, not row batching.

---

## 7. Transformation Phase (Cleaning & Business Rules)

### Applied Transformations

* Filter valid `event_type` values
* Convert `event_time` from string → timestamp
* Enforce business rules before database insertion

This ensures only **clean, valid data** moves forward.

---

## 8. Watermarking (Late Data Handling)

### Why Watermarks Are Critical

A watermark defines how long Spark should wait for **late-arriving data**.

Example:

* Watermark = 5 minutes
* Any data older than `(max event_time - 5 minutes)` is dropped

### What This Solves

* Prevents infinite state growth
* Enables safe deduplication
* Handles delayed file arrivals gracefully

### Important Clarification

Watermarking **does not reject files arbitrarily**.
It only affects:

* Late events
* Stateful operations like `dropDuplicates`

---

## 9. Deduplication Logic

Deduplication is performed using:

* `dropDuplicates` on `event_id`
* Combined with watermarking

### Why This Matters

* Spark may restart
* Files may be re-read
* Micro-batches may retry

This ensures **logical exactly-once processing** at the Spark level.

---

## 10. JDBC Configuration

### Batch Size (`batchsize = 5000`)

This controls how many **rows** are sent to PostgreSQL per write operation.

> **Important distinction:**

* `maxFilesPerTrigger` → files per micro-batch
* `batchsize` → rows per JDBC write

They are **not related**.

### Benefits

* Prevents database overload
* Improves write efficiency
* Reduces network overhead

---

## 11. Writing to PostgreSQL (Sink)

### `foreachBatch`

Used instead of built-in JDBC sink to:

* Control transactions
* Log batch progress
* Handle failures safely

Each micro-batch:

1. Checks data availability
2. Writes rows to PostgreSQL
3. Logs batch ID and row count

---

## 12. Checkpointing (Fault Tolerance)

Checkpointing stores:

* Streaming progress
* Offsets
* Batch metadata

### Why It’s Mandatory

* Prevents reprocessing old data
* Enables recovery after crashes
* Guarantees consistent streaming state

Without checkpointing, Spark treats every restart as a new stream.

---

## 13. Trigger Interval

### `processingTime = '10 seconds'`

This means:

* Spark checks for new data every 10 seconds
* If no new files exist, no batch is created

This controls **latency**, not throughput.

---

## 14. `awaitTermination()`

Prevents the Spark application from exiting immediately.

Without it:

* Spark would start
* Then stop instantly

This keeps the streaming job alive until explicitly terminated.

---

## 15. PostgreSQL Schema Design

### Table Guarantees

* `PRIMARY KEY (event_id)` → prevents duplicates
* `CHECK` constraint → enforces valid event types
* `NOT NULL` constraints → ensures data quality

### Indexes

* Index on `event_time` → fast time-based queries
* Index on `user_id` → fast user-level analytics

The database acts as the **final enforcement layer**.

---

## 16. End-to-End Idempotency

| Layer      | Responsibility            |
| ---------- | ------------------------- |
| Spark      | Deduplication + watermark |
| PostgreSQL | Primary key + constraints |

This dual-layer protection ensures:

* No duplicate records
* No invalid data
* Safe restarts and retries

---



