# User Guide: Ecommerce Streaming Pipeline

This guide provides step-by-step instructions to set up and run the ecommerce streaming pipeline on my local machine.

## Prerequisites

- **Docker Desktop**: Ensure Docker is installed and running. The pipeline uses Docker Compose to manage containers.
- **Python 3.x**: Required for the data generator script.
- **Git**: To clone the repository (if not already done).
- **Windows PowerShell or Command Prompt**: For running commands.

## Project Structure

Before starting, ensure your project directory looks like this:

```
ecommerce-streaming-pipeline/
├── data/
│   ├── checkpoints/
│   └── input/
├── data_generator/
│   ├── data_generator.py
│   └── requirements.txt
├── docker/
│   ├── docker-compose.yml
│   └── postgres/
│       └── postgres_setup.sql
├── docs/
│   ├── project_overview.md
│   └── user_guide.md
└── spark/
    └── spark_streaming_to_postgres.py
```

## Step-by-Step Setup and Execution

### Step 1: Navigate to the Project Directory

Open a terminal (PowerShell or Command Prompt) and navigate to the project root:



### Step 2: Start PostgreSQL Database

The pipeline uses PostgreSQL to store processed ecommerce events. Start it using Docker Compose:

```
cd docker
docker compose up -d postgres
```

- This command starts the PostgreSQL container in detached mode.
- It initializes the `ecommerce` database with the `ecommerce_events` table.
- Wait a few seconds for the database to fully initialize.

**Verify**: Check if the container is running:

```
docker compose ps
```

You should see `ecommerce_postgres` with status "Up".

### Step 3: Run the Data Generator

The data generator creates synthetic CSV files with ecommerce events. Run it in a separate terminal:

```
cd data_generator
python data_generator.py
```

- This script generates 100 events per CSV file every 5 seconds.
- Files are saved in `../data/input/` with timestamps (e.g., `events_20260105_144552.csv`).
- Keep this terminal open; the script runs continuously.
- Note: You may see deprecation warnings for `datetime.utcnow()` – these don't affect functionality.

**Verify**: After a few seconds, check for new files in `data/input/`:

```
dir ..\data\input
```

### Step 4: Start Apache Spark Streaming

Now start the Spark service to process the streaming data:

```
cd docker
docker compose up -d spark
```

- This starts the Spark container, which runs the streaming job.
- The job reads CSV files from `data/input/`, processes them, and writes to PostgreSQL.
- Processing triggers every 10 seconds for continuous streaming.

**Verify**: Check container status:

```
docker compose ps
```

Both `ecommerce_postgres` and `spark_master` should be "Up".

### Step 5: Monitor the Pipeline

#### Check Data Ingestion
Query the PostgreSQL database to see ingested events:

```
docker compose exec postgres psql -U spark -d ecommerce -c "SELECT COUNT(*) FROM ecommerce_events;"
```

- Initially, it may show 0 or a small number.
- As the pipeline runs, the count should increase (e.g., +100 every ~10-15 seconds).

#### View Sample Data
See the actual events:

```
docker compose exec postgres psql -U spark -d ecommerce -c "SELECT * FROM ecommerce_events LIMIT 5;"
```

#### Monitor Spark Logs
Check for processing activity:

```
docker compose logs spark
```

- Look for messages like "Writing batch X with Y rows" indicating successful processing.
- Errors (if any) will appear here.

#### Monitor Data Files
Watch new CSV files being created:

```
dir data\input
```

## Troubleshooting

### PostgreSQL Connection Issues
- Ensure Docker is running and containers are up.
- If "role does not exist", restart PostgreSQL: `docker compose down postgres && docker compose up -d postgres`

### Spark Not Processing
- Check if data generator is running and creating files.
- Verify checkpoint directory: `data/checkpoints/ecommerce` should exist.
- Restart Spark: `docker compose restart spark`

### Data Not Appearing in Database
- Confirm JDBC URL in `spark/spark_streaming_to_postgres.py` matches container name.
- Check Spark logs for JDBC errors.
- Ensure PostgreSQL is accessible: `docker compose exec postgres psql -U spark -d ecommerce -c "SELECT 1;"`

### Port Conflicts
- PostgreSQL uses port 5432. If in use, stop other services or change the port in `docker-compose.yml`.

## Stopping the Pipeline

To stop all services:

```
cd docker
docker compose down
```

- This stops and removes containers but preserves data volumes.
- To remove volumes (reset database): `docker compose down -v`

## Advanced Usage

- **Custom Data**: Modify `data_generator/data_generator.py` to change event generation logic.
- **Scaling**: Adjust Spark configuration in `docker-compose.yml` for more resources.
- **Production**: Use Kubernetes or cloud services instead of Docker Compose.

## Support

If you encounter issues not covered here, check the logs and ensure all prerequisites are met. Refer to `docs/project_overview.md` for system details. 

Happy streaming!