# Ecommerce Streaming Pipeline

A real-time data pipeline for processing ecommerce events using Apache Spark Streaming and PostgreSQL.

## Overview

This project demonstrates a complete streaming ETL pipeline that:
- Generates synthetic ecommerce event data
- Processes events in real-time using Apache Spark
- Stores transformed data in PostgreSQL for analytics

## Architecture

```
Data Generator → CSV Files → Spark Streaming → PostgreSQL
     ↓              ↓              ↓              ↓
  Python Script   File System   Transformations  Analytics DB
```

### Components

- **Data Generator**: Python script creating CSV files with ecommerce events
- **Apache Spark**: Processes streaming data with structured streaming
- **PostgreSQL**: Stores processed events with optimized schema
- **Docker Compose**: Orchestrates all services

## Quick Start

### Prerequisites

- Docker Desktop installed and running
- Python 3.x (for data generator)
- Windows/Linux/Mac with sufficient RAM (4GB+ recommended)

### Running the Pipeline

1. **Clone and navigate**:
   ```bash
   cd ecommerce-streaming-pipeline
   ```

2. **Start PostgreSQL**:
   ```bash
   cd docker
   docker compose up -d postgres
   ```

3. **Run data generator** (in separate terminal):
   ```bash
   cd data_generator
   python data_generator.py
   ```

4. **Start Spark streaming**:
   ```bash
   docker compose up -d spark
   ```

5. **Monitor progress**:
   - Check data: `docker compose exec postgres psql -U spark -d ecommerce -c "SELECT COUNT(*) FROM ecommerce_events;"`
   - View logs: `docker compose logs spark`

## Data Flow

1. **Data Generation**: Python script creates CSV files every 5 seconds (100 events each)
2. **File Ingestion**: Spark monitors `data/input/` for new CSV files
3. **Processing**: Events are parsed, transformed, and deduplicated
4. **Storage**: Processed data is inserted into PostgreSQL via JDBC

## Configuration

### Environment Variables

- `POSTGRES_DB`: Database name (default: ecommerce)
- `POSTGRES_USER`: Database user (default: spark)
- `POSTGRES_PASSWORD`: Database password (default: spark123)

### Spark Settings

- **Batch Interval**: 10 seconds
- **Checkpoint Location**: `data/checkpoints/`
- **JDBC Batch Size**: 5000

## API Reference

### Data Schema

```sql
CREATE TABLE ecommerce_events (
    event_id VARCHAR(36) PRIMARY KEY,
    user_id INT NOT NULL,
    event_type VARCHAR(20) NOT NULL,
    product_id INT NOT NULL,
    price NUMERIC(10,2) NOT NULL,
    event_time TIMESTAMP NOT NULL
);
```

### Sample Event

```json
{
  "event_id": "550e8400-e29b-41d4-a716-446655440000",
  "user_id": 12345,
  "event_type": "purchase",
  "product_id": 67890,
  "price": 99.99,
  "event_time": "2026-01-05T14:47:56.000Z"
}
```

## Monitoring

### Health Checks

- **PostgreSQL**: `docker compose exec postgres pg_isready`
- **Spark**: Check logs for "Query started" message
- **Data Generator**: Monitor `data/input/` for new files

### Performance Metrics

- **Throughput**: ~600 events/minute
- **Latency**: <5 seconds end-to-end
- **Resource Usage**: 1-2GB RAM total

## Development

### Project Structure

```
ecommerce-streaming-pipeline/
├── data/
│   ├── checkpoints/          # Spark checkpoints
│   └── input/               # CSV files
├── data_generator/
│   ├── data_generator.py    # Event generation script
│   └── requirements.txt     # Python dependencies
├── docker/
│   ├── docker-compose.yml   # Service orchestration
│   └── postgres/
│       └── postgres_setup.sql # DB initialization
├── docs/                    # Documentation
└── spark/
    └── spark_streaming_to_postgres.py # Streaming job
```

### Modifying the Pipeline

- **Change data format**: Edit `data_generator.py`
- **Add transformations**: Modify `spark_streaming_to_postgres.py`
- **Update schema**: Change `postgres_setup.sql`

## Troubleshooting

### Common Issues

1. **PostgreSQL connection fails**
   - Ensure containers are running: `docker compose ps`
   - Check logs: `docker compose logs postgres`

2. **Spark not processing data**
   - Verify data generator is running
   - Check file permissions on `data/` directory
   - Review Spark logs for errors

3. **Data not appearing in database**
   - Confirm JDBC URL in Spark script
   - Check PostgreSQL user permissions
   - Verify table exists

### Logs and Debugging

```bash
# View all logs
docker compose logs

# Follow Spark logs
docker compose logs -f spark

# Access PostgreSQL shell
docker compose exec postgres psql -U spark -d ecommerce
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make changes with tests
4. Submit a pull request

## License

This project is open source. See LICENSE file for details.

## Documentation

- [Project Overview](docs/project_overview.md)
- [User Guide](docs/user_guide.md)
- [Test Cases](docs/test_cases.md)
- [Performance Metrics](docs/performance_metrics.md)

## Support

For issues or questions:
- Check the troubleshooting section
- Review the documentation
- Open an issue on GitHub