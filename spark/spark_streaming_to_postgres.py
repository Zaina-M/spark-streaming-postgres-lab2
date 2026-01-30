import os
import logging
from datetime import datetime
from dotenv import load_dotenv

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, to_timestamp, when, lit, current_timestamp, expr,
    abs as spark_abs, length, trim, lower, regexp_replace,
    input_file_name, monotonically_increasing_id, 
    year, month, dayofmonth, hour, dayofweek,
    count, sum as spark_sum, avg, min as spark_min, max as spark_max
)

# Import schema from registry instead of hardcoding
from schema import get_registry


# LOGGING CONFIGURATION

# Create logs directory if it doesn't exist
LOG_DIR = "/data/logs"
os.makedirs(LOG_DIR, exist_ok=True)

# Configure logging to both file AND console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        # Log to file (rotates daily based on filename)
        logging.FileHandler(
            os.path.join(LOG_DIR, f"spark_streaming_{datetime.now().strftime('%Y%m%d')}.log")
        ),
        # Also log to console (so we can see in docker logs)
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("EcommerceStreaming")


# Load environment variables

load_dotenv()

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

INPUT_PATH = os.getenv("INPUT_PATH", "/data/input")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "/data/checkpoints/ecommerce")

JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# Valid event types (extended)
VALID_EVENT_TYPES = ["view", "purchase", "add_to_cart", "remove_from_cart", "wishlist", "search"]

# Price limits for outlier detection
MAX_VALID_PRICE = 10000.0
MIN_VALID_PRICE = 0.0

# Data quality thresholds
NULL_RATE_THRESHOLD = 0.1  # Alert if > 10% nulls in a batch


# Spark Session

spark = (
    SparkSession.builder
    .appName("EcommerceStreamingToPostgres")
    .getOrCreate()
)
# Set log level to WARN to reduce verbosity because spark is very chatty
spark.sparkContext.setLogLevel("WARN")


# Get schema from registry (v2 is current default)
schema_registry = get_registry()
schema = schema_registry.get_schema()  # Uses current version (v2)
logger.info(f"Using schema version: {schema_registry.get_current_version()}")


# Read streaming CSV files

raw_df = (
    spark.readStream
    .schema(schema)
    .option("header", "true")
    .option("maxFilesPerTrigger", 1)
    .option("cleanSource", "off")  # Keep source files for debugging
    .csv(INPUT_PATH)
)

# DATA VALIDATION & QUALITY CHECKS


def validate_data(df: DataFrame) -> DataFrame:
   
    validated_df = df.withColumn(
        "validation_errors",
        when(col("event_id").isNull(), lit("null_event_id"))
        .when(col("product_id").isNull(), lit("null_product_id"))
        .when(col("event_type").isNull(), lit("null_event_type"))
        .when(~col("event_type").isin(VALID_EVENT_TYPES), lit("invalid_event_type"))
        # Business logic: Actions that require login
        .when(
            (col("event_type") == "purchase") & (col("user_id").isNull()),
            lit("purchase_without_user")
        )
        .when(
            (col("event_type") == "add_to_cart") & (col("user_id").isNull()),
            lit("cart_without_user")
        )
        .when(
            (col("event_type") == "wishlist") & (col("user_id").isNull()),
            lit("wishlist_without_user")
        )
        .when(
            (col("event_type") == "remove_from_cart") & (col("user_id").isNull()),
            lit("remove_cart_without_user")
        )
        # Price validation
        .when(col("price") < MIN_VALID_PRICE, lit("negative_price"))
        .when(col("price") > MAX_VALID_PRICE, lit("extreme_price"))
        .when(
            (col("event_type") == "purchase") & (col("price") <= 0),
            lit("purchase_zero_price")
        )
        .when(
            (col("event_type") != "purchase") & (col("event_type") != "add_to_cart") & (col("price") > 0),
            lit("non_purchase_has_price")
        )
        .otherwise(lit(None))
    )
    
    # Mark record validity
    validated_df = validated_df.withColumn(
        "is_valid",
        col("validation_errors").isNull()
    )
    
    return validated_df



# DATA TRANSFORMATIONS & ENRICHMENT


def transform_and_enrich(df: DataFrame) -> DataFrame:
    
    # Apply data cleaning, transformations, and enrichment.
    
    transformed_df = df
    
    # 1. Parse timestamp
    transformed_df = transformed_df.withColumn(
        "event_time", 
        to_timestamp(col("event_time"))
    )
    
    # 2. Add data lineage columns
    transformed_df = transformed_df.withColumn(
        "source_file",
        input_file_name()
    ).withColumn(
        "processed_at",
        current_timestamp()
    )
    
    # 3. Clean and normalize text fields
    transformed_df = transformed_df.withColumn(
        "event_type",
        lower(trim(col("event_type")))
    ).withColumn(
        "category",
        lower(trim(col("category")))
    ).withColumn(
        "search_query",
        lower(trim(regexp_replace(col("search_query"), r"[^\w\s]", "")))
    )
    
    # 4. Add time-based features for analytics
    transformed_df = transformed_df.withColumn(
        "event_year", year(col("event_time"))
    ).withColumn(
        "event_month", month(col("event_time"))
    ).withColumn(
        "event_day", dayofmonth(col("event_time"))
    ).withColumn(
        "event_hour", hour(col("event_time"))
    ).withColumn(
        "event_dayofweek", dayofweek(col("event_time"))
    )
    
    # 5. Calculate total_amount for purchases
    transformed_df = transformed_df.withColumn(
        "total_amount",
        when(
            col("event_type").isin("purchase", "add_to_cart"),
            col("price") * col("quantity")
        ).otherwise(lit(0.0))
    )
    
    # 6. Flag late-arriving data (for monitoring) - using INTERVAL for proper timestamp arithmetic
    transformed_df = transformed_df.withColumn(
        "is_late_arrival",
        col("event_time") < (current_timestamp() - expr("INTERVAL 5 MINUTES"))
    )
    
    # 7. Coalesce nulls with defaults
    transformed_df = transformed_df.withColumn(
        "quantity",
        when(col("quantity").isNull(), lit(0)).otherwise(col("quantity"))
    ).withColumn(
        "category",
        when(col("category").isNull(), lit("unknown")).otherwise(col("category"))
    ).withColumn(
        "user_segment",
        when(col("user_segment").isNull(), lit("unknown")).otherwise(col("user_segment"))
    ).withColumn(
        "search_query",
        when(col("search_query").isNull(), lit("")).otherwise(col("search_query"))
    ).withColumn(
        "session_id",
        when(col("session_id").isNull(), lit("unknown")).otherwise(col("session_id"))
    ).withColumn(
        "source_system",
        when(col("source_system").isNull(), lit("unknown")).otherwise(col("source_system"))
    )
    
    return transformed_df


# DATA QUALITY METRICS


def calculate_quality_metrics(df: DataFrame, batch_id: int) -> dict:
    
    # Calculate data quality metrics for monitoring and alerting.
    
    total_rows = df.count()
    if total_rows == 0:
        return {}
    
    # Count validation issues
    valid_count = df.filter(col("is_valid") == True).count()
    invalid_count = total_rows - valid_count
    
    # Count nulls per column
    null_counts = {}
    for column in ["user_id", "product_id", "event_type", "price", "event_time"]:
        null_count = df.filter(col(column).isNull()).count()
        null_counts[f"{column}_null_rate"] = null_count / total_rows
    
    # Count late arrivals
    late_count = df.filter(col("is_late_arrival") == True).count()
    
    # Event type distribution
    event_dist = df.groupBy("event_type").count().collect()
    event_distribution = {row["event_type"]: row["count"] for row in event_dist}
    
    metrics = {
        "batch_id": batch_id,
        "total_rows": total_rows,
        "valid_rows": valid_count,
        "invalid_rows": invalid_count,
        "validity_rate": valid_count / total_rows,
        "late_arrival_count": late_count,
        "late_arrival_rate": late_count / total_rows,
        "event_distribution": event_distribution,
        **null_counts
    }
    
    return metrics


def log_quality_metrics(metrics: dict):
    """Log quality metrics and raise alerts if thresholds are exceeded."""
    if not metrics:
        return
    
    validity_pct = metrics['validity_rate'] * 100
    
    logger.info("-" * 70)
    logger.info(f" [Batch {metrics['batch_id']}] DATA QUALITY REPORT")
    logger.info("-" * 70)
    logger.info(f"   Total rows:    {metrics['total_rows']}")
    logger.info(f"   Valid rows:    {metrics['valid_rows']} ({validity_pct:.1f}%)")
    logger.info(f"   Invalid rows:  {metrics['invalid_rows']}")
    logger.info(f"   Late arrivals: {metrics['late_arrival_count']} ({metrics['late_arrival_rate']*100:.1f}%)")
    logger.info(f"   Events:        {metrics['event_distribution']}")
    
    # Quality status
    if validity_pct >= 95:
        logger.info(f"    Data Quality: EXCELLENT ({validity_pct:.1f}%)")
    elif validity_pct >= 90:
        logger.info(f"    Data Quality: ACCEPTABLE ({validity_pct:.1f}%)")
    else:
        logger.warning(f"    Data Quality: POOR ({validity_pct:.1f}%) - NEEDS ATTENTION")
    
    # Alert on high null rates
    alerts = []
    for key, value in metrics.items():
        if key.endswith("_null_rate") and value > NULL_RATE_THRESHOLD:
            field = key.replace('_null_rate', '')
            alerts.append(f"{field}: {value*100:.1f}% null")
            logger.warning(f"    ALERT: High null rate for {field}: {value*100:.1f}%")
    
    # Alert on low validity rate
    if metrics["validity_rate"] < 0.9:
        logger.warning(f"    ALERT: Low validity rate: {validity_pct:.1f}% (threshold: 90%)")
    
    logger.info("-" * 70)


# Transformations - Apply validation and enrichment

validated_df = validate_data(raw_df)
enriched_df = transform_and_enrich(validated_df)

# Apply watermark and deduplication on valid data
clean_df = (
    enriched_df
    .filter(col("is_valid") == True)
    .withWatermark("event_time", "10 minutes")
    .dropDuplicates(["event_id"])
)

# Separate invalid records for dead-letter processing
dead_letter_df = enriched_df.filter(col("is_valid") == False)


# JDBC properties 

jdbc_properties = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver",
    "batchsize": "5000",
    "isolationLevel": "READ_COMMITTED"
}

# Columns to write to main events table
EVENTS_TABLE_COLUMNS = [
    "event_id", "user_id", "session_id", "event_type", "product_id",
    "category", "price", "quantity", "total_amount", "user_segment",
    "search_query", "event_time", "event_year", "event_month", "event_day",
    "event_hour", "event_dayofweek", "is_late_arrival", "source_file",
    "source_system", "processed_at"
]

# Columns for dead-letter table
DEAD_LETTER_COLUMNS = [
    "event_id", "user_id", "event_type", "product_id", "price",
    "event_time", "validation_errors", "source_file", "processed_at"
]


# Write function (foreachBatch) - Enhanced with quality metrics

def write_to_postgres(batch_df, batch_id):
    # Write each micro-batch to PostgreSQL with quality metrics logging.
    if batch_df.rdd.isEmpty():
        logger.debug(f"[Batch {batch_id}] Empty batch, skipping")
        return
    
    start_time = datetime.now()
    
    try:
        logger.info("=" * 70)
        logger.info(f" [Batch {batch_id}] PROCESSING STARTED")
        logger.info("=" * 70)
        
        # Calculate and log quality metrics
        metrics = calculate_quality_metrics(batch_df, batch_id)
        log_quality_metrics(metrics)
        
        # Filter valid records
        valid_df = batch_df.filter(col("is_valid") == True)
        invalid_df = batch_df.filter(col("is_valid") == False)
        
        valid_count = valid_df.count()
        invalid_count = invalid_df.count()
        
        # Write valid records to main table
        if valid_count > 0:
            logger.info(f"    Writing {valid_count} valid rows to ecommerce_events...")
            (
                valid_df
                .select(EVENTS_TABLE_COLUMNS)
                .write
                .mode("append")
                .jdbc(
                    url=JDBC_URL,
                    table="ecommerce_events",
                    properties=jdbc_properties
                )
            )
            logger.info(f"  ecommerce_events: SUCCESS ({valid_count} rows)")
        
        # Write invalid records to dead-letter table
        if invalid_count > 0:
            logger.info(f"  Writing {invalid_count} invalid rows to dead_letter_events...")
            (
                invalid_df
                .select(DEAD_LETTER_COLUMNS)
                .write
                .mode("append")
                .jdbc(
                    url=JDBC_URL,
                    table="dead_letter_events",
                    properties=jdbc_properties
                )
            )
            logger.info(f"  dead_letter_events: SUCCESS ({invalid_count} rows)")
        
        # Write quality metrics to monitoring table
        if metrics:
            write_quality_metrics_to_db(metrics)
            logger.info(f"  data_quality_metrics: SUCCESS")
        
        # Calculate processing time
        processing_time = (datetime.now() - start_time).total_seconds()
        
        logger.info("=" * 70)
        logger.info(f" [Batch {batch_id}] COMPLETED SUCCESSFULLY")
        logger.info(f"   Processing time: {processing_time:.2f} seconds")
        logger.info(f"  Valid: {valid_count} | Invalid: {invalid_count} | Total: {valid_count + invalid_count}")
        logger.info("=" * 70)
        
    except Exception as e:
        processing_time = (datetime.now() - start_time).total_seconds()
        logger.error("=" * 70)
        logger.error(f" [Batch {batch_id}] FAILED")
        logger.error(f"   Failed after: {processing_time:.2f} seconds")
        logger.error(f"   Error: {str(e)}")
        logger.error("=" * 70)
        # In production, implement retry logic or write to error queue
        raise


def write_quality_metrics_to_db(metrics: dict):
    # Write quality metrics to monitoring table for dashboards.
    try:
        from pyspark.sql import Row
        
        metrics_row = Row(
            batch_id=metrics["batch_id"],
            total_rows=metrics["total_rows"],
            valid_rows=metrics["valid_rows"],
            invalid_rows=metrics["invalid_rows"],
            validity_rate=float(metrics["validity_rate"]),
            late_arrival_count=metrics["late_arrival_count"],
            recorded_at=datetime.utcnow()
        )
        
        metrics_df = spark.createDataFrame([metrics_row])
        
        (
            metrics_df
            .write
            .mode("append")
            .jdbc(
                url=JDBC_URL,
                table="data_quality_metrics",
                properties=jdbc_properties
            )
        )
    except Exception as e:
        # Don't fail the batch if metrics write fails
        logger.warning(f"Failed to write quality metrics: {str(e)}")

# Start streaming query

logger.info("=" * 70)
logger.info(" STARTING ECOMMERCE STREAMING PIPELINE")
logger.info("=" * 70)
logger.info(f" Input path: {INPUT_PATH}")
logger.info(f" Checkpoint path: {CHECKPOINT_PATH}")
logger.info(f" PostgreSQL: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
logger.info(f" Valid event types: {VALID_EVENT_TYPES}")
logger.info(f" Log file: {LOG_DIR}/spark_streaming_*.log")
logger.info("=" * 70)

# Test PostgreSQL connection before starting
try:
    logger.info("Testing PostgreSQL connection...")
    test_df = spark.read.jdbc(
        url=JDBC_URL,
        table="(SELECT 1 as test) as test_query",
        properties=jdbc_properties
    )
    test_df.collect()
    logger.info(" PostgreSQL connection: SUCCESS")
except Exception as e:
    logger.error(f" PostgreSQL connection: FAILED - {str(e)}")
    logger.error(" Pipeline cannot start without database connection. Exiting.")
    raise SystemExit(1)

logger.info("=" * 70)
logger.info(" PIPELINE STARTED SUCCESSFULLY - Waiting for data...")
logger.info("=" * 70)

query = (
    enriched_df.writeStream
    .foreachBatch(write_to_postgres)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .outputMode("append")
    .trigger(processingTime="10 seconds")
    .start()
)

query.awaitTermination()
