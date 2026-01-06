import os
from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType,
    DoubleType, TimestampType
)
from pyspark.sql.functions import col, to_timestamp


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


# Spark Session

spark = (
    SparkSession.builder
    .appName("EcommerceStreamingToPostgres")
    .getOrCreate()
)
# Set log level to WARN to reduce verbosity because spark is very chatty
spark.sparkContext.setLogLevel("WARN")


# Explicit schema (to avoid schema inference on streaming data)

schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("user_id", IntegerType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("event_time", StringType(), True),
])


# Read streaming CSV files

raw_df = (
    spark.readStream
    .schema(schema)
    .option("header", "true")
    .option("maxFilesPerTrigger", 1)
    .csv(INPUT_PATH)
)


# Transformations

clean_df = (
    raw_df
    .filter(col("event_type").isin("view", "purchase"))
    .withColumn("event_time", to_timestamp(col("event_time")))
    .withWatermark("event_time", "5 minutes")
    .dropDuplicates(["event_id"])   # logical deduplication
)


# JDBC properties 

jdbc_properties = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver",
    "batchsize": "5000",
    "isolationLevel": "READ_COMMITTED"
}

# --------------------------------------------------
# Write function (foreachBatch)
# --------------------------------------------------
def write_to_postgres(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    print(f"[Batch {batch_id}] Writing {batch_df.count()} rows")

    (
        batch_df
        .write
        .mode("append")
        .jdbc(
            url=JDBC_URL,
            table="ecommerce_events",
            properties=jdbc_properties
        )
    )

# Start streaming query

query = (
    clean_df.writeStream
    .foreachBatch(write_to_postgres)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .outputMode("append")
    .trigger(processingTime="10 seconds")
    .start()
)

query.awaitTermination()
