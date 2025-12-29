from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    window, avg, col, lag, corr
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, TimestampType
)

# =========================
# Spark Session
# =========================

spark = (
    SparkSession.builder
        .appName("bronze-crypto-price-ingest")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        # MinIO / S3A
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minio")
        .config("spark.hadoop.fs.s3a.secret.key", "minio123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")

        # Numeric timeouts ONLY
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
        .config("spark.hadoop.fs.s3a.socket.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")
        .config("fs.s3a.connection.timeout", "60000")
        .config("fs.s3a.socket.timeout", "60000")
        .config("fs.s3a.connection.establish.timeout", "60000")

        .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# =========================
# Paths
# =========================

BRONZE_PATH = "s3a://delta-bronze/crypto_prices_raw"
CHECKPOINT  = "s3a://delta-bronze/_checkpoints/crypto_prices"

# =========================
# Kafka Schema
# =========================

price_schema = StructType([
    StructField("asset", StringType(), False),
    StructField("price_usd", DoubleType(), True),
    StructField("market_cap", DoubleType(), True),
    StructField("volume_24h", DoubleType(), True),
    StructField("source_timestamp", StringType(), True),
    StructField("ingest_timestamp", StringType(), True),
    StructField("source", StringType(), True),
])

# =========================
# Read Kafka Stream
# =========================

raw_stream = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "crypto_prices")
        .option("startingOffsets", "latest")
        .load()
)

# =========================
# Parse & Normalize
# =========================

parsed = (
    raw_stream
        .selectExpr("CAST(value AS STRING) AS json")
        .select(F.from_json(col("json"), price_schema).alias("data"))
        .select("data.*")

        # Deterministic id (idempotency)
        .withColumn(
            "event_id",
            F.sha2(
                F.concat_ws(
                    "||",
                    col("asset"),
                    col("source_timestamp"),
                    col("price_usd").cast("string")
                ),
                256
            )
        )
)

# =========================
# Write Bronze (Append-only)
# =========================

(
    parsed.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT)
        .start(BRONZE_PATH)
        .awaitTermination()
)