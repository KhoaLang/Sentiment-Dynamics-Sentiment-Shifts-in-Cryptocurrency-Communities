from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col
from pyspark.sql.types import ArrayType, FloatType, StringType, TimestampType

# =========================
# Spark Session
# =========================

spark = (
    SparkSession.builder
        .appName("bronze-to-silver-reddit")
        .master("local[*]")

        # Delta
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

BRONZE_PATH = "s3a://delta-bronze/reddit_raw"
SILVER_PATH = "s3a://delta-silver/reddit_clean"
CHECKPOINT_PATH = "s3a://delta-silver/_checkpoints/reddit_clean"

# =========================
# Read Bronze as STREAM
# =========================

bronze_df = (
    spark.readStream
        .format("delta")
        .load(BRONZE_PATH)
)

# =========================
# Transform → Silver
# =========================

silver_df = (
    bronze_df
        .select(
            "event_id",
            "subreddit",
            "raw_text",
            "created_utc",
            "ingested_at"
        )
        # Ensure timestamps are proper
        .withColumn(
            "created_utc",
            col("created_utc").cast(TimestampType())
        )
        .withColumn(
            "ingested_at",
            col("ingested_at").cast(TimestampType())
        )
        .withColumn(
            "processed_at",
            current_timestamp()
        )
        # 🚨 Declare embedding columns NOW
        .withColumn("embedding", lit(None).cast(ArrayType(FloatType())))
        .withColumn("embedding_model", lit(None).cast(StringType()))
        .withColumn("embedding_at", lit(None).cast(TimestampType()))
        # Drop obvious garbage
        .filter(col("event_id").isNotNull())
        .filter(col("raw_text").isNotNull())
)

# =========================
# Write to Silver (stream)
# =========================

query = (
    silver_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .start(SILVER_PATH)
)

query.awaitTermination()
