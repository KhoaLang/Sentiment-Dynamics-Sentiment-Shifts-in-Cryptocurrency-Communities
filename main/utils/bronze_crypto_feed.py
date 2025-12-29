import pyspark
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, current_timestamp, sha2
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, LongType
from datetime import datetime

spark = (
    pyspark.sql.SparkSession.builder.appName("MyApp")
    .appName("delta-bronze-minio")
    .master("local[*]")

    # Delta Lake
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    # MinIO / S3A (Spark side)
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minio")
    .config("spark.hadoop.fs.s3a.secret.key", "minio123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")

    # Spark → Hadoop bridge (numeric only)
    .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
    .config("spark.hadoop.fs.s3a.socket.timeout", "60000")
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")

    # Hadoop-side S3A (CRITICAL)
    .config("fs.s3a.connection.timeout", "60000")
    .config("fs.s3a.socket.timeout", "60000")
    .config("fs.s3a.connection.establish.timeout", "60000")

    # Neutralize Spark duration leakage
    .config("spark.network.timeout", "60000")
    .config("spark.executor.heartbeatInterval", "60000")

    .getOrCreate()
)


price_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("asset", StringType(), False),
    StructField("price_usd", DoubleType(), True),
    StructField("market_cap", DoubleType(), True),
    StructField("volume_24h", DoubleType(), True),
    StructField("source_timestamp", StringType(), True),
    StructField("ingest_timestamp", StringType(), True),
    StructField("source", StringType(), True),
])

spark.createDataFrame([], price_schema) \
    .write.format("delta") \
    .mode("overwrite") \
    .save("s3a://delta-bronze/crypto_prices_raw")

print("✅ Bronze table bootstrapped")
