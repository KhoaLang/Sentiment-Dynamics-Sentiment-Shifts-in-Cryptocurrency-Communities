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


print("=========== Configs ===========")
conf = spark.sparkContext.getConf().getAll()
for k, v in conf:
    if "s3a" in k.lower() or "timeout" in k.lower():
        print(k, v)


bronze_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("subreddit", StringType(), True),
    StructField("raw_text", StringType(), True),
    StructField("created_utc", TimestampType(), True),
    StructField("ingested_at", TimestampType(), True),
])

sample_data = [
    (
        "t3_abc123",
        "dataengineering",
        "Delta Lake streaming from Kafka is awesome",
        datetime(2025, 12, 13, 18, 32, 10),
        datetime(2025, 12, 13, 18, 32, 45)
    ),
    (
        "t3_def456",
        "machinelearning",
        "Anyone using LLMs for sentiment analysis?",
        datetime(2025, 12, 13, 18, 35, 2),
        datetime(2025, 12, 13, 13, 35, 40)
    ),
    (
        "t3_ghi789",
        "bigdata",
        "Spark Structured Streaming vs Flink",
        datetime(2025, 12, 13, 18, 40, 11),
        datetime(2025, 12, 13, 18, 40, 55)
    )
]

df = spark.createDataFrame(sample_data, schema=bronze_schema)

# Write to delta table
# df.show(truncate=False)
df.write.format("delta")\
    .mode("append")\
    .save("s3a://delta-bronze/reddit_raw")

# spark.range(1).write.format("delta") \
#   .save("s3a://delta-bronze/reddit_raw")
