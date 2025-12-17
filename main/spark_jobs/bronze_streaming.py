import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

spark = (
    SparkSession.builder
        .appName("bronze-kafka-to-delta")
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

bronze_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("subreddit", StringType(), True),
    StructField("raw_text", StringType(), True),
    StructField("created_utc", TimestampType(), True),
    StructField("ingested_at", TimestampType(), True),
    StructField("source_url", StringType(), True),
])

kafka_df = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "reddit.bronze")
        .option("startingOffsets", "earliest")
        .load()
)

parsed_df = (
    kafka_df
        .selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), bronze_schema).alias("data"))
        .select("data.*")
)

query = (
    parsed_df
        .writeStream
        .format("delta")
        .outputMode("append")
        .option(
            "checkpointLocation",
            "s3a://delta-bronze/_checkpoints/reddit_raw"
        )
        .start("s3a://delta-bronze/reddit_raw")
)

query.awaitTermination()
