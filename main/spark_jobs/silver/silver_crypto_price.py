from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as F

# =========================
# Spark Session
# =========================

spark = (
    SparkSession.builder
        .appName("silver-crypto-price-ingest")
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

BRONZE_PATH = "s3a://delta-bronze/crypto_prices_raw"
SILVER_PATH = "s3a://delta-silver/crypto_prices_enriched"

bronze_prices = (
    spark.readStream
        .format("delta")
        .load(BRONZE_PATH)
)


silver_prices = (
    bronze_prices
        .withColumn("event_time",
            F.to_timestamp(col("source_timestamp"))
        )
        .withColumn("minute",
            F.date_trunc("minute", col("event_time"))
        )
        .withColumn("event_date", F.to_date("event_time"))
        .dropDuplicates(["asset", "minute"])
)

query = (silver_prices.writeStream \
    .format("delta") \
    .partitionBy("asset", "event_date") \
    .option("checkpointLocation", "s3a://delta-silver/_checkpoints/price_silver") \
    .start(SILVER_PATH)
)
query.awaitTermination()