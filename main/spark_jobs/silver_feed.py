from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
        .appName("read-delta-bronze-batch")
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

        .getOrCreate()
)

bronze_path = "s3a://delta-bronze/reddit_raw"

df = (
    spark.read
        .format("delta")
        .load(bronze_path)
)

print(f"\n\n\n{df.count()} records")

df.printSchema()
df.show(20, truncate=False)

spark.stop()
