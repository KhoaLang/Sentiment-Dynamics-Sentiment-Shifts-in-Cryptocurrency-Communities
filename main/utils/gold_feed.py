from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
from pyspark.sql import functions as F

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

gold_path = "s3a://delta-gold/sentiment_price"


df = (
    spark.read
        .format("delta")
        .load(gold_path)
        # .filter((F.col("anger")!=None) & (F.col("fear")!=None) & (F.col("joy")!=None))
)

# df.write.format("delta") \
#     .mode("overwrite") \
#     .save(gold_path)

# print(f"\n\n\n{df.count()} records")

df.printSchema()
df.show(20, truncate=False)
# .filter(F.col("price_return").isNotNull())
# .sort(desc("source_timestamp"))

spark.stop()
