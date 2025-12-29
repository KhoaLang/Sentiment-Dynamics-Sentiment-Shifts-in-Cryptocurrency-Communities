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

def truncate_table():
    import psycopg2

    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="gold_db",
        user="gold_user",
        password="gold_password"
    )

    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE gold_sentiment_price;")
    cur.close()
    conn.close()

truncate_table()


df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/gold_db") \
    .option("dbtable", "gold_sentiment_price") \
    .option("user", "gold_user") \
    .option("password", "gold_password") \
    .option("driver", "org.postgresql.Driver") \
    .option("truncate", "true") \
    .mode("append") \
    .save()


spark.stop()
