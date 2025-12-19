from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, current_timestamp
from pyspark.sql.types import StringType
from transformers import pipeline
import torch

# =========================
# Spark Session
# =========================

spark = (
    SparkSession.builder
        .appName("silver-to-gold-sentiment-lm")
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

SILVER_PATH = "s3a://delta-silver/reddit_clean"
GOLD_PATH = "s3a://delta-gold/reddit_sentiment"
CHECKPOINT_PATH = "s3a://delta-gold/_checkpoints/reddit_sentiment"

# =========================
# Load lightweight sentiment LM
# =========================

device = 0 if torch.cuda.is_available() else -1

sentiment_model = pipeline("sentiment-analysis", model="siebert/sentiment-roberta-large-english", device=device)

# Broadcast model reference
bc_model = spark.sparkContext.broadcast(sentiment_model)

# =========================
# Sentiment UDF
# =========================

def predict_sentiment(text: str) -> str:
    if not text:
        return "neutral"

    model = bc_model.value
    # result = model(text[:512])[0]  # truncate safely
    result = model(text)[0]  # truncate safely
    return result["label"].lower()  # positive / negative


sentiment_udf = udf(predict_sentiment, StringType())

# =========================
# Read Silver as STREAM
# =========================

silver_df = (
    spark.readStream
        .format("delta")
        .load(SILVER_PATH)
)

# =========================
# Transform → Gold
# =========================

gold_df = (
    silver_df
        .filter(col("raw_text").isNotNull())
        .withColumn("sentiment", sentiment_udf(col("raw_text")))
        .withColumn("gold_processed_at", current_timestamp())
        .select(
            "event_id",
            "subreddit",
            "raw_text",
            "embedding",            # reused from Silver
            "sentiment",
            "gold_processed_at"
        )
)

# =========================
# Write Gold (streaming)
# =========================

query = (
    gold_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .start(GOLD_PATH)
)

query.awaitTermination()
