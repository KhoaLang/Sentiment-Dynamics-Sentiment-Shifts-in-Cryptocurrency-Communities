from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col
from pyspark.sql.types import ArrayType, DoubleType, FloatType, StringType, TimestampType
from delta.tables import DeltaTable
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch
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
# Sentiment analysis
# =========================

MODEL_NAME = "j-hartmann/emotion-english-distilroberta-base"
tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
model = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME)
model.eval()

LABELS = ["anger", "disgust", "fear", "joy", "sadness", "surprise"]

def score_emotions(texts):
    inputs = tokenizer(texts, return_tensors="pt", truncation=True, padding=True)
    with torch.no_grad():
        logits = model(**inputs).logits
    probs = torch.softmax(logits, dim=1).numpy()
    return probs

def emotion_and_merge(batch_df, batch_id):
    silver_df = (
        batch_df
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
            # # 🚨 Declare embedding columns NOW
            # .withColumn("embedding", lit(None).cast(ArrayType(FloatType())))
            # .withColumn("embedding_model", lit(None).cast(StringType()))
            # .withColumn("embedding_at", lit(None).cast(TimestampType()))

            # 🚨 Declare sentiment columns NOW
            .withColumn("anger", lit(None).cast(DoubleType()))
            .withColumn("joy", lit(None).cast(DoubleType()))
            .withColumn("disgust", lit(None).cast(DoubleType()))
            .withColumn("fear", lit(None).cast(DoubleType()))
            .withColumn("surprise", lit(None).cast(DoubleType()))
            .withColumn("sadness", lit(None).cast(DoubleType()))

            # Drop obvious garbage
            .filter(col("event_id").isNotNull())
            .filter(col("raw_text").isNotNull())
    )

    pdf = silver_df.toPandas()
    scores = score_emotions(pdf["raw_text"].tolist())
    print("Sentiment analysis: ", scores)
    for i, label in enumerate(LABELS):
        pdf[label] = scores[:, i]

    updates_df = spark.createDataFrame(pdf)
    updates_df.createOrReplaceTempView("emotion_updates")

    # spark.sql(f"""
    #     MERGE INTO delta.`{SILVER_PATH}` t
    #     USING emotion_updates s
    #     ON t.event_id = s.event_id
    #     WHEN MATCHED AND t.fear IS NULL
    #     THEN UPDATE SET
    #       t.anger = s.anger,
    #       t.disgust = s.disgust,
    #       t.fear = s.fear,
    #       t.joy = s.joy,
    #       t.sadness = s.sadness,
    #       t.surprise = s.surprise
    # """)

    spark.sql(f"""
        MERGE INTO delta.`{SILVER_PATH}` t
        USING emotion_updates s
        ON t.event_id = s.event_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


# query = (
#     silver_df.writeStream
#         .foreachBatch(emotion_and_merge)
#         .format("delta")
#         .option("checkpointLocation", CHECKPOINT_PATH)
#         .start()
# )

(
    spark.readStream
        .format("delta")
        .load(BRONZE_PATH)
        .writeStream
        .foreachBatch(emotion_and_merge)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .start()
        .awaitTermination()
)
