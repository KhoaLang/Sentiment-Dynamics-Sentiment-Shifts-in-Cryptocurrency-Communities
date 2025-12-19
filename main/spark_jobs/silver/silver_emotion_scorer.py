from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch
import pandas as pd

spark = (
    SparkSession.builder
        .appName("silver-async-emotion")
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

SILVER_PATH = "s3a://delta-silver/reddit_clean"
CHECKPOINT_PATH = "s3a://delta-silver/_checkpoints/reddit_emotion"

MODEL_NAME = "bhadresh-savani/distilbert-base-uncased-emotion"
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
    pending = (
        batch_df
        .filter(col("fear").isNull())   # sentinel
        .select("event_id", "raw_text")
    )

    if pending.isEmpty():
        return

    pdf = pending.toPandas()
    scores = score_emotions(pdf["raw_text"].tolist())

    for i, label in enumerate(LABELS):
        pdf[label] = scores[:, i]

    updates_df = spark.createDataFrame(pdf)
    updates_df.createOrReplaceTempView("emotion_updates")

    spark.sql(f"""
        MERGE INTO delta.`{SILVER_PATH}` t
        USING emotion_updates s
        ON t.event_id = s.event_id
        WHEN MATCHED AND t.fear IS NULL
        THEN UPDATE SET
          t.anger = s.anger,
          t.disgust = s.disgust,
          t.fear = s.fear,
          t.joy = s.joy,
          t.sadness = s.sadness,
          t.surprise = s.surprise
    """)

(
    spark.readStream
        .format("delta")
        .load(SILVER_PATH)
        .writeStream
        .foreachBatch(emotion_and_merge)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .start()
        .awaitTermination()
)
