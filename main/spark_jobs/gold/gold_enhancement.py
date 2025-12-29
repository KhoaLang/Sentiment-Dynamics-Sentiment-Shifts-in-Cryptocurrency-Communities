from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, window, avg, expr, lag
)
from pyspark.sql import Window
# =========================
# Spark Session
# =========================

spark = (
    SparkSession.builder
        .appName("gold-sentiment-price-lag")
        .master("local[*]")

        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        # MinIO
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minio")
        .config("spark.hadoop.fs.s3a.secret.key", "minio123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")

        .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# =========================
# Paths
# =========================

SENTIMENT_SILVER = "s3a://delta-silver/reddit_clean"
PRICE_SILVER = "s3a://delta-silver/crypto_prices_enriched"
GOLD_PATH = "s3a://delta-gold/sentiment_price_lag"
CHECKPOINT_PATH = "s3a://delta-gold/_checkpoints/sentiment_price_lag"

# =========================
# Streaming Sentiment Input
# =========================

sentiment_stream = (
    spark.readStream
        .format("delta")
        # .option("skipChangeCommits", "true")
        .load(SENTIMENT_SILVER)
)

# =========================
# Gold Processing Logic
# =========================

def process_sentiment_batch(sentiment_df, batch_id: int):

    if sentiment_df.isEmpty():
        return

    # ----------------------------------
    # 1. Compute sentiment time bounds
    # ----------------------------------

    bounds = sentiment_df.selectExpr(
        "min(created_utc) as min_ts",
        "max(created_utc) as max_ts"
    ).collect()[0]

    min_ts = bounds.min_ts
    max_ts = bounds.max_ts

    if min_ts is None or max_ts is None:
        return

    # ----------------------------------
    # 2. Aggregate sentiment (5-minute windows)
    # ----------------------------------

    sentiment_agg = (
        sentiment_df
            .withWatermark("created_utc", "1 hour")
            .groupBy(
                window(col("created_utc"), "30 minutes")
            )
            .agg(
                avg("anger").alias("anger"),
                avg("fear").alias("fear"),
                avg("joy").alias("joy"),
                avg("sadness").alias("sadness"),
                avg("surprise").alias("surprise")
            )
    )

    # ----------------------------------
    # 3. Bounded read of crypto prices
    # ----------------------------------

    prices = (
        spark.read
            .format("delta")
            .load(PRICE_SILVER)
            .filter(
                (col("event_time") >= expr(f"TIMESTAMP '{min_ts}' - INTERVAL 30 MINUTES")) &
                (col("event_time") <= expr(f"TIMESTAMP '{max_ts}'"))
            )
    )

    # 5-minute price aggregation
    price_agg = (
        prices
            .groupBy(
                "asset",
                window(col("event_time"), "5 minutes")
            )
            .agg(
                avg("price_usd").alias("avg_price")
            )
    )

    # ----------------------------------
    # 4. Compute price returns (lag)
    # ----------------------------------

    w = Window.partitionBy("asset").orderBy("window.start")

    price_with_return = (
        price_agg
            .withColumn("prev_price", lag("avg_price", 1).over(w))
            .withColumn(
                "price_return",
                (col("avg_price") - col("prev_price")) / col("prev_price")
            )
    )

    # ----------------------------------
    # 5. Join sentiment ↔ price
    # ----------------------------------

    joined = (
        sentiment_agg.alias("s")
            .join(
                price_with_return.alias("p"),
                col("s.window.start") == col("p.window.start"),
                "inner"
            )
            .select(
                col("p.asset"),
                col("s.window.start").alias("window_start"),
                col("s.window.end").alias("window_end"),

                col("s.anger"),
                col("s.fear"),
                col("s.joy"),
                col("s.sadness"),
                col("s.surprise"),

                col("p.price_return")
            )
    )

    # ----------------------------------
    # 6. Write to Gold (append-only)
    # ----------------------------------

    (
        joined
            .dropDuplicates(
                ["asset", "window_start", "window_end"]
            )
            .write
            .format("delta")
            .mode("append")
            .save(GOLD_PATH)
    )

# =========================
# Start Gold Stream
# =========================

query = (
    sentiment_stream
        .writeStream
        .foreachBatch(process_sentiment_batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .start()
)

query.awaitTermination()
