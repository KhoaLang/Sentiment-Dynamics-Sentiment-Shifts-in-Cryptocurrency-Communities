from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import (
    window, avg, col, lag, corr
)
from pyspark.sql.window import Window

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


# =====================
# Read Silver
# =====================

sentiment = spark.read.format("delta").load(
    "s3a://delta-silver/reddit_clean"
)

price = spark.read.format("delta").load(
    "s3a://delta-silver/price_btc"
)

# =====================
# Window aggregation (5 min)
# =====================

sentiment_w = (
    sentiment
    .groupBy(window("processed_at", "5 minutes"))
    .agg(
        avg("fear").alias("avg_fear"),
        avg("anger").alias("avg_anger"),
        avg("joy").alias("avg_joy")
    )
    .withColumn("ts", col("window.start"))
    .drop("window")
)

price_w = (
    price
    .groupBy(window("timestamp", "5 minutes"))
    .agg(avg("price").alias("price"))
    .withColumn("ts", col("window.start"))
    .drop("window")
)

joined = sentiment_w.join(price_w, "ts", "inner")

# =====================
# Lag analysis
# =====================

lags = [1, 2, 3, 6]  # 5m, 10m, 15m, 30m

results = []

for l in lags:
    w = Window.orderBy("ts")

    lagged = joined.withColumn(
        f"price_lag_{l}",
        lag("price", l).over(w)
    )

    results.append(
        lagged.selectExpr(
            f"{l * 5} as lag_minutes",
            "corr(avg_sentiment, price_lag_{}) as sentiment_corr".format(l),
            "corr(avg_fear, price_lag_{}) as fear_corr".format(l),
            "corr(avg_anger, price_lag_{}) as anger_corr".format(l),
            "corr(avg_joy, price_lag_{}) as joy_corr".format(l)
        )
    )

final = results[0]
for r in results[1:]:
    final = final.unionByName(r)

# =====================
# Write Gold
# =====================

final.write.format("delta") \
    .mode("overwrite") \
    .save("s3a://delta-gold/sentiment_price_lag")