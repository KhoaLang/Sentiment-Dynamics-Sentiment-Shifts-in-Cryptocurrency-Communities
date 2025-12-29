from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, window, avg, stddev, log, lag,
    to_date, min as min_, max as max_, when, lit, date_trunc
)
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
        .appName("gold-sentiment-batch")

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

SILVER_PATH = "s3a://delta-silver/reddit_clean"
PRICE_PATH  = "s3a://delta-silver/crypto_prices_enriched"
GOLD_PATH   = "s3a://delta-gold/sentiment_price"

# =========================
# Read Silver (BATCH)
# =========================

silver = (
    spark.read
        .format("delta")
        .load(SILVER_PATH)
        .filter(col("fear").isNotNull())   # ensure scored
)

date_bounds = (
    silver
        .select(to_date("created_utc").alias("d"))
        .agg(
            min_("d").alias("min_d"),
            max_("d").alias("max_d")
        )
        .collect()[0]
)

min_date = date_bounds["min_d"]
max_date = date_bounds["max_d"]


# =========================
# Sentiment Aggregation
# =========================

sentiment_1h = (
    silver
        .withColumn(
            "asset",
            when(col("subreddit") == "CryptoCurrency", "BTC")
            .when(col("subreddit") == "CryptoMarkets", "ETH")
            .otherwise(None)
        )
        .filter(col("asset").isNotNull())
        .groupBy(
            window(col("created_utc"), "1 hour"),
            col("asset")   # map BTC / ETH upstream
        )
        .agg(
            avg("anger").alias("anger"),
            avg("fear").alias("fear"),
            avg("joy").alias("joy"),
            avg("sadness").alias("sadness"),
            avg("surprise").alias("surprise"),
            stddev("fear").alias("fear_volatility")
        )
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .drop("window")
)

# =========================
# Fear Index (Market Stress)
# =========================

sentiment_enriched = (
    sentiment_1h
        .withColumn(
            "fear_index",
            col("fear") + col("sadness") - col("joy")
        )
)

# =========================
# Read Price (BATCH)
# =========================

price = (
    spark.read
        .format("delta")
        .load(PRICE_PATH)
        .withColumn(
            "window_start",
            date_trunc("hour", col("event_time"))
        )
        .filter(
            (col("event_date") >= lit(min_date)) &
            (col("event_date") <= lit(max_date))
        )
        .filter(col("price_usd").isNotNull())
)

# =========================
# Price Return (log-return)
# =========================

price_window = Window.partitionBy("asset").orderBy("window_start")

price_returns = (
    price
        .withColumn(
            "price_return",
            log(col("price_usd")) - log(lag("price_usd").over(price_window))
        )
        .select("asset", "window_start", "price_return")
)

# =========================
# Join Sentiment ↔ Price
# =========================

sentiment_enriched.sort(col("window_start").desc()).show(20, truncate=False)
price_returns.sort(col("window_start").desc()).show(5, truncate=False)


gold = (
    sentiment_enriched
        .join(
            price_returns,
            on=["asset", "window_start"],
            how="left"
        )
)

# =========================
# Write Gold (OVERWRITE)
# =========================

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

# truncate_table()

gold.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/gold_db") \
    .option("dbtable", "gold_sentiment_price") \
    .option("user", "gold_user") \
    .option("password", "gold_password") \
    .option("driver", "org.postgresql.Driver") \
    .option("truncate", "true") \
    .mode("overwrite") \
    .save()


print("✅ Gold batch job completed successfully")
