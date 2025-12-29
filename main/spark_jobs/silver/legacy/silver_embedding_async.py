# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col
# from sentence_transformers import SentenceTransformer
# import pandas as pd

# # =========================
# # Spark Session
# # =========================

# spark = (
#     SparkSession.builder
#         .appName("silver-async-embedding")
#         .master("local[*]")

#         # Delta
#         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
#         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

#         # MinIO / S3A
#         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
#         .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
#         .config("spark.hadoop.fs.s3a.access.key", "minio")
#         .config("spark.hadoop.fs.s3a.secret.key", "minio123")
#         .config("spark.hadoop.fs.s3a.path.style.access", "true")

#         # Numeric timeouts ONLY
#         .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
#         .config("spark.hadoop.fs.s3a.socket.timeout", "60000")
#         .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")
#         .config("fs.s3a.connection.timeout", "60000")
#         .config("fs.s3a.socket.timeout", "60000")
#         .config("fs.s3a.connection.establish.timeout", "60000")

#         .getOrCreate()
# )

# spark.sparkContext.setLogLevel("WARN")

# # =========================
# # Paths
# # =========================

# SILVER_PATH = "s3a://delta-silver/reddit_clean"
# CHECKPOINT_PATH = "s3a://delta-silver/_checkpoints/reddit_embedding"

# # =========================
# # Load model ONCE
# # =========================

# model = SentenceTransformer("all-MiniLM-L6-v2")

# # =========================
# # Enrichment logic
# # =========================

# def embed_and_merge(batch_df, batch_id: int):
#     """
#     Enrich ONLY rows missing embeddings.
#     Idempotent.
#     Safe to restart.
#     """

#     if batch_df.isEmpty():
#         return

#     # Only rows that still need embeddings
#     pending = batch_df.filter(col("embedding").isNull())

#     if pending.isEmpty():
#         return

#     pdf = pending.select(
#         "event_id",
#         "raw_text"
#     ).toPandas()

#     # Compute embeddings
#     pdf["embedding"] = model.encode(
#         pdf["raw_text"].tolist(),
#         batch_size=32,
#         show_progress_bar=False,
#         normalize_embeddings=True
#     ).tolist()

#     pdf["embedding_model"] = "all-MiniLM-L6-v2"
#     pdf["embedding_at"] = pd.Timestamp.now()

#     updates_df = spark.createDataFrame(pdf)
#     updates_df.createOrReplaceTempView("updates")

#     # Idempotent MERGE
#     spark.sql(f"""
#         MERGE INTO delta.`{SILVER_PATH}` t
#         USING updates s
#         ON t.event_id = s.event_id
#         WHEN MATCHED AND t.embedding IS NULL
#         THEN UPDATE SET t.embedding = s.embedding
#     """)

# # =========================
# # Read Silver as stream (CDC-style)
# # =========================

# silver_stream = (
#     spark.readStream
#         .format("delta")
#         .load(SILVER_PATH)
# )

# query = (
#     silver_stream.writeStream
#         .foreachBatch(embed_and_merge)
#         .option("checkpointLocation", CHECKPOINT_PATH)
#         .start()
# )

# query.awaitTermination()
