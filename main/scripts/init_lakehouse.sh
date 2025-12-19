#!/usr/bin/env bash
set -e

# =========================
# MinIO config
# =========================
MINIO_ALIAS="local"
MINIO_ENDPOINT="http://localhost:9000"
MINIO_ACCESS_KEY="minio"
MINIO_SECRET_KEY="minio123"

# Buckets
BRONZE_BUCKET="delta-bronze"
SILVER_BUCKET="delta-silver"
GOLD_BUCKET="delta-gold"

# Paths
BRONZE_PATH="s3a://${BRONZE_BUCKET}/reddit_raw"
SILVER_PATH="s3a://${SILVER_BUCKET}/reddit_clean"

echo "🔧 Configuring MinIO alias..."
mc alias set ${MINIO_ALIAS} ${MINIO_ENDPOINT} ${MINIO_ACCESS_KEY} ${MINIO_SECRET_KEY} --quiet

# =========================
# Create buckets if missing
# =========================

create_bucket_if_missing () {
  local bucket=$1
  if mc ls ${MINIO_ALIAS}/${bucket} >/dev/null 2>&1; then
    echo "✅ Bucket exists: ${bucket}"
  else
    echo "🪣 Creating bucket: ${bucket}"
    mc mb ${MINIO_ALIAS}/${bucket}
  fi
}

create_bucket_if_missing ${BRONZE_BUCKET}
create_bucket_if_missing ${SILVER_BUCKET}
create_bucket_if_missing ${GOLD_BUCKET}

# =========================
# Initialize Delta schemas
# =========================

echo "📐 Initializing Delta schemas..."

spark-submit <<EOF
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, ArrayType, FloatType

spark = (
    SparkSession.builder
        .appName("init-delta-schema")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", "${MINIO_ENDPOINT}")
        .config("spark.hadoop.fs.s3a.access.key", "${MINIO_ACCESS_KEY}")
        .config("spark.hadoop.fs.s3a.secret.key", "${MINIO_SECRET_KEY}")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .getOrCreate()
)

# -------- Bronze schema --------
bronze_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("subreddit", StringType(), True),
    StructField("raw_text", StringType(), True),
    StructField("created_utc", TimestampType(), True),
    StructField("ingested_at", TimestampType(), True),
])

# -------- Silver schema --------
silver_schema = StructType(bronze_schema.fields + [
    StructField("processed_at", TimestampType(), True),
    StructField("embedding", ArrayType(FloatType()), True),
])

def init_table(path, schema):
    if not spark._jsparkSession.catalog().tableExists(f"delta.`{path}`"):
        print(f"🆕 Initializing table: {path}")
        spark.createDataFrame([], schema) \
            .write.format("delta") \
            .mode("overwrite") \
            .save(path)
    else:
        print(f"✅ Table exists: {path}")

init_table("${BRONZE_PATH}", bronze_schema)
init_table("${SILVER_PATH}", silver_schema)

spark.stop()
EOF

echo "🎉 Lakehouse initialization complete"
