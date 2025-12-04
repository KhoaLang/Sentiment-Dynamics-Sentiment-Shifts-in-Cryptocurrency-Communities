#!/usr/bin/env python3
"""
Preprocess Job - Bronze Layer Ingestion
Cleans and normalizes raw social media data before writing to Bronze layer.
"""

import os
import sys
import logging
import json
import yaml
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, when, trim, lower, regexp_replace,
    length, split, size, array_contains, udf, pandas_udf, PandasUDFType
)
from pyspark.sql.types import StructType, StringType, TimestampType, BooleanType
from pyspark.sql.streaming import StreamingQuery
import langdetect
import re
import html
import pandas as pd

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.spark_utils import create_spark_session
from utils.text_utils import clean_text, detect_language_pandas

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PreprocessJob:
    """Preprocess streaming job for Bronze layer."""

    def __init__(self, config_path: str = 'config/spark_config.yaml'):
        """
        Initialize preprocess job.

        Args:
            config_path: Path to Spark configuration file
        """
        self.config = self._load_config(config_path)
        self.spark = self._create_spark_session()
        self.kafka_source_topic = "reddit_raw"  # Will be extended for multiple sources
        self.bronze_table = "bronze.social_media_raw"
        self.checkpoint_location = "s3://sentiment-data/checkpoints/preprocess"

    def _load_config(self, config_path: str) -> dict:
        """Load configuration from YAML file."""
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            return config.get('spark', {})
        except FileNotFoundError:
            logger.warning(f"Config file {config_path} not found, using defaults")
            return {}
        except yaml.YAMLError as e:
            logger.error(f"Error parsing config file: {e}")
            raise

    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session."""
        return create_spark_session(
            app_name="Sentiment Preprocess Job",
            config=self.config
        )

    def _create_source_schema(self) -> StructType:
        """Define schema for incoming Kafka messages."""
        return StructType() \
            .add("event_id", StringType()) \
            .add("event_timestamp", TimestampType()) \
            .add("source", StringType()) \
            .add("schema_version", StringType()) \
            .add("data", StructType()
                .add("platform", StringType())
                .add("platform_id", StringType())
                .add("item_type", StringType())
                .add("title", StringType())
                .add("text", StringType())
                .add("author", StringType())
                .add("author_id", StringType())
                .add("community", StringType())
                .add("score", StringType())
                .add("upvote_ratio", StringType())
                .add("comment_count", StringType())
                .add("retweet_count", StringType())
                .add("like_count", StringType())
                .add("quote_count", StringType())
                .add("view_count", StringType())
                .add("created_at", StringType())
                .add("url", StringType())
                .add("permalink", StringType())
                .add("language", StringType())
                .add("text_clean", StringType())
                .add("metadata", StringType())
            )

    def _read_from_kafka(self) -> StreamingQuery:
        """
        Read streaming data from Kafka.

        Returns:
            Streaming DataFrame from Kafka
        """
        logger.info("Reading from Kafka topic: reddit_raw")

        # Define schema for incoming data
        schema = self._create_source_schema()

        # Read from Kafka
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')) \
            .option("subscribe", "reddit_raw,twitter_raw,webhook_raw") \
            .option("startingOffsets", "latest") \
            .option("maxOffsetsPerTrigger", "10000") \
            .option("failOnDataLoss", "false") \
            .load()

        # Parse JSON value
        parsed_df = df.selectExpr("CAST(value AS STRING) as json_string") \
            .select(from_json("json_string", schema).alias("data")) \
            .select("data.*")

        return parsed_df

    def _preprocess_data(self, df) -> StreamingQuery:
        """
        Preprocess and clean the streaming data.

        Args:
            df: Input DataFrame

        Returns:
            Processed DataFrame
        """
        logger.info("Preprocessing data...")

        # Define pandas UDF for text cleaning and language detection
        def preprocess_text_pandas(series: pd.Series) -> pd.Series:
            """Preprocess text content using pandas UDF."""
            return series.apply(lambda x: clean_text(str(x)) if pd.notna(x) else "")

        def detect_language_pandas_udf(series: pd.Series) -> pd.Series:
            """Detect language using pandas UDF."""
            return series.apply(lambda x: detect_language_pandas(str(x)) if pd.notna(x) and len(str(x)) > 10 else "unknown")

        # Register UDFs
        preprocess_text_udf = pandas_udf(preprocess_text_pandas, StringType())
        detect_language_udf = pandas_udf(detect_language_pandas_udf, StringType())

        # Process the data
        processed_df = df.filter(col("data").isNotNull()) \
            .withColumn("platform", col("data.platform")) \
            .withColumn("platform_id", col("data.platform_id")) \
            .withColumn("item_type", col("data.item_type")) \
            .withColumn("title", col("data.title")) \
            .withColumn("text_original", col("data.text")) \
            .withColumn("text_cleaned", preprocess_text_udf(col("data.text"))) \
            .withColumn("language", detect_language_udf(col("data.text"))) \
            .withColumn("author", col("data.author")) \
            .withColumn("author_id", col("data.author_id")) \
            .withColumn("community", col("data.community")) \
            .withColumn("created_at", to_timestamp(col("data.created_at"))) \
            .withColumn("url", col("data.url")) \
            .withColumn("permalink", col("data.permalink")) \
            .withColumn("score", col("data.score").cast("double")) \
            .withColumn("upvote_ratio", col("data.upvote_ratio").cast("double")) \
            .withColumn("comment_count", col("data.comment_count").cast("long")) \
            .withColumn("retweet_count", col("data.retweet_count").cast("long")) \
            .withColumn("like_count", col("data.like_count").cast("long")) \
            .withColumn("quote_count", col("data.quote_count").cast("long")) \
            .withColumn("view_count", col("data.view_count").cast("long")) \
            .withColumn("processing_timestamp", current_timestamp()) \
            .withColumn("schema_version", col("schema_version")) \
            .withColumn("source_event_timestamp", col("event_timestamp"))

        # Parse metadata if present
        metadata_df = processed_df.withColumn(
            "metadata_json",
            when(col("data.metadata").isNotNull(), col("data.metadata"))
            .otherwise("{}")
        )

        # Add data quality checks
        quality_df = metadata_df \
            .withColumn("has_text", when(length(col("text_cleaned")) > 10, True).otherwise(False)) \
            .withColumn("has_author", when(col("author").isNotNull() & (col("author") != ""), True).otherwise(False)) \
            .withColumn("has_timestamp", when(col("created_at").isNotNull(), True).otherwise(False)) \
            .withColumn("text_length", length(col("text_cleaned"))) \
            .withColumn("word_count", size(split(col("text_cleaned"), " "))) \
            .withColumn("quality_score", self._calculate_quality_score())

        # Filter out low-quality records
        filtered_df = quality_df.filter(
            (col("has_text") == True) &
            (col("text_length") >= 10) &
            (col("text_length") <= 10000) &
            (col("quality_score") >= 0.3)
        )

        # Select final columns for Bronze layer
        bronze_df = filtered_df.select(
            col("event_id"),
            col("processing_timestamp"),
            col("platform"),
            col("platform_id"),
            col("item_type"),
            col("title"),
            col("text_original"),
            col("text_cleaned").alias("text_clean"),
            col("language"),
            col("author"),
            col("author_id"),
            col("community"),
            col("score"),
            col("upvote_ratio"),
            col("comment_count"),
            col("retweet_count"),
            col("like_count"),
            col("quote_count"),
            col("view_count"),
            col("created_at"),
            col("url"),
            col("permalink"),
            col("metadata_json").alias("metadata")
        )

        return bronze_df

    def _calculate_quality_score(self):
        """Calculate data quality score."""
        return (
            when(col("has_text"), 0.4).otherwise(0.0) +
            when(col("has_author"), 0.2).otherwise(0.0) +
            when(col("has_timestamp"), 0.2).otherwise(0.0) +
            when((col("text_length") >= 50) & (col("text_length") <= 1000), 0.2).otherwise(0.1)
        )

    def _write_to_iceberg(self, df) -> StreamingQuery:
        """
        Write processed data to Iceberg Bronze table.

        Args:
            df: Processed DataFrame

        Returns:
            StreamingQuery object
        """
        logger.info(f"Writing to Bronze table: {self.bronze_table}")

        def write_batch_to_iceberg(batch_df, batch_id):
            """Write micro-batch to Iceberg."""
            try:
                logger.info(f"Processing batch {batch_id} with {batch_df.count()} records")

                # Write to Iceberg table
                batch_df.write \
                    .format("iceberg") \
                    .mode("append") \
                    .save(self.bronze_table)

                logger.info(f"Successfully wrote batch {batch_id} to Bronze layer")

            except Exception as e:
                logger.error(f"Error writing batch {batch_id} to Bronze layer: {e}")
                raise

        query = df.writeStream \
            .foreachBatch(write_batch_to_iceberg) \
            .option("checkpointLocation", self.checkpoint_location) \
            .trigger(processingTime="30 seconds") \
            .outputMode("append") \
            .start()

        return query

    def run(self):
        """Run the preprocess streaming job."""
        try:
            logger.info("Starting Preprocess streaming job...")

            # Read from Kafka
            source_df = self._read_from_kafka()

            # Preprocess data
            processed_df = self._preprocess_data(source_df)

            # Write to Bronze layer
            query = self._write_to_iceberg(processed_df)

            logger.info("Preprocess job started successfully")
            logger.info(f"Checkpoint location: {self.checkpoint_location}")
            logger.info(f"Bronze table: {self.bronze_table}")

            # Wait for termination
            query.awaitTermination()

        except Exception as e:
            logger.error(f"Error in preprocess job: {e}")
            raise
        finally:
            self.spark.stop()

    def stop(self):
        """Stop the streaming job."""
        if self.spark:
            self.spark.stop()
            logger.info("Preprocess job stopped")


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description='Preprocess Streaming Job')
    parser.add_argument('--config', type=str, default='config/spark_config.yaml', help='Spark config file path')
    parser.add_argument('--checkpoint', type=str, help='Override checkpoint location')
    args = parser.parse_args()

    # Create and run job
    job = PreprocessJob(args.config)

    if args.checkpoint:
        job.checkpoint_location = args.checkpoint

    job.run()


if __name__ == '__main__':
    main()