#!/usr/bin/env python3
"""
Sentiment Analysis Job - Silver Layer Processing
Applies sentiment analysis models to cleaned social media data.
"""

import os
import sys
import logging
import yaml
import joblib
import numpy as np
from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, to_timestamp, when, lit, struct, udf, pandas_udf,
    PandasUDFType, array, explode
)
from pyspark.sql.types import (
    StructType, StringType, TimestampType, DoubleType, FloatType,
    ArrayType, StructType as StructTypeType
)
from pyspark.sql.streaming import StreamingQuery
import pandas as pd
from transformers import pipeline
import torch

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.spark_utils import create_spark_session

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SentimentAnalysisJob:
    """Sentiment analysis streaming job for Silver layer."""

    def __init__(self, config_path: str = 'config/spark_config.yaml'):
        """
        Initialize sentiment analysis job.

        Args:
            config_path: Path to Spark configuration file
        """
        self.config = self._load_config(config_path)
        self.spark = self._create_spark_session()
        self.bronze_table = "iceberg.bronze.social_media_raw"
        self.silver_table = "iceberg.silver.sentiment_enriched"
        self.checkpoint_location = "s3://sentiment-data/checkpoints/sentiment"

        # Initialize sentiment models
        self.sentiment_model = self._load_sentiment_model()
        self.crypto_sentiment_model = self._load_crypto_sentiment_model()

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
            app_name="Sentiment Analysis Job",
            config=self.config
        )

    def _load_sentiment_model(self):
        """
        Load general sentiment analysis model.

        Returns:
            Sentiment analysis pipeline/model
        """
        try:
            model_path = os.getenv('SENTIMENT_MODEL_PATH', 'models/sentiment_model.joblib')

            if os.path.exists(model_path):
                # Load scikit-learn model
                model = joblib.load(model_path)
                logger.info(f"Loaded scikit-learn sentiment model from {model_path}")
                return model
            else:
                # Use transformer model as fallback
                logger.info("Loading HuggingFace sentiment model as fallback")
                return pipeline(
                    "sentiment-analysis",
                    model="cardiffnlp/twitter-roberta-base-sentiment-latest",
                    device=0 if torch.cuda.is_available() else -1
                )

        except Exception as e:
            logger.error(f"Error loading sentiment model: {e}")
            logger.info("Using rule-based sentiment analysis as fallback")
            return None

    def _load_crypto_sentiment_model(self):
        """
        Load cryptocurrency-specific sentiment model.

        Returns:
            Crypto sentiment analysis model
        """
        try:
            crypto_model_path = os.getenv('CRYPTO_SENTIMENT_MODEL_PATH', 'models/crypto_sentiment_model.joblib')

            if os.path.exists(crypto_model_path):
                model = joblib.load(crypto_model_path)
                logger.info(f"Loaded crypto sentiment model from {crypto_model_path}")
                return model
            else:
                logger.info("Crypto sentiment model not found, will use general model")
                return None

        except Exception as e:
            logger.error(f"Error loading crypto sentiment model: {e}")
            return None

    def _read_from_bronze(self) -> DataFrame:
        """
        Read streaming data from Bronze layer.

        Returns:
            Streaming DataFrame from Bronze table
        """
        logger.info(f"Reading from Bronze table: {self.bronze_table}")

        # Read from Iceberg Bronze table
        df = self.spark.readStream \
            .format("iceberg") \
            .load(self.bronze_table) \
            .filter(col("processing_timestamp") > (current_timestamp() - expr("interval 1 hour")))

        return df

    def _sentiment_analysis_pandas(self, texts: pd.Series) -> pd.DataFrame:
        """
        Apply sentiment analysis using pandas UDF.

        Args:
            texts: Series of text content

        Returns:
            DataFrame with sentiment results
        """
        results = []

        for text in texts:
            if pd.isna(text) or not text.strip():
                results.append({
                    'label': 'neutral',
                    'score': 0.0,
                    'confidence': 0.0,
                    'positive_prob': 0.33,
                    'negative_prob': 0.33,
                    'neutral_prob': 0.34
                })
                continue

            try:
                if self.sentiment_model is not None:
                    # Use loaded model
                    if hasattr(self.sentiment_model, 'predict'):
                        # Scikit-learn model
                        prediction = self.sentiment_model.predict([text])[0]
                        probabilities = self.sentiment_model.predict_proba([text])[0]

                        label_map = {0: 'negative', 1: 'neutral', 2: 'positive'}
                        label = label_map.get(prediction, 'neutral')

                        results.append({
                            'label': label,
                            'score': float(probabilities[2] - probabilities[0]),  # pos - neg
                            'confidence': float(max(probabilities)),
                            'positive_prob': float(probabilities[2]),
                            'negative_prob': float(probabilities[0]),
                            'neutral_prob': float(probabilities[1])
                        })
                    else:
                        # HuggingFace transformer
                        result = self.sentiment_model(text)[0]

                        # Normalize labels
                        label = result['label'].lower()
                        if label == 'label_0':
                            label = 'negative'
                            score = -result['score']
                        elif label == 'label_1':
                            label = 'neutral'
                            score = 0.0
                        else:
                            label = 'positive'
                            score = result['score']

                        results.append({
                            'label': label,
                            'score': score,
                            'confidence': result['score'],
                            'positive_prob': float(result['score'] if label == 'positive' else 0.0),
                            'negative_prob': float(-result['score'] if label == 'negative' else 0.0),
                            'neutral_prob': float(1.0 - result['score'] if label != 'neutral' else 0.5)
                        })
                else:
                    # Rule-based fallback
                    results.append(self._rule_based_sentiment(text))

            except Exception as e:
                logger.warning(f"Error in sentiment analysis for text: {text[:100]}... - {e}")
                results.append({
                    'label': 'neutral',
                    'score': 0.0,
                    'confidence': 0.0,
                    'positive_prob': 0.33,
                    'negative_prob': 0.33,
                    'neutral_prob': 0.34
                })

        return pd.DataFrame(results)

    def _rule_based_sentiment(self, text: str) -> Dict[str, float]:
        """
        Rule-based sentiment analysis as fallback.

        Args:
            text: Input text

        Returns:
            Dictionary with sentiment results
        """
        positive_words = ['good', 'great', 'excellent', 'bullish', 'moon', 'buy', 'hold', 'wagmi']
        negative_words = ['bad', 'terrible', 'awful', 'bearish', 'dump', 'sell', 'rekt', 'ngmi']

        text_lower = text.lower()
        pos_count = sum(1 for word in positive_words if word in text_lower)
        neg_count = sum(1 for word in negative_words if word in text_lower)

        total = pos_count + neg_count
        if total == 0:
            return {
                'label': 'neutral',
                'score': 0.0,
                'confidence': 0.5,
                'positive_prob': 0.33,
                'negative_prob': 0.33,
                'neutral_prob': 0.34
            }

        if pos_count > neg_count:
            label = 'positive'
            score = pos_count / total
        elif neg_count > pos_count:
            label = 'negative'
            score = -neg_count / total
        else:
            label = 'neutral'
            score = 0.0

        return {
            'label': label,
            'score': score,
            'confidence': min(1.0, total / 5.0),
            'positive_prob': pos_count / max(1, total),
            'negative_prob': neg_count / max(1, total),
            'neutral_prob': 0.34
        }

    def _apply_sentiment_analysis(self, df: DataFrame) -> DataFrame:
        """
        Apply sentiment analysis to the streaming data.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with sentiment analysis results
        """
        logger.info("Applying sentiment analysis...")

        # Define pandas UDF schema
        sentiment_schema = StructType([
            StructField("label", StringType(), False),
            StructField("score", FloatType(), False),
            StructField("confidence", FloatType(), False),
            StructField("probability", StructType([
                StructField("positive", FloatType(), False),
                StructField("negative", FloatType(), False),
                StructField("neutral", FloatType(), False)
            ]), False)
        ])

        # Create pandas UDF
        sentiment_udf = pandas_udf(self._sentiment_analysis_pandas, sentiment_schema)

        # Apply sentiment analysis
        enriched_df = df.withColumn(
            "sentiment",
            sentiment_udf(col("text_clean"))
        )

        return enriched_df

    def _enhance_with_content_analysis(self, df: DataFrame) -> DataFrame:
        """
        Add content analysis features.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with content analysis
        """
        # Add text length, word count, etc.
        enhanced_df = df \
            .withColumn("text_length", length(col("text_clean"))) \
            .withColumn("word_count", size(split(col("text_clean"), " "))) \
            .withColumn("sentence_count", size(split(col("text_clean"), "[.!?]+"))) \
            .withColumn("has_urls", when(col("text_original").rlike(r"http[s]?://"), True).otherwise(False)) \
            .withColumn("has_mentions", when(col("text_original").rlike(r"@"), True).otherwise(False)) \
            .withColumn("has_hashtags", when(col("text_original").rlike(r"#"), True).otherwise(False)) \
            .withColumn("uppercase_ratio", self._calculate_uppercase_ratio(col("text_original")))

        return enhanced_df

    def _calculate_uppercase_ratio(self, text_col):
        """Calculate ratio of uppercase letters."""
        return when(
            length(text_col) > 0,
            (length(regexp_replace(text_col, r"[A-Z]", "")) - length(text_col)).cast("double") / length(text_col)
        ).otherwise(0.0)

    def _write_to_silver(self, df: DataFrame) -> StreamingQuery:
        """
        Write enriched data to Silver layer.

        Args:
            df: Enriched DataFrame

        Returns:
            StreamingQuery object
        """
        logger.info(f"Writing to Silver table: {self.silver_table}")

        def write_batch_to_silver(batch_df, batch_id):
            """Write micro-batch to Silver Iceberg table."""
            try:
                logger.info(f"Processing batch {batch_id} with {batch_df.count()} records")

                # Select and transform columns for Silver layer
                silver_df = batch_df.select(
                    col("event_id"),
                    col("processing_timestamp").alias("processing_timestamp"),
                    col("platform"),
                    col("platform_id"),
                    col("item_type"),
                    col("title"),
                    col("text_original").alias("text_original"),
                    col("text_clean").alias("text_cleaned"),
                    col("text_clean").alias("text_normalized"),  # Same for now
                    col("author"),
                    col("author_id"),
                    col("community"),
                    col("created_at"),
                    col("url"),
                    col("permalink"),
                    col("language"),
                    lit(None).cast("float").alias("language_confidence"),  # Will be added later
                    col("sentiment"),
                    # Create engagement struct
                    struct(
                        col("score"),
                        col("upvote_ratio"),
                        col("comment_count"),
                        col("retweet_count"),
                        col("like_count"),
                        col("quote_count"),
                        col("view_count")
                    ).alias("engagement"),
                    # Add content analysis
                    struct(
                        col("text_length"),
                        col("word_count"),
                        col("sentence_count"),
                        col("has_urls"),
                        col("has_mentions"),
                        col("has_hashtags"),
                        lit(0).cast("long").alias("url_count"),
                        lit(0).cast("long").alias("mention_count"),
                        lit(0).cast("long").alias("hashtag_count"),
                        lit(0).cast("long").alias("emoji_count"),
                        lit(0).cast("long").alias("exclamation_count"),
                        lit(0).cast("long").alias("question_count"),
                        col("uppercase_ratio"),
                        lit(0.5).cast("float").alias("readability_score")
                    ).alias("content_analysis"),
                    # Add empty structs for now (will be populated in enrichment job)
                    struct(
                        array().cast("array<string>").alias("hashtags"),
                        array().cast("array<string>").alias("mentions"),
                        array().cast("array<string>").alias("urls"),
                        array().cast("array<string>").alias("cryptocurrencies"),
                        array().cast("array<string>").alias("companies"),
                        array().cast("array<string>").alias("people")
                    ).alias("entities"),
                    # Add processing metadata
                    struct(
                        lit("v1.0").alias("processing_version"),
                        lit(None).cast("long").alias("processing_duration_ms"),
                        array("sentiment_analysis").alias("models_used"),
                        array().cast("array<string>").alias("processing_errors"),
                        array().cast("array<string>").alias("quality_flags"),
                        current_timestamp().alias("enrichment_timestamp"),
                        col("processing_timestamp").alias("source_event_timestamp")
                    ).alias("processing_metadata")
                )

                # Write to Iceberg table
                silver_df.write \
                    .format("iceberg") \
                    .mode("append") \
                    .save(self.silver_table)

                logger.info(f"Successfully wrote batch {batch_id} to Silver layer")

            except Exception as e:
                logger.error(f"Error writing batch {batch_id} to Silver layer: {e}")
                raise

        query = df.writeStream \
            .foreachBatch(write_batch_to_silver) \
            .option("checkpointLocation", self.checkpoint_location) \
            .trigger(processingTime="60 seconds") \
            .outputMode("append") \
            .start()

        return query

    def run(self):
        """Run the sentiment analysis streaming job."""
        try:
            logger.info("Starting Sentiment Analysis streaming job...")

            # Read from Bronze layer
            bronze_df = self._read_from_bronze()

            # Apply sentiment analysis
            sentiment_df = self._apply_sentiment_analysis(bronze_df)

            # Enhance with content analysis
            enriched_df = self._enhance_with_content_analysis(sentiment_df)

            # Write to Silver layer
            query = self._write_to_silver(enriched_df)

            logger.info("Sentiment Analysis job started successfully")
            logger.info(f"Checkpoint location: {self.checkpoint_location}")
            logger.info(f"Silver table: {self.silver_table}")

            # Wait for termination
            query.awaitTermination()

        except Exception as e:
            logger.error(f"Error in sentiment analysis job: {e}")
            raise
        finally:
            self.spark.stop()

    def stop(self):
        """Stop the streaming job."""
        if self.spark:
            self.spark.stop()
            logger.info("Sentiment Analysis job stopped")


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description='Sentiment Analysis Streaming Job')
    parser.add_argument('--config', type=str, default='config/spark_config.yaml', help='Spark config file path')
    parser.add_argument('--checkpoint', type=str, help='Override checkpoint location')
    args = parser.parse_args()

    # Create and run job
    job = SentimentAnalysisJob(args.config)

    if args.checkpoint:
        job.checkpoint_location = args.checkpoint

    job.run()


if __name__ == '__main__':
    main()