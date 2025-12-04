"""
Silver layer schema definitions for Apache Iceberg.
Stores cleaned and enriched social media data with sentiment analysis.
"""

from pyiceberg.schema import Schema
from pyiceberg.types import (
    StringType, LongType, DoubleType, TimestampType, BooleanType,
    ListType, MapType, NestedField, StructType, FloatType
)
from datetime import datetime


# Silver layer schema - Cleaned and enriched data
SILVER_SCHEMA = Schema(
    NestedField(1, "event_id", StringType(), doc="Unique event identifier"),
    NestedField(2, "processing_timestamp", TimestampType(), doc="Processing timestamp"),
    NestedField(3, "platform", StringType(), doc="Platform name"),
    NestedField(4, "platform_id", StringType(), doc="Original platform ID"),
    NestedField(5, "item_type", StringType(), doc="Item type (post, comment, tweet)"),
    NestedField(6, "title", StringType(), doc="Original title"),
    NestedField(7, "text_original", StringType(), doc="Original text content"),
    NestedField(8, "text_cleaned", StringType(), doc="Cleaned text content"),
    NestedField(9, "text_normalized", StringType(), doc="Normalized text content"),
    NestedField(10, "author", StringType(), doc="Author username"),
    NestedField(11, "author_id", StringType(), doc="Author platform ID"),
    NestedField(12, "community", StringType(), doc="Community/subreddit"),
    NestedField(13, "created_at", TimestampType(), doc="Original creation timestamp"),
    NestedField(14, "url", StringType(), doc="URL to the content"),
    NestedField(15, "permalink", StringType(), doc="Permanent URL"),
    NestedField(16, "language", StringType(), doc="Detected language code"),
    NestedField(17, "language_confidence", FloatType(), doc="Language detection confidence"),

    # Sentiment analysis results
    NestedField(20, "sentiment", StructType([
        NestedField(201, "label", StringType(), doc="Sentiment label (positive, negative, neutral)"),
        NestedField(202, "score", FloatType(), doc="Sentiment score (-1 to 1)"),
        NestedField(203, "confidence", FloatType(), doc="Sentiment confidence"),
        NestedField(204, "model_version", StringType(), doc="Sentiment model version"),
        NestedField(205, "probability", StructType([
            NestedField(2051, "positive", FloatType()),
            NestedField(2052, "negative", FloatType()),
            NestedField(2053, "neutral", FloatType())
        ]))
    ])),

    # Engagement metrics
    NestedField(21, "engagement", StructType([
        NestedField(211, "score", DoubleType(), doc="Platform-specific score"),
        NestedField(212, "upvote_ratio", DoubleType(), doc="Upvote ratio"),
        NestedField(213, "comment_count", LongType(), doc="Number of comments"),
        NestedField(214, "retweet_count", LongType(), doc="Retweet count"),
        NestedField(215, "like_count", LongType(), doc="Like count"),
        NestedField(216, "quote_count", LongType(), doc="Quote count"),
        NestedField(217, "view_count", LongType(), doc="View count"),
        NestedField(218, "engagement_rate", FloatType(), doc="Calculated engagement rate")
    ])),

    # Content analysis
    NestedField(22, "content_analysis", StructType([
        NestedField(221, "text_length", LongType(), doc="Text character count"),
        NestedField(222, "word_count", LongType(), doc="Word count"),
        NestedField(223, "sentence_count", LongType(), doc="Sentence count"),
        NestedField(224, "has_urls", BooleanType(), doc="Contains URLs"),
        NestedField(225, "has_mentions", BooleanType(), doc="Contains user mentions"),
        NestedField(226, "has_hashtags", BooleanType(), doc="Contains hashtags"),
        NestedField(227, "url_count", LongType(), doc="Number of URLs"),
        NestedField(228, "mention_count", LongType(), doc="Number of mentions"),
        NestedField(229, "hashtag_count", LongType(), doc="Number of hashtags"),
        NestedField(230, "emoji_count", LongType(), doc="Number of emojis"),
        NestedField(231, "exclamation_count", LongType(), doc="Number of exclamation marks"),
        NestedField(232, "question_count", LongType(), doc="Number of question marks"),
        NestedField(233, "uppercase_ratio", FloatType(), doc="Ratio of uppercase letters"),
        NestedField(234, "readability_score", FloatType(), doc="Readability score")
    ])),

    # Entity extraction
    NestedField(23, "entities", StructType([
        NestedField(231, "hashtags", ListType(StringType()), doc="Extracted hashtags"),
        NestedField(232, "mentions", ListType(StringType()), doc="Extracted mentions"),
        NestedField(233, "urls", ListType(StringType()), doc="Extracted URLs"),
        NestedField(234, "cryptocurrencies", ListType(StringType()), doc="Mentioned cryptocurrencies"),
        NestedField(235, "companies", ListType(StringType()), doc="Mentioned companies"),
        NestedField(236, "people", ListType(StringType()), doc="Mentioned people")
    ])),

    # Fake account detection
    NestedField(24, "author_analysis", StructType([
        NestedField(241, "is_verified", BooleanType(), doc="Author is verified"),
        NestedField(242, "followers_count", LongType(), doc="Number of followers"),
        NestedField(243, "following_count", LongType(), doc="Number of following"),
        NestedField(244, "account_age_days", LongType(), doc="Account age in days"),
        NestedField(245, "post_count", LongType(), doc="Number of posts/tweets"),
        NestedField(246, "account_tier", StringType(), doc="Account quality tier"),
        NestedField(247, "spam_score", FloatType(), doc="Spam likelihood score"),
        NestedField(248, "bot_probability", FloatType(), doc="Bot probability"),
        NestedField(249, "fake_account_score", FloatType(), doc="Fake account detection score")
    ])),

    # Enrichment features
    NestedField(25, "enrichment", StructType([
        NestedField(251, "embedding_id", StringType(), doc="Reference to vector embedding"),
        NestedField(252, "topic_id", StringType(), doc="Assigned topic ID"),
        NestedField(253, "topic_probability", FloatType(), doc="Topic assignment probability"),
        NestedField(254, "trending_score", FloatType(), doc="Trending detection score"),
        NestedField(255, "viral_score", FloatType(), doc="Viral potential score"),
        NestedField(256, "quality_score", FloatType(), doc="Content quality score"),
        NestedField(257, "technical_terms", ListType(StringType()), doc="Extracted technical terms"),
        NestedField(258, "market_sentiment", StringType(), doc="Market sentiment indicator"),
        NestedField(259, "price_mentions", ListType(StringType()), doc="Price-related terms"),
        NestedField(260, "timestamp_anomalies", BooleanType(), doc="Timestamp anomalies detected")
    ])),

    # Processing metadata
    NestedField(30, "processing_metadata", StructType([
        NestedField(301, "processing_version", StringType(), doc="Processing pipeline version"),
        NestedField(302, "processing_duration_ms", LongType(), doc="Processing time in milliseconds"),
        NestedField(303, "models_used", ListType(StringType()), doc="Models applied"),
        NestedField(304, "processing_errors", ListType(StringType()), doc="Processing errors"),
        NestedField(305, "quality_flags", ListType(StringType()), doc="Data quality flags"),
        NestedField(306, "enrichment_timestamp", TimestampType(), doc="Enrichment timestamp"),
        NestedField(307, "source_event_timestamp", TimestampType(), doc="Original event timestamp")
    ]))
)

# Silver table properties
SILVER_TABLE_PROPERTIES = {
    'format-version': '2',
    'write.format.default': 'parquet',
    'write.parquet.compression-codec': 'snappy',
    'write.parquet.row-group-size-bytes': '134217728',  # 128MB
    'write.target-file-size-bytes': '536870912',  # 512MB
    'write.distribution-mode': 'hash',
    'write.fanout-enabled': 'true',
    'write.parquet.bloom-filter-enabled': 'sentiment,score',
    'write.parquet.bloom-filter.fpp': '0.1',
    'read.split.target-size': '134217728',  # 128MB
    'read.split.open-file-cost': '4194304',  # 4MB
    'gc.enabled': 'true',
    'gc.delete-file-type': 'COPY_ON_WRITE',
    'gc.max-file-age-ms': '604800000',  # 7 days
    'snapshot.expire.min-snapshots-to-keep': '5',
    'snapshot.expire.max-snapshots-to-keep': '20',
    'snapshot.expire.max-ref-age-ms': '7776000000'  # 90 days
}

# Silver partition specification
SILVER_PARTITION_SPEC = [
    "processing_timestamp_day(processing_timestamp)",
    "platform"
]

def create_silver_table(name: str, catalog) -> None:
    """
    Create Silver layer Iceberg table.

    Args:
        name: Table name
        catalog: Iceberg catalog instance
    """
    try:
        catalog.create_table(
            identifier=f"silver.{name}",
            schema=SILVER_SCHEMA,
            partition_spec=SILVER_PARTITION_SPEC,
            properties=SILVER_TABLE_PROPERTIES
        )
        print(f"Created Silver table: silver.{name}")
    except Exception as e:
        print(f"Error creating Silver table {name}: {e}")
        raise