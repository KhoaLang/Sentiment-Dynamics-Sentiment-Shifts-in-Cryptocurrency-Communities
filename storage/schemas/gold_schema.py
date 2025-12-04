"""
Gold layer schema definitions for Apache Iceberg.
Stores aggregated analytics and metrics ready for dashboards and BI.
"""

from pyiceberg.schema import Schema
from pyiceberg.types import (
    StringType, LongType, DoubleType, TimestampType, BooleanType,
    ListType, MapType, NestedField, StructType, FloatType
)
from datetime import datetime


# Gold layer schema - Aggregated analytics and metrics
GOLD_SCHEMA = Schema(
    NestedField(1, "window_start", TimestampType(), doc="Aggregation window start"),
    NestedField(2, "window_end", TimestampType(), doc="Aggregation window end"),
    NestedField(3, "window_type", StringType(), doc="Window type (1min, 5min, 15min, 1hour, 1day)"),
    NestedField(4, "platform", StringType(), doc="Platform name or 'all'"),
    NestedField(5, "community", StringType(), doc="Community name or 'all'"),
    NestedField(6, "created_at", TimestampType(), doc="Record creation timestamp"),

    # Volume metrics
    NestedField(10, "volume_metrics", StructType([
        NestedField(101, "total_posts", LongType(), doc="Total number of posts"),
        NestedField(1011, "total_comments", LongType(), doc="Total number of comments"),
        NestedField(1012, "total_items", LongType(), doc="Total items (posts + comments)"),
        NestedField(1013, "unique_authors", LongType(), doc="Number of unique authors"),
        NestedField(1014, "new_authors", LongType(), doc="Number of new authors"),
        NestedField(1015, "active_authors", LongType(), doc="Number of active authors"),
        NestedField(1016, "avg_posts_per_author", FloatType(), doc="Average posts per author"),
        NestedField(1017, "growth_rate", FloatType(), doc="Volume growth rate"),
        NestedField(1018, "velocity", FloatType(), doc="Posting velocity (posts/min)")
    ])),

    # Sentiment metrics
    NestedField(11, "sentiment_metrics", StructType([
        NestedField(111, "sentiment_distribution", StructType([
            NestedField(1111, "positive_count", LongType()),
            NestedField(1112, "negative_count", LongType()),
            NestedField(1113, "neutral_count", LongType()),
            NestedField(1114, "positive_percentage", FloatType()),
            NestedField(1115, "negative_percentage", FloatType()),
            NestedField(1116, "neutral_percentage", FloatType())
        ])),
        NestedField(112, "sentiment_scores", StructType([
            NestedField(1121, "avg_sentiment_score", FloatType()),
            NestedField(1122, "median_sentiment_score", FloatType()),
            NestedField(1123, "min_sentiment_score", FloatType()),
            NestedField(1124, "max_sentiment_score", FloatType()),
            NestedField(1125, "sentiment_stddev", FloatType()),
            NestedField(1126, "sentiment_skewness", FloatType()),
            NestedField(1127, "sentiment_kurtosis", FloatType())
        ])),
        NestedField(113, "sentiment_volatility", FloatType(), doc="Sentiment volatility score"),
        NestedField(114, "sentiment_momentum", FloatType(), doc="Sentiment momentum"),
        NestedField(115, "sentiment_zscore", FloatType(), doc="Sentiment z-score"),
        NestedField(116, "sentiment_drift", FloatType(), doc="Sentiment drift indicator"),
        NestedField(117, "sentiment_change_rate", FloatType(), doc="Rate of sentiment change")
    ])),

    # Engagement metrics
    NestedField(12, "engagement_metrics", StructType([
        NestedField(121, "total_engagement", LongType(), doc="Total engagement count"),
        NestedField(122, "avg_engagement", FloatType(), doc="Average engagement per post"),
        NestedField(123, "median_engagement", FloatType(), doc="Median engagement"),
        NestedField(124, "engagement_distribution", StructType([
            NestedField(1241, "high_engagement_count", LongType()),
            NestedField(1242, "medium_engagement_count", LongType()),
            NestedField(1243, "low_engagement_count", LongType())
        ])),
        NestedField(125, "viral_posts", LongType(), doc="Number of viral posts"),
        NestedField(126, "viral_rate", FloatType(), doc="Viral content rate"),
        NestedField(127, "engagement_efficiency", FloatType(), doc="Engagement per character")
    ])),

    # Author metrics
    NestedField(13, "author_metrics", StructType([
        NestedField(131, "author_quality_distribution", StructType([
            NestedField(1311, "high_quality_authors", LongType()),
            NestedField(1312, "medium_quality_authors", LongType()),
            NestedField(1313, "low_quality_authors", LongType()),
            NestedField(1314, "spam_authors", LongType())
        ])),
        NestedField(132, "verification_rate", FloatType(), doc="Percentage of verified authors"),
        NestedField(133, "avg_author_age", FloatType(), doc="Average author account age in days"),
        NestedField(134, "avg_author_followers", FloatType(), doc="Average follower count"),
        NestedField(135, "influencer_activity", LongType(), doc="Number of influencer posts"),
        NestedField(136, "bot_activity", LongType(), doc="Number of bot posts"),
        NestedField(137, "authenticity_score", FloatType(), doc="Overall authenticity score")
    ])),

    # Content metrics
    NestedField(14, "content_metrics", StructType([
        NestedField(141, "avg_text_length", FloatType(), doc="Average text length"),
        NestedField(142, "avg_word_count", FloatType(), doc="Average word count"),
        NestedField(143, "content_diversity", FloatType(), doc="Content diversity index"),
        NestedField(144, "hashtag_usage", FloatType(), doc="Hashtag usage rate"),
        NestedField(145, "mention_usage", FloatType(), doc="Mention usage rate"),
        NestedField(146, "url_usage", FloatType(), doc="URL usage rate"),
        NestedField(147, "emoji_usage", FloatType(), doc="Emoji usage rate"),
        NestedField(148, "multilingual_posts", LongType(), doc="Number of multilingual posts"),
        NestedField(149, "quality_score_avg", FloatType(), doc="Average content quality score")
    ])),

    # Cryptocurrency-specific metrics
    NestedField(15, "crypto_metrics", StructType([
        NestedField(151, "cryptocurrency_mentions", LongType(), doc="Total crypto mentions"),
        NestedField(152, "unique_cryptocurrencies", LongType(), doc="Number of unique cryptocurrencies mentioned"),
        NestedField(153, "top_cryptocurrencies", ListType(StringType()), doc="Top mentioned cryptocurrencies"),
        NestedField(154, "price_mentions", LongType(), doc="Number of price-related mentions"),
        NestedField(155, "trading_mentions", LongType(), doc="Number of trading-related mentions"),
        NestedField(156, "technical_analysis_mentions", LongType(), doc="Technical analysis mentions"),
        NestedField(157, "market_sentiment", StringType(), doc="Overall market sentiment"),
        NestedField(158, "bull_bear_ratio", FloatType(), doc="Bull vs bear sentiment ratio"),
        NestedField(159, "fear_greed_indicator", FloatType(), doc="Fear/greed indicator")
    ])),

    # Anomaly detection
    NestedField(16, "anomaly_metrics", StructType([
        NestedField(161, "anomaly_score", FloatType(), doc="Overall anomaly score"),
        NestedField(162, "volume_anomaly", BooleanType(), doc="Volume anomaly detected"),
        NestedField(163, "sentiment_anomaly", BooleanType(), doc="Sentiment anomaly detected"),
        NestedField(164, "engagement_anomaly", BooleanType(), doc="Engagement anomaly detected"),
        NestedField(165, "author_anomaly", BooleanType(), doc="Author behavior anomaly"),
        NestedField(166, "content_anomaly", BooleanType(), doc="Content anomaly"),
        NestedField(167, "burst_detected", BooleanType(), doc="Activity burst detected"),
        NestedField(168, "spike_magnitude", FloatType(), doc="Magnitude of detected spike"),
        NestedField(169, "anomaly_reasons", ListType(StringType()), doc="Reasons for anomaly detection")
    ])),

    # Trending metrics
    NestedField(17, "trending_metrics", StructType([
        NestedField(171, "trending_topics", ListType(StringType()), doc="Current trending topics"),
        NestedField(172, "trending_hashtags", ListType(StringType()), doc="Trending hashtags"),
        NestedField(173, "trending_entities", ListType(StringType()), doc="Trending entities"),
        NestedField(174, "topic_velocity", MapType(StringType(), FloatType()), doc="Topic velocity scores"),
        NestedField(175, "emerging_trends", ListType(StringType()), doc="Emerging trends"),
        NestedField(176, "trending_cryptos", ListType(StringType()), doc="Trending cryptocurrencies"),
        NestedField(177, "trend_strength", FloatType(), doc="Overall trend strength"),
        NestedField(178, "trend_persistence", FloatType(), doc="Trend persistence score")
    ])),

    # Time series features
    NestedField(18, "time_series_features", StructType([
        NestedField(181, "lag_1_volume", LongType(), doc="Volume 1 window ago"),
        NestedField(182, "lag_1_sentiment", FloatType(), doc="Sentiment 1 window ago"),
        NestedField(183, "volume_moving_avg_3", FloatType(), doc="3-window moving average"),
        NestedField(184, "volume_moving_avg_7", FloatType(), doc="7-window moving average"),
        NestedField(185, "sentiment_moving_avg_3", FloatType(), doc="3-window sentiment moving average"),
        NestedField(186, "sentiment_moving_avg_7", FloatType(), doc="7-window sentiment moving average"),
        NestedField(187, "trend_direction", StringType(), doc="Trend direction (up/down/stable)"),
        NestedField(188, "seasonal_pattern", BooleanType(), doc="Seasonal pattern detected")
    ])),

    # Quality metrics
    NestedField(19, "quality_metrics", StructType([
        NestedField(191, "data_completeness", FloatType(), doc="Data completeness score"),
        NestedField(192, "processing_success_rate", FloatType(), doc="Processing success rate"),
        NestedField(193, "duplicate_rate", FloatType(), doc="Duplicate content rate"),
        NestedField(194, "spam_rate", FloatType(), doc="Spam content rate"),
        NestedField(195, "bot_rate", FloatType(), doc="Bot content rate"),
        NestedField(196, "quality_score", FloatType(), doc("Overall data quality score")),
        NestedField(197, "confidence_level", FloatType(), doc="Confidence in metrics")
    ]))
)

# Gold table properties
GOLD_TABLE_PROPERTIES = {
    'format-version': '2',
    'write.format.default': 'parquet',
    'write.parquet.compression-codec': 'zstd',  # Better compression for analytics
    'write.parquet.row-group-size-bytes': '134217728',  # 128MB
    'write.target-file-size-bytes': '268435456',  # 256MB (smaller for analytics)
    'write.distribution-mode': 'hash',
    'write.fanout-enabled': 'true',
    'write.parquet.bloom-filter-enabled': 'window_start,platform,community',
    'write.parquet.bloom-filter.fpp': '0.05',
    'read.split.target-size': '134217728',  # 128MB
    'read.split.open-file-cost': '4194304',  # 4MB
    'gc.enabled': 'true',
    'gc.delete-file-type': 'COPY_ON_WRITE',
    'gc.max-file-age-ms': '2592000000',  # 30 days
    'snapshot.expire.min-snapshots-to-keep': '10',
    'snapshot.expire.max-snapshots-to-keep': '50',
    'snapshot.expire.max-ref-age-ms': '15552000000'  # 180 days
}

# Gold partition specification
GOLD_PARTITION_SPEC = [
    "window_type",
    "window_start_month(window_start)",  # Monthly partitions for long-term storage
    "platform"
]

def create_gold_table(name: str, catalog) -> None:
    """
    Create Gold layer Iceberg table.

    Args:
        name: Table name
        catalog: Iceberg catalog instance
    """
    try:
        catalog.create_table(
            identifier=f"gold.{name}",
            schema=GOLD_SCHEMA,
            partition_spec=GOLD_PARTITION_SPEC,
            properties=GOLD_TABLE_PROPERTIES
        )
        print(f"Created Gold table: gold.{name}")
    except Exception as e:
        print(f"Error creating Gold table {name}: {e}")
        raise