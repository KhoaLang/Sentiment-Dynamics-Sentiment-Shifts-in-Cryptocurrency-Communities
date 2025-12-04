"""
Bronze layer schema definitions for Apache Iceberg.
Stores raw, immutable social media data with minimal transformation.
"""

from pyiceberg.schema import Schema
from pyiceberg.types import (
    StringType, LongType, DoubleType, TimestampType, BooleanType,
    ListType, MapType, NestedField, StructType
)
from datetime import datetime


# Bronze layer schema - Raw social media data
BRONZE_SCHEMA = Schema(
    NestedField(1, "event_id", StringType(), doc="Unique event identifier"),
    NestedField(2, "event_timestamp", TimestampType(), doc="Event processing timestamp"),
    NestedField(3, "source", StringType(), doc="Data source platform"),
    NestedField(4, "schema_version", StringType(), doc="Schema version"),
    NestedField(5, "data", StructType([
        NestedField(51, "platform", StringType(), doc="Platform name"),
        NestedField(52, "platform_id", StringType(), doc="Original platform ID"),
        NestedField(53, "item_type", StringType(), doc="Item type (post, comment, tweet)"),
        NestedField(54, "title", StringType(), doc="Title if available"),
        NestedField(55, "text", StringType(), doc="Main text content"),
        NestedField(56, "author", StringType(), doc="Author username"),
        NestedField(57, "author_id", StringType(), doc="Author platform ID"),
        NestedField(58, "community", StringType(), doc="Community/subreddit"),
        NestedField(59, "score", DoubleType(), doc="Engagement score"),
        NestedField(60, "upvote_ratio", DoubleType(), doc="Upvote ratio if available"),
        NestedField(61, "comment_count", LongType(), doc="Number of comments"),
        NestedField(62, "retweet_count", LongType(), doc="Retweet count (Twitter)"),
        NestedField(63, "like_count", LongType(), doc="Like count (Twitter)"),
        NestedField(64, "quote_count", LongType(), doc="Quote count (Twitter)"),
        NestedField(65, "view_count", LongType(), doc="View count (Twitter)"),
        NestedField(66, "created_at", TimestampType(), doc="Original creation timestamp"),
        NestedField(67, "url", StringType(), doc="URL to the content"),
        NestedField(68, "permalink", StringType(), doc="Permanent URL"),
        NestedField(69, "language", StringType(), doc="Detected language code"),
        NestedField(70, "text_clean", StringType(), doc="Cleaned text content"),
        NestedField(71, "metadata", StructType([
            NestedField(711, "is_original_content", BooleanType()),
            NestedField(712, "link_flair_text", StringType()),
            NestedField(713, "over_18", BooleanType()),
            NestedField(714, "awards", LongType()),
            NestedField(715, "distinguished", StringType()),
            NestedField(716, "stickied", BooleanType()),
            NestedField(717, "post_id", StringType()),
            NestedField(718, "parent_id", StringType()),
            NestedField(719, "depth", LongType()),
            NestedField(720, "tweet_type", StringType()),
            NestedField(721, "hashtags", ListType(StringType())),
            NestedField(722, "mentions", ListType(StringType())),
            NestedField(723, "media_count", LongType()),
            NestedField(724, "context_annotations", ListType(StringType())),
            NestedField(725, "geo", StringType()),
            NestedField(726, "reply_settings", StringType()),
            NestedField(727, "author_verified", BooleanType()),
            NestedField(728, "author_followers", LongType()),
            NestedField(729, "referenced_tweets", ListType(StringType())),
            NestedField(730, "webhook_headers", MapType(StringType(), StringType())),
            NestedField(731, "original_metadata", MapType(StringType(), StringType())),
            NestedField(732, "source_system", StringType())
        ]))
    ]))
)

# Bronze table properties
BRONZE_TABLE_PROPERTIES = {
    'format-version': '2',
    'write.format.default': 'parquet',
    'write.parquet.compression-codec': 'snappy',
    'write.parquet.row-group-size-bytes': '134217728',  # 128MB
    'write.target-file-size-bytes': '536870912',  # 512MB
    'write.distribution-mode': 'hash',
    'write.fanout-enabled': 'true',
    'read.split.target-size': '134217728',  # 128MB
    'read.split.open-file-cost': '4194304',  # 4MB
    'gc.enabled': 'true',
    'gc.delete-file-type': 'COPY_ON_WRITE',
    'gc.max-file-age-ms': '604800000',  # 7 days
    'snapshot.expire.min-snapshots-to-keep': '3',
    'snapshot.expire.max-snapshots-to-keep': '10',
    'snapshot.expire.max-ref-age-ms': '2592000000'  # 30 days
}

# Bronze partition specification
BRONZE_PARTITION_SPEC = [
    "event_timestamp_day(event_timestamp)"  # Partition by day
]

def create_bronze_table(name: str, catalog) -> None:
    """
    Create Bronze layer Iceberg table.

    Args:
        name: Table name
        catalog: Iceberg catalog instance
    """
    try:
        catalog.create_table(
            identifier=f"bronze.{name}",
            schema=BRONZE_SCHEMA,
            partition_spec=BRONZE_PARTITION_SPEC,
            properties=BRONZE_TABLE_PROPERTIES
        )
        print(f"Created Bronze table: bronze.{name}")
    except Exception as e:
        print(f"Error creating Bronze table {name}: {e}")
        raise