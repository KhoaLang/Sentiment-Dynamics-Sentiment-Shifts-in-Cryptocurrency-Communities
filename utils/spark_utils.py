"""
Spark utility functions for creating and configuring Spark sessions.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from dotenv import load_dotenv

load_dotenv()


def create_spark_session(app_name: str, config: dict = None) -> SparkSession:
    """
    Create and configure Spark session with Iceberg support.

    Args:
        app_name: Name of the Spark application
        config: Configuration dictionary

    Returns:
        Configured SparkSession
    """
    # Default configuration
    default_config = {
        'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
        'spark.sql.catalog.iceberg': 'org.apache.iceberg.spark.SparkCatalog',
        'spark.sql.catalog.iceberg.type': 'rest',
        'spark.sql.catalog.iceberg.uri': os.getenv('ICEBERG_CATALOG_URI', 'http://localhost:8181'),
        'spark.sql.catalog.iceberg.warehouse': os.getenv('ICEBERG_WAREHOUSE', 's3://sentiment-data/warehouse'),
        'spark.sql.catalog.iceberg.io-impl': 'org.apache.iceberg.io.s3.S3FileIO',
        'spark.sql.catalog.iceberg.s3.endpoint': os.getenv('MINIO_ENDPOINT', 'http://localhost:9000'),
        'spark.sql.catalog.iceberg.s3.access-key-id': os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
        'spark.sql.catalog.iceberg.s3.secret-access-key': os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
        'spark.sql.catalog.iceberg.s3.path-style-access': 'true',

        # Hadoop S3A configuration
        'spark.hadoop.fs.s3a.endpoint': os.getenv('MINIO_ENDPOINT', 'http://localhost:9000'),
        'spark.hadoop.fs.s3a.access.key': os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
        'spark.hadoop.fs.s3a.secret.key': os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
        'spark.hadoop.fs.s3a.path.style.access': 'true',
        'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
        'spark.hadoop.fs.s3a.connection.maximum': '100',
        'spark.hadoop.fs.s3a.fast.upload': 'true',

        # Kafka configuration
        'spark.kafka.bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),

        # Performance optimizations
        'spark.sql.adaptive.enabled': 'true',
        'spark.sql.adaptive.coalescePartitions.enabled': 'true',
        'spark.sql.adaptive.skewJoin.enabled': 'true',
        'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
        'spark.sql.inMemoryColumnarStorage.compressed': 'true',
        'spark.sql.parquet.compression.codec': 'snappy',

        # Dynamic allocation (disabled for local development)
        'spark.dynamicAllocation.enabled': 'false',
        'spark.sql.shuffle.partitions': '200'
    }

    # Merge with provided config
    if config:
        default_config.update(config.get('spark.sql.catalog.iceberg', {}))
        default_config.update({f'spark.{k}': v for k, v in config.items() if k != 'spark.sql.catalog.iceberg'})

    # Build Spark session
    builder = SparkSession.builder.appName(app_name)

    # Add configurations
    for key, value in default_config.items():
        builder = builder.config(key, str(value))

    # Add JARs if needed (Iceberg, S3, Kafka)
    jar_paths = os.getenv('SPARK_JARS', '').split(',')
    for jar_path in jar_paths:
        if jar_path.strip():
            builder = builder.config('spark.jars', jar_path.strip())

    # Create session
    spark = builder.getOrCreate()

    # Set log level
    spark.sparkContext.setLogLevel(os.getenv('SPARK_LOG_LEVEL', 'INFO'))

    return spark


def register_iceberg_tables(spark: SparkSession, catalog_name: str = "iceberg"):
    """
    Register Iceberg tables if they exist.

    Args:
        spark: SparkSession instance
        catalog_name: Name of the Iceberg catalog
    """
    try:
        # List namespaces
        namespaces = spark.sql(f"SHOW NAMESPACES IN {catalog_name}").collect()
        print(f"Found namespaces: {[row.namespace for row in namespaces]}")

        # List tables in each namespace
        for ns_row in namespaces:
            namespace = ns_row.namespace
            try:
                tables = spark.sql(f"SHOW TABLES IN {catalog_name}.{namespace}").collect()
                print(f"Tables in {namespace}: {[row.tableName for row in tables]}")
            except Exception as e:
                print(f"Error listing tables in {namespace}: {e}")

    except Exception as e:
        print(f"Error registering Iceberg tables: {e}")


def validate_iceberg_connection(spark: SparkSession, catalog_name: str = "iceberg"):
    """
    Validate Iceberg catalog connection.

    Args:
        spark: SparkSession instance
        catalog_name: Name of the Iceberg catalog

    Returns:
        bool: True if connection is successful
    """
    try:
        # Test catalog connection
        spark.sql(f"SELECT current_namespace() FROM {catalog_name}.information_schema.tables LIMIT 1").collect()
        print("Iceberg catalog connection successful")
        return True
    except Exception as e:
        print(f"Iceberg catalog connection failed: {e}")
        return False


def get_table_info(spark: SparkSession, table_name: str):
    """
    Get information about an Iceberg table.

    Args:
        spark: SparkSession instance
        table_name: Full table name (catalog.namespace.table)
    """
    try:
        # Get table schema
        print(f"\nSchema for {table_name}:")
        spark.sql(f"DESCRIBE {table_name}").show(50, truncate=False)

        # Get table properties
        print(f"\nProperties for {table_name}:")
        spark.sql(f"SELECT * FROM {table_name}.properties").show(truncate=False)

        # Get current snapshot
        print(f"\nCurrent snapshot for {table_name}:")
        spark.sql(f"SELECT * FROM {table_name}.snapshots ORDER BY committed_at DESC LIMIT 1").show(truncate=False)

    except Exception as e:
        print(f"Error getting table info for {table_name}: {e}")


def optimize_iceberg_table(spark: SparkSession, table_name: str):
    """
    Optimize an Iceberg table by compacting files.

    Args:
        spark: SparkSession instance
        table_name: Full table name (catalog.namespace.table)
    """
    try:
        print(f"Optimizing table: {table_name}")

        # Run compact
        spark.sql(f"ALTER TABLE {table_name} COMPACT")

        # Run rewrite data files
        spark.sql(f"ALTER TABLE {table_name} REWRITE DATA FILES")

        print(f"Optimization completed for {table_name}")

    except Exception as e:
        print(f"Error optimizing table {table_name}: {e}")


def expire_snapshots(spark: SparkSession, table_name: str, older_than_days: int = 30):
    """
    Expire old snapshots for an Iceberg table.

    Args:
        spark: SparkSession instance
        table_name: Full table name (catalog.namespace.table)
        older_than_days: Expire snapshots older than this many days
    """
    try:
        print(f"Expiring snapshots older than {older_than_days} days for {table_name}")

        # Expire snapshots
        spark.sql(f"ALTER TABLE {table_name} EXPIRE SNAPSHOTS OLDER_THAN '{older_than_days}d'")

        print(f"Snapshot expiration completed for {table_name}")

    except Exception as e:
        print(f"Error expiring snapshots for {table_name}: {e}")