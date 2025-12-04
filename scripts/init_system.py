#!/usr/bin/env python3
"""
System initialization script.
Sets up all required infrastructure: Kafka topics, Iceberg tables, and configurations.
"""

import os
import sys
import logging
import yaml
import json
import time
import requests
from typing import Dict, Any, List
from pyspark.sql import SparkSession

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.spark_utils import create_spark_session
from storage.schemas.bronze_schema import create_bronze_table, BRONZE_SCHEMA
from storage.schemas.silver_schema import create_silver_table, SILVER_SCHEMA
from storage.schemas.gold_schema import create_gold_table, GOLD_SCHEMA

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SystemInitializer:
    """Initialize all system components."""

    def __init__(self):
        """Initialize system initializer."""
        self.spark = None
        self.catalog = None
        self.kafka_admin = None

    def wait_for_services(self):
        """Wait for all services to be available."""
        services = {
            'Kafka': f"{os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')}",
            'Schema Registry': f"{os.getenv('KAFKA_SCHEMA_REGISTRY_URL', 'http://localhost:8081')}",
            'MinIO': f"{os.getenv('MINIO_ENDPOINT', 'localhost:9000')}",
            'Iceberg Catalog': f"{os.getenv('ICEBERG_CATALOG_URI', 'http://localhost:8181')}",
            'OpenSearch': f"{os.getenv('OPENSEARCH_HOSTS', 'http://localhost:9200')}",
            'Milvus': f"{os.getenv('MILVUS_HOST', 'localhost')}:{os.getenv('MILVUS_PORT', '19530')}"
        }

        for service_name, url in services.items():
            self._wait_for_service(service_name, url)

    def _wait_for_service(self, service_name: str, url: str, timeout: int = 300):
        """Wait for a service to be available."""
        logger.info(f"Waiting for {service_name} at {url}")

        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                if service_name == 'Kafka':
                    # Try to connect to Kafka
                    from kafka import KafkaAdminClient
                    admin = KafkaAdminClient(
                        bootstrap_servers=url,
                        request_timeout_ms=5000
                    )
                    admin.list_topics()
                    admin.close()
                    logger.info(f"{service_name} is ready")
                    return

                elif service_name == 'Schema Registry':
                    # Try to connect to Schema Registry
                    response = requests.get(f"{url}/subjects", timeout=5)
                    if response.status_code == 200:
                        logger.info(f"{service_name} is ready")
                        return

                elif service_name == 'MinIO':
                    # Try to connect to MinIO
                    from minio import Minio
                    client = Minio(
                        url.replace('http://', '').replace('https://', ''),
                        access_key=os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
                        secret_key=os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
                        secure=False
                    )
                    client.list_buckets()
                    logger.info(f"{service_name} is ready")
                    return

                elif service_name == 'Iceberg Catalog':
                    # Try to connect to Iceberg catalog
                    response = requests.get(f"{url}/v1/config", timeout=5)
                    if response.status_code == 200:
                        logger.info(f"{service_name} is ready")
                        return

                elif service_name == 'OpenSearch':
                    # Try to connect to OpenSearch
                    response = requests.get(f"{url}/_cluster/health", timeout=5)
                    if response.status_code == 200:
                        logger.info(f"{service_name} is ready")
                        return

                elif service_name == 'Milvus':
                    # Try to connect to Milvus
                    from pymilvus import connections
                    host, port = url.split(':')
                    connections.connect("default", host=host, port=int(port))
                    connections.disconnect("default")
                    logger.info(f"{service_name} is ready")
                    return

            except Exception as e:
                logger.debug(f"Waiting for {service_name}: {e}")
                time.sleep(5)

        raise Exception(f"Timeout waiting for {service_name}")

    def initialize_kafka_topics(self):
        """Create Kafka topics."""
        logger.info("Initializing Kafka topics...")

        try:
            from kafka import KafkaAdminClient
            from kafka.admin import ConfigResource, ConfigResourceType, NewTopic
            from kafka.admin import KafkaAdminClient

            admin = KafkaAdminClient(
                bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
                request_timeout_ms=30000
            )

            # Load topics configuration
            with open('config/kafka_topics.yaml', 'r') as f:
                topics_config = yaml.safe_load(f)

            # Create topics
            topics_to_create = []
            for topic_name, topic_config in topics_config['topics'].items():
                topics_to_create.append(NewTopic(
                    name=topic_config['name'],
                    num_partitions=topic_config['partitions'],
                    replication_factor=topic_config['replication_factor']
                ))

            if topics_to_create:
                admin.create_topics(topics_to_create)
                logger.info(f"Created {len(topics_to_create)} Kafka topics")

            # Configure topics
            for topic_name, topic_config in topics_config['topics'].items():
                if 'config' in topic_config:
                    resource = ConfigResource(ConfigResourceType.TOPIC, topic_config['name'])
                    configs = {k: str(v) for k, v in topic_config['config'].items()}
                    admin.alter_configs([ConfigResource(resource, configs)])

            admin.close()
            logger.info("Kafka topics initialization completed")

        except Exception as e:
            logger.error(f"Error initializing Kafka topics: {e}")
            raise

    def initialize_minio_buckets(self):
        """Create MinIO buckets."""
        logger.info("Initializing MinIO buckets...")

        try:
            from minio import Minio

            client = Minio(
                os.getenv('MINIO_ENDPOINT', 'localhost:9000').replace('http://', ''),
                access_key=os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
                secret_key=os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
                secure=False
            )

            # Create buckets
            buckets = [
                os.getenv('MINIO_BUCKET_NAME', 'sentiment-data'),
                'sentiment-warehouse',
                'sentiment-checkpoints',
                'sentiment-models'
            ]

            for bucket in buckets:
                if not client.bucket_exists(bucket):
                    client.make_bucket(bucket)
                    logger.info(f"Created bucket: {bucket}")
                else:
                    logger.info(f"Bucket already exists: {bucket}")

        except Exception as e:
            logger.error(f"Error initializing MinIO buckets: {e}")
            raise

    def initialize_iceberg_tables(self):
        """Create Iceberg tables for all layers."""
        logger.info("Initializing Iceberg tables...")

        try:
            # Create Spark session
            self.spark = create_spark_session("System Initialization")

            # Get catalog
            self.catalog = self.spark.conf.get("spark.sql.catalog.iceberg")

            # Create namespaces
            namespaces = ['bronze', 'silver', 'gold']
            for namespace in namespaces:
                try:
                    self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {self.catalog}.{namespace}")
                    logger.info(f"Created namespace: {namespace}")
                except Exception as e:
                    logger.warning(f"Error creating namespace {namespace}: {e}")

            # Create Bronze tables
            bronze_tables = ['social_media_raw']
            for table_name in bronze_tables:
                try:
                    create_bronze_table(table_name, self.spark)
                except Exception as e:
                    logger.warning(f"Error creating Bronze table {table_name}: {e}")

            # Create Silver tables
            silver_tables = ['sentiment_enriched', 'author_analysis', 'content_enrichment']
            for table_name in silver_tables:
                try:
                    create_silver_table(table_name, self.spark)
                except Exception as e:
                    logger.warning(f"Error creating Silver table {table_name}: {e}")

            # Create Gold tables
            gold_tables = ['sentiment_metrics', 'engagement_analytics', 'trending_analysis']
            for table_name in gold_tables:
                try:
                    create_gold_table(table_name, self.spark)
                except Exception as e:
                    logger.warning(f"Error creating Gold table {table_name}: {e}")

            logger.info("Iceberg tables initialization completed")

        except Exception as e:
            logger.error(f"Error initializing Iceberg tables: {e}")
            raise

    def initialize_opensearch_indices(self):
        """Create OpenSearch indices."""
        logger.info("Initializing OpenSearch indices...")

        try:
            from opensearchpy import OpenSearch

            client = OpenSearch(
                hosts=[{'host': 'localhost', 'port': 9200}],
                http_auth=('admin', 'admin'),
                use_ssl=False,
                verify_certs=False
            )

            # Create indices
            indices = [
                {
                    'name': 'sentiment-search',
                    'mappings': {
                        'properties': {
                            'platform': {'type': 'keyword'},
                            'text': {'type': 'text'},
                            'sentiment_label': {'type': 'keyword'},
                            'sentiment_score': {'type': 'float'},
                            'created_at': {'type': 'date'},
                            'community': {'type': 'keyword'}
                        }
                    }
                },
                {
                    'name': 'real-time-metrics',
                    'mappings': {
                        'properties': {
                            'timestamp': {'type': 'date'},
                            'platform': {'type': 'keyword'},
                            'sentiment_score': {'type': 'float'},
                            'volume': {'type': 'integer'}
                        }
                    }
                }
            ]

            for index_config in indices:
                if not client.indices.exists(index=index_config['name']):
                    client.indices.create(
                        index=index_config['name'],
                        body={'mappings': index_config['mappings']}
                    )
                    logger.info(f"Created OpenSearch index: {index_config['name']}")
                else:
                    logger.info(f"OpenSearch index already exists: {index_config['name']}")

        except Exception as e:
            logger.error(f"Error initializing OpenSearch indices: {e}")
            raise

    def initialize_milvus_collections(self):
        """Create Milvus collections for vector storage."""
        logger.info("Initializing Milvus collections...")

        try:
            from pymilvus import connections, Collection, FieldSchema, CollectionSchema, DataType

            # Connect to Milvus
            connections.connect(
                alias="default",
                host=os.getenv('MILVUS_HOST', 'localhost'),
                port=int(os.getenv('MILVUS_PORT', '19530'))
            )

            # Define collection schema
            fields = [
                FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=True),
                FieldSchema(name="event_id", dtype=DataType.VARCHAR, max_length=100),
                FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=384),
                FieldSchema(name="platform", dtype=DataType.VARCHAR, max_length=50),
                FieldSchema(name="sentiment_label", dtype=DataType.VARCHAR, max_length=20),
                FieldSchema(name="created_at", dtype=DataType.INT64)
            ]

            schema = CollectionSchema(
                fields=fields,
                description="Sentiment text embeddings"
            )

            # Create collection
            collection_name = "sentiment_embeddings"
            if not Collection.has_collection(collection_name):
                collection = Collection(
                    name=collection_name,
                    schema=schema
                )
                logger.info(f"Created Milvus collection: {collection_name}")
            else:
                logger.info(f"Milvus collection already exists: {collection_name}")

            # Create index
            index_params = {
                "metric_type": "COSINE",
                "index_type": "IVF_FLAT",
                "params": {"nlist": 128}
            }

            collection = Collection(collection_name)
            if not collection.has_index():
                collection.create_index(
                    field_name="embedding",
                    index_params=index_params
                )
                logger.info(f"Created index for collection: {collection_name}")

        except Exception as e:
            logger.error(f"Error initializing Milvus collections: {e}")
            raise

    def create_sample_data(self):
        """Create sample data for testing."""
        logger.info("Creating sample data...")

        try:
            # Create sample records
            sample_data = [
                {
                    "event_id": "sample_001",
                    "processing_timestamp": "2024-01-01T12:00:00Z",
                    "platform": "reddit",
                    "platform_id": "abc123",
                    "item_type": "post",
                    "title": "Bitcoin is reaching new heights!",
                    "text_original": "I can't believe how well Bitcoin is doing today. Everyone should buy now!",
                    "text_clean": "believe well bitcoin doing today everyone buy now",
                    "author": "crypto_enthusiast",
                    "author_id": "user123",
                    "community": "cryptocurrency",
                    "created_at": "2024-01-01T11:45:00Z",
                    "url": "https://reddit.com/r/cryptocurrency/abc123",
                    "permalink": "https://reddit.com/r/cryptocurrency/abc123",
                    "language": "en",
                    "score": 125.5,
                    "upvote_ratio": 0.95,
                    "comment_count": 45,
                    "retweet_count": 0,
                    "like_count": 0,
                    "quote_count": 0,
                    "view_count": 0
                }
            ]

            # Insert into Bronze table
            if self.spark:
                bronze_df = self.spark.createDataFrame([sample_data[0]])
                bronze_df.write \
                    .format("iceberg") \
                    .mode("append") \
                    .save("iceberg.bronze.social_media_raw")

                logger.info("Created sample data in Bronze layer")

        except Exception as e:
            logger.error(f"Error creating sample data: {e}")

    def run_initialization(self):
        """Run complete system initialization."""
        logger.info("Starting system initialization...")

        try:
            # Wait for services
            self.wait_for_services()

            # Initialize components
            self.initialize_kafka_topics()
            self.initialize_minio_buckets()
            self.initialize_iceberg_tables()
            self.initialize_opensearch_indices()
            self.initialize_milvus_collections()
            self.create_sample_data()

            logger.info("System initialization completed successfully!")

        except Exception as e:
            logger.error(f"System initialization failed: {e}")
            raise
        finally:
            if self.spark:
                self.spark.stop()


def main():
    """Main entry point."""
    initializer = SystemInitializer()
    initializer.run_initialization()


if __name__ == '__main__':
    main()