"""
Base collector class for all social media data sources.
Provides common functionality for Kafka production and data processing.
"""

import json
import time
import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
from confluent_kafka import Producer, KafkaError, KafkaException
import pytz
import uuid
import os
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BaseCollector(ABC):
    """Base class for social media data collectors."""

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the collector with configuration.

        Args:
            config: Configuration dictionary containing Kafka settings
        """
        self.config = config
        self.producer = self._create_kafka_producer()
        self.topic = config.get('topic')
        self.running = False
        self.collected_count = 0
        self.error_count = 0

    def _create_kafka_producer(self) -> Producer:
        """Create and configure Kafka producer."""
        producer_config = {
            'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
            'client.id': f"{self.__class__.__name__}_{os.getpid()}",
            'acks': 'all',
            'retries': 3,
            'max.in.flight.requests.per.connection': 1,
            'enable.idempotence': True,
            'compression.type': 'snappy',
            'linger.ms': 10,
            'batch.size': 16384,
            'queue.buffering.max.messages': 100000,
            'queue.buffering.max.ms': 1000,
            'delivery.timeout.ms': 30000,
            'request.timeout.ms': 20000
        }

        return Producer(producer_config)

    @abstractmethod
    def collect_data(self) -> List[Dict[str, Any]]:
        """
        Collect data from the social media platform.
        Must be implemented by subclasses.

        Returns:
            List of collected data items
        """
        pass

    @abstractmethod
    def transform_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform raw data into standardized format.
        Must be implemented by subclasses.

        Args:
            raw_data: Raw data from social media platform

        Returns:
            Transformed data in standardized format
        """
        pass

    def _delivery_report(self, err: Optional[KafkaError], msg: Any) -> None:
        """
        Callback for message delivery reports.

        Args:
            err: Error if delivery failed, None if successful
            msg: Message object
        """
        if err is not None:
            logger.error(f'Message delivery failed: {err}')
            self.error_count += 1
        else:
            self.collected_count += 1
            if self.collected_count % 100 == 0:
                logger.info(f'Successfully delivered {self.collected_count} messages')

    def _create_envelope(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create standardized envelope for all messages.

        Args:
            data: Transformed data item

        Returns:
            Envelope with metadata
        """
        envelope = {
            'event_id': str(uuid.uuid4()),
            'event_timestamp': datetime.now(timezone.utc).isoformat(),
            'source': self.__class__.__name__.replace('Collector', '').lower(),
            'schema_version': '1.0',
            'data': data
        }

        return envelope

    def produce_to_kafka(self, data: Dict[str, Any]) -> None:
        """
        Produce data to Kafka topic.

        Args:
            data: Data to produce
        """
        try:
            # Create envelope
            envelope = self._create_envelope(data)

            # Serialize to JSON
            message = json.dumps(envelope, default=str)

            # Produce to Kafka
            self.producer.produce(
                topic=self.topic,
                value=message.encode('utf-8'),
                key=data.get('id', str(uuid.uuid4())),
                callback=self._delivery_report
            )

            # Trigger delivery reports
            self.producer.poll(0)

        except KafkaException as e:
            logger.error(f'Kafka error: {e}')
            self.error_count += 1
        except Exception as e:
            logger.error(f'Unexpected error producing message: {e}')
            self.error_count += 1

    def collect_and_produce(self) -> None:
        """Main collection loop."""
        logger.info(f'Starting {self.__class__.__name__}...')
        self.running = True

        try:
            while self.running:
                try:
                    # Collect data
                    raw_data_list = self.collect_data()

                    if raw_data_list:
                        for raw_data in raw_data_list:
                            # Transform data
                            transformed_data = self.transform_data(raw_data)

                            # Produce to Kafka
                            self.produce_to_kafka(transformed_data)

                    # Sleep to avoid rate limiting
                    time.sleep(self.config.get('sleep_interval', 1))

                except KeyboardInterrupt:
                    logger.info('Received interrupt signal, shutting down...')
                    self.running = False
                    break
                except Exception as e:
                    logger.error(f'Error in collection loop: {e}')
                    self.error_count += 1
                    time.sleep(5)  # Back off on error

        finally:
            self.shutdown()

    def shutdown(self) -> None:
        """Clean shutdown of the collector."""
        logger.info(f'Shutting down {self.__class__.__name__}...')
        self.running = False

        # Flush producer
        if self.producer:
            try:
                remaining = self.producer.flush(timeout=10)
                if remaining > 0:
                    logger.warning(f'{remaining} messages were not delivered')
            except Exception as e:
                logger.error(f'Error flushing producer: {e}')

        logger.info(f'Collector stats - Collected: {self.collected_count}, Errors: {self.error_count}')
        logger.info(f'{self.__class__.__name__} shutdown complete')

    def get_stats(self) -> Dict[str, Any]:
        """Get collector statistics."""
        return {
            'collected_count': self.collected_count,
            'error_count': self.error_count,
            'running': self.running,
            'topic': self.topic,
            'success_rate': self.collected_count / max(1, self.collected_count + self.error_count)
        }