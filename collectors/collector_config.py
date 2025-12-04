"""
Configuration management for data collectors.
"""

import os
import yaml
from typing import Dict, Any, List
from dotenv import load_dotenv

load_dotenv()


class CollectorConfig:
    """Configuration manager for data collectors."""

    @staticmethod
    def get_reddit_config() -> Dict[str, Any]:
        """Get Reddit collector configuration."""
        return {
            'topic': 'reddit_raw',
            'subreddits': [
                'cryptocurrency',
                'Bitcoin',
                'ethereum',
                'CryptoMarkets',
                'CryptoCurrencyTrading',
                'binance',
                'Cardano',
                'solana',
                'dogecoin',
                'Chainlink'
            ],
            'keywords': [
                'bitcoin', 'btc', 'ethereum', 'eth', 'cryptocurrency',
                'crypto', 'blockchain', 'defi', 'nft', 'altcoin',
                'trading', 'investing', 'price', 'market', 'bull',
                'bear', 'hodl', 'satoshi', 'wallet', 'mining'
            ],
            'post_limit': 100,
            'comment_limit': 20,
            'min_score': 3,
            'sleep_interval': 2
        }

    @staticmethod
    def get_twitter_config() -> Dict[str, Any]:
        """Get Twitter collector configuration."""
        return {
            'topic': 'twitter_raw',
            'keywords': [
                'bitcoin', 'btc', 'ethereum', 'eth', 'cryptocurrency',
                'crypto', 'blockchain', 'defi', 'nft', 'altcoin',
                'trading', 'investing', 'price', 'market', 'bullish',
                'bearish', 'hodl', 'satoshi', 'wallet', 'mining',
                'solana', 'sol', 'cardano', 'ada', 'dogecoin', 'doge'
            ],
            'hashtags': [
                '#bitcoin', '#btc', '#ethereum', '#eth', '#crypto',
                '#cryptocurrency', '#blockchain', '#defi', '#nft',
                '#trading', '#investing', '#altcoin', '#solana',
                '#cardano', '#dogecoin'
            ],
            'usernames': [
                'elonmusk', 'VitalikButerin', 'saylor', 'cz_binance',
                'APompliano', 'bgarlinghouse', 'IOHK_Charles', 'justinsuntron',
                'rogerkver', 'BarrySilbert', 'mikeinwaukesha', 'DocumentingBTC'
            ],
            'sleep_interval': 1
        }

    @staticmethod
    def get_webhook_config() -> Dict[str, Any]:
        """Get webhook collector configuration."""
        return {
            'topic': 'webhook_raw',
            'require_auth': os.getenv('WEBHOOK_REQUIRE_AUTH', 'false').lower() == 'true',
            'api_key': os.getenv('WEBHOOK_API_KEY'),
            'rate_limit': int(os.getenv('WEBHOOK_RATE_LIMIT', '100'))
        }

    @staticmethod
    def get_kafka_config() -> Dict[str, Any]:
        """Get Kafka configuration."""
        return {
            'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
            'schema.registry.url': os.getenv('KAFKA_SCHEMA_REGISTRY_URL', 'http://localhost:8081'),
            'client.id': f"collector_{os.getpid()}"
        }

    @staticmethod
    def load_topics_config() -> Dict[str, Any]:
        """Load Kafka topics configuration from YAML file."""
        try:
            with open('config/kafka_topics.yaml', 'r') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            # Return default topics configuration
            return {
                'topics': {
                    'reddit_raw': {'name': 'reddit_raw', 'partitions': 6, 'replication_factor': 1},
                    'twitter_raw': {'name': 'twitter_raw', 'partitions': 6, 'replication_factor': 1},
                    'webhook_raw': {'name': 'webhook_raw', 'partitions': 3, 'replication_factor': 1},
                    'enriched_sentiment': {'name': 'enriched_sentiment', 'partitions': 6, 'replication_factor': 1},
                    'dead_letter_queue': {'name': 'dead_letter_queue', 'partitions': 3, 'replication_factor': 1}
                }
            }

    @staticmethod
    def get_logging_config() -> Dict[str, Any]:
        """Get logging configuration."""
        return {
            'level': os.getenv('LOG_LEVEL', 'INFO'),
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            'file': os.getenv('LOG_FILE', 'logs/collectors.log')
        }

    @staticmethod
    def get_all_configs() -> Dict[str, Dict[str, Any]]:
        """Get all collector configurations."""
        return {
            'reddit': CollectorConfig.get_reddit_config(),
            'twitter': CollectorConfig.get_twitter_config(),
            'webhook': CollectorConfig.get_webhook_config(),
            'kafka': CollectorConfig.get_kafka_config(),
            'topics': CollectorConfig.load_topics_config(),
            'logging': CollectorConfig.get_logging_config()
        }