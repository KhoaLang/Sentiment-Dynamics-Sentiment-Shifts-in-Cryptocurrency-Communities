#!/usr/bin/env python3
"""
Runner script for Twitter data collector.
"""

import sys
import os
import logging
import signal
import argparse
from twitter_collector import TwitterCollector
from collector_config import CollectorConfig

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def setup_logging(config):
    """Setup logging configuration."""
    log_config = config['logging']

    # Create logs directory if it doesn't exist
    log_file = log_config.get('file', 'logs/collectors.log')
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    # Configure logging
    logging.basicConfig(
        level=getattr(logging, log_config.get('level', 'INFO')),
        format=log_config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s'),
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )


def signal_handler(signum, frame, collector):
    """Handle shutdown signals."""
    logging.info(f"Received signal {signum}, shutting down collector...")
    collector.running = False
    collector.shutdown()
    sys.exit(0)


def main():
    """Main runner function."""
    parser = argparse.ArgumentParser(description='Twitter Data Collector')
    parser.add_argument('--config', type=str, help='Configuration file path')
    parser.add_argument('--dry-run', action='store_true', help='Run without publishing to Kafka')
    parser.add_argument('--keywords', nargs='+', help='Override keywords for collection')
    parser.add_argument('--hashtags', nargs='+', help='Override hashtags for collection')
    parser.add_argument('--users', nargs='+', help='Override user timelines for collection')
    args = parser.parse_args()

    # Load configuration
    config = CollectorConfig.get_all_configs()

    # Override config with command line arguments
    if args.keywords:
        config['twitter']['keywords'] = args.keywords
    if args.hashtags:
        config['twitter']['hashtags'] = args.hashtags
    if args.users:
        config['twitter']['usernames'] = args.users

    # Setup logging
    setup_logging(config)
    logger = logging.getLogger(__name__)

    try:
        # Create collector
        logger.info("Initializing Twitter collector...")
        logger.info(f"Keywords: {config['twitter']['keywords']}")
        logger.info(f"Hashtags: {config['twitter']['hashtags']}")
        logger.info(f"Users: {config['twitter']['usernames']}")

        collector = TwitterCollector(config['twitter'])

        # Setup signal handlers
        signal.signal(signal.SIGINT, lambda s, f: signal_handler(s, f, collector))
        signal.signal(signal.SIGTERM, lambda s, f: signal_handler(s, f, collector))

        if args.dry_run:
            # Dry run - collect but don't publish
            logger.info("Running in dry-run mode - collecting data without publishing...")
            data = collector.collect_data()
            logger.info(f"Collected {len(data)} items in dry-run mode")
            for item in data[:3]:  # Show first 3 items
                logger.info(f"Sample tweet: {item.get('text', 'No text')[:200]}...")
        else:
            # Normal operation
            logger.info("Starting Twitter data collection...")
            collector.collect_and_produce()

    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down...")
    except Exception as e:
        logger.error(f"Error in Twitter collector: {e}")
        sys.exit(1)
    finally:
        if 'collector' in locals():
            collector.shutdown()


if __name__ == '__main__':
    main()