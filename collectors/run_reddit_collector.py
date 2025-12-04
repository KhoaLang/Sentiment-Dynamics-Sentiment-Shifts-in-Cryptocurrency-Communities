#!/usr/bin/env python3
"""
Runner script for Reddit data collector.
"""

import sys
import os
import logging
import signal
import argparse
from reddit_collector import RedditCollector
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
    parser = argparse.ArgumentParser(description='Reddit Data Collector')
    parser.add_argument('--config', type=str, help='Configuration file path')
    parser.add_argument('--dry-run', action='store_true', help='Run without publishing to Kafka')
    parser.add_argument('--stats', action='store_true', help='Show collector statistics')
    args = parser.parse_args()

    # Load configuration
    config = CollectorConfig.get_all_configs()

    # Setup logging
    setup_logging(config)
    logger = logging.getLogger(__name__)

    try:
        # Create collector
        logger.info("Initializing Reddit collector...")
        collector = RedditCollector(config['reddit'])

        # Setup signal handlers
        signal.signal(signal.SIGINT, lambda s, f: signal_handler(s, f, collector))
        signal.signal(signal.SIGTERM, lambda s, f: signal_handler(s, f, collector))

        if args.stats:
            # Show subreddit statistics
            for subreddit in config['reddit']['subreddits']:
                stats = collector.get_subreddit_stats(subreddit)
                logger.info(f"Subreddit stats for {subreddit}: {stats}")
            return

        if args.dry_run:
            # Dry run - collect but don't publish
            logger.info("Running in dry-run mode - collecting data without publishing...")
            data = collector.collect_data()
            logger.info(f"Collected {len(data)} items in dry-run mode")
            for item in data[:5]:  # Show first 5 items
                logger.info(f"Sample item: {item}")
        else:
            # Normal operation
            logger.info("Starting Reddit data collection...")
            collector.collect_and_produce()

    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down...")
    except Exception as e:
        logger.error(f"Error in Reddit collector: {e}")
        sys.exit(1)
    finally:
        if 'collector' in locals():
            collector.shutdown()


if __name__ == '__main__':
    main()