#!/usr/bin/env python3
"""
Runner script for webhook collector.
"""

import sys
import os
import logging
import argparse
from webhook_collector import WebhookCollector
from collector_config import CollectorConfig

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def setup_logging():
    """Setup logging configuration."""
    import os
    from dotenv import load_dotenv
    load_dotenv()

    log_level = os.getenv('LOG_LEVEL', 'INFO')
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    # Configure logging
    logging.basicConfig(
        level=getattr(logging, log_level),
        format=log_format,
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )


def main():
    """Main runner function."""
    parser = argparse.ArgumentParser(description='Webhook Data Collector')
    parser.add_argument('--host', type=str, default='0.0.0.0', help='Host to bind to')
    parser.add_argument('--port', type=int, default=8000, help='Port to bind to')
    parser.add_argument('--config', type=str, help='Configuration file path')
    args = parser.parse_args()

    # Setup logging
    setup_logging()
    logger = logging.getLogger(__name__)

    try:
        # Load configuration
        config = CollectorConfig.get_webhook_config()

        # Create collector
        logger.info("Initializing webhook collector...")
        logger.info(f"Starting server on {args.host}:{args.port}")
        logger.info(f"Topic: {config['topic']}")
        logger.info(f"Auth required: {config['require_auth']}")
        logger.info(f"Rate limit: {config['rate_limit']} requests/minute")

        collector = WebhookCollector(config)

        # Start server
        collector.start(host=args.host, port=args.port)

    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down...")
    except Exception as e:
        logger.error(f"Error in webhook collector: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()