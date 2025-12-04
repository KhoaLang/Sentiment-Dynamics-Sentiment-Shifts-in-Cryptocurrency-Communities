"""
Webhook collector for receiving data from external sources via HTTP endpoints.
Uses FastAPI to create webhook endpoints that ingest data and publish to Kafka.
"""

from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional
import uvicorn
import logging
import asyncio
from datetime import datetime, timezone
import json
import hashlib
from base_collector import BaseCollector
from confluent_kafka import KafkaError
import os
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# Pydantic models for webhook validation
class WebhookData(BaseModel):
    id: Optional[str] = Field(None, description="Unique identifier for the data item")
    text: str = Field(..., description="Text content for sentiment analysis")
    title: Optional[str] = Field(None, description="Optional title")
    author: Optional[str] = Field(None, description="Author name")
    source: str = Field(..., description="Source platform or system")
    timestamp: Optional[datetime] = Field(None, description="Timestamp of the data")
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Additional metadata")
    url: Optional[str] = Field(None, description="Associated URL")

class WebhookCollector(BaseCollector):
    """Webhook collector using FastAPI."""

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize webhook collector.

        Args:
            config: Configuration dictionary
        """
        # Initialize base collector but skip producer creation (will be done in start)
        self.config = config
        self.producer = None
        self.topic = config.get('topic', 'webhook_raw')
        self.running = False
        self.collected_count = 0
        self.error_count = 0

        # FastAPI app
        self.app = FastAPI(
            title="Sentiment Analysis Webhook Collector",
            description="Webhook endpoints for ingesting social media data",
            version="1.0.0"
        )

        # Setup routes
        self._setup_routes()

        # Authentication
        self.api_key = config.get('api_key')
        self.require_auth = config.get('require_auth', False)

        # Rate limiting
        self.rate_limit = config.get('rate_limit', 100)  # requests per minute
        self.request_counts = {}

    def _setup_routes(self):
        """Setup FastAPI routes."""

        @self.app.post("/ingest")
        async def ingest_data(
            data: WebhookData,
            request: Request,
            background_tasks: BackgroundTasks
        ):
            """Main endpoint for data ingestion."""
            try:
                # Authentication
                if self.require_auth:
                    await self._authenticate(request)

                # Rate limiting
                await self._check_rate_limit(request)

                # Queue for background processing
                background_tasks.add_task(
                    self._process_webhook_data,
                    data.dict(),
                    dict(request.headers)
                )

                return JSONResponse(
                    status_code=202,
                    content={
                        "status": "accepted",
                        "message": "Data accepted for processing",
                        "received_at": datetime.now(timezone.utc).isoformat()
                    }
                )

            except HTTPException:
                raise
            except Exception as e:
                logger.error(f"Error processing webhook: {e}")
                raise HTTPException(
                    status_code=500,
                    detail="Internal server error"
                )

        @self.app.post("/ingest/batch")
        async def ingest_batch(
            data: List[WebhookData],
            request: Request,
            background_tasks: BackgroundTasks
        ):
            """Batch endpoint for multiple data items."""
            try:
                # Authentication
                if self.require_auth:
                    await self._authenticate(request)

                # Rate limiting for batches
                await self._check_rate_limit(request, multiplier=len(data))

                # Queue batch for background processing
                background_tasks.add_task(
                    self._process_webhook_batch,
                    [item.dict() for item in data],
                    dict(request.headers)
                )

                return JSONResponse(
                    status_code=202,
                    content={
                        "status": "accepted",
                        "message": f"Batch of {len(data)} items accepted for processing",
                        "received_at": datetime.now(timezone.utc).isoformat()
                    }
                )

            except HTTPException:
                raise
            except Exception as e:
                logger.error(f"Error processing webhook batch: {e}")
                raise HTTPException(
                    status_code=500,
                    detail="Internal server error"
                )

        @self.app.get("/health")
        async def health_check():
            """Health check endpoint."""
            stats = self.get_stats()
            return JSONResponse(
                status_code=200,
                content={
                    "status": "healthy",
                    "running": self.running,
                    "stats": stats,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
            )

        @self.app.get("/stats")
        async def get_collector_stats(request: Request):
            """Get collector statistics."""
            if self.require_auth:
                await self._authenticate(request)

            stats = self.get_stats()
            return JSONResponse(status_code=200, content=stats)

    async def _authenticate(self, request: Request):
        """Authenticate incoming requests."""
        if not self.api_key:
            return

        # Check API key in headers
        api_key = request.headers.get("X-API-Key")
        if not api_key or api_key != self.api_key:
            raise HTTPException(
                status_code=401,
                detail="Invalid or missing API key"
            )

    async def _check_rate_limit(self, request: Request, multiplier: int = 1):
        """Simple rate limiting based on client IP."""
        if self.rate_limit <= 0:
            return

        client_ip = request.client.host
        now = datetime.now()

        # Clean old entries
        cutoff = now.replace(second=0, microsecond=0)
        if client_ip in self.request_counts:
            self.request_counts[client_ip] = [
                timestamp for timestamp in self.request_counts[client_ip]
                if timestamp >= cutoff
            ]
        else:
            self.request_counts[client_ip] = []

        # Check rate limit
        if len(self.request_counts[client_ip]) + multiplier > self.rate_limit:
            raise HTTPException(
                status_code=429,
                detail="Rate limit exceeded"
            )

        # Add current request
        for _ in range(multiplier):
            self.request_counts[client_ip].append(now)

    async def _process_webhook_data(self, data: Dict[str, Any], headers: Dict[str, str]):
        """Process individual webhook data item."""
        try:
            # Transform data
            transformed_data = self.transform_data(data, headers)

            # Produce to Kafka
            await self._async_produce_to_kafka(transformed_data)

        except Exception as e:
            logger.error(f"Error processing webhook data: {e}")
            self.error_count += 1

    async def _process_webhook_batch(self, data_batch: List[Dict[str, Any]], headers: Dict[str, str]):
        """Process batch of webhook data items."""
        for data in data_batch:
            await self._process_webhook_data(data, headers)

    async def _async_produce_to_kafka(self, data: Dict[str, Any]):
        """Asynchronously produce data to Kafka."""
        if not self.producer:
            self.producer = self._create_kafka_producer()

        try:
            # Create envelope
            envelope = self._create_envelope(data)

            # Serialize to JSON
            message = json.dumps(envelope, default=str)

            # Produce to Kafka
            self.producer.produce(
                topic=self.topic,
                value=message.encode('utf-8'),
                key=data.get('id', str(hashlib.md5(str(data).encode()).hexdigest())),
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

    def transform_data(self, raw_data: Dict[str, Any], headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """
        Transform webhook data into standardized format.

        Args:
            raw_data: Raw webhook data
            headers: HTTP headers (optional)

        Returns:
            Transformed data
        """
        # Generate ID if not provided
        if not raw_data.get('id'):
            raw_data['id'] = hashlib.md5(
                f"{raw_data.get('text', '')}{raw_data.get('timestamp', '')}".encode()
            ).hexdigest()[:16]

        transformed = {
            'platform': raw_data.get('source', 'webhook'),
            'platform_id': raw_data['id'],
            'item_type': raw_data.get('item_type', 'post'),
            'title': raw_data.get('title', ''),
            'text': raw_data['text'],
            'author': raw_data.get('author'),
            'author_id': None,
            'community': None,
            'score': 0,  # Webhook data doesn't have scores
            'upvote_ratio': None,
            'comment_count': 0,
            'retweet_count': 0,
            'like_count': 0,
            'quote_count': 0,
            'view_count': 0,
            'created_at': raw_data.get('timestamp', datetime.now(timezone.utc).isoformat()),
            'url': raw_data.get('url'),
            'permalink': raw_data.get('url'),
            'language': None,  # Will be detected in preprocessing
            'text_clean': None,  # Will be cleaned in preprocessing
            'metadata': {
                'webhook_headers': headers or {},
                'original_metadata': raw_data.get('metadata', {}),
                'source_system': raw_data.get('source')
            }
        }

        return transformed

    def start(self, host: str = "0.0.0.0", port: int = 8000):
        """Start the webhook server."""
        logger.info(f"Starting webhook collector on {host}:{port}")

        # Initialize producer
        self.producer = self._create_kafka_producer()
        self.running = True

        # Start server
        uvicorn.run(
            self.app,
            host=host,
            port=port,
            log_level="info"
        )

    def stop(self):
        """Stop the webhook server."""
        logger.info("Stopping webhook collector...")
        self.running = False

        if self.producer:
            self.producer.flush(timeout=10)

    def collect_data(self) -> List[Dict[str, Any]]:
        """Not used in webhook collector - data comes via HTTP."""
        return []

    def collect_and_produce(self) -> None:
        """Not used in webhook collector - use start() instead."""
        pass


# Standalone execution
if __name__ == "__main__":
    config = {
        'topic': 'webhook_raw',
        'require_auth': os.getenv('WEBHOOK_REQUIRE_AUTH', 'false').lower() == 'true',
        'api_key': os.getenv('WEBHOOK_API_KEY'),
        'rate_limit': int(os.getenv('WEBHOOK_RATE_LIMIT', '100'))
    }

    collector = WebhookCollector(config)
    collector.start()