# Real-Time Streaming Sentiment Analysis System

A comprehensive real-time streaming pipeline for sentiment analysis on social media data using Apache Kafka, Spark Structured Streaming, and Apache Iceberg with Medallion architecture.

## Architecture Overview

```
Data Sources → Kafka → Spark Jobs → Iceberg Bronze → Silver → Gold → Dashboards
                ↓
            [Model Serving, Vector Store, Search Layer]
```

### Core Components

1. **Data Ingestion**
   - Reddit API scraper
   - Twitter/X API scraper
   - Webhook-based collectors
   - Kafka topics for raw events

2. **Streaming Processing (Spark)**
   - Preprocess Job → Bronze layer
   - Sentiment Analysis Job → Silver layer
   - Enrichment Job → Silver layer
   - Aggregation Job → Gold layer

3. **Storage (Apache Iceberg)**
   - Bronze: Raw immutable data
   - Silver: Cleaned & enriched
   - Gold: Aggregated analytics

4. **Side Systems**
   - Model serving (FastAPI)
   - Vector store (Milvus)
   - Search (OpenSearch)
   - OLAP (ClickHouse)

## Quick Start

```bash
# 1. Start infrastructure
docker-compose up -d

# 2. Install Python dependencies
pip install -r requirements.txt

# 3. Set up environment
cp .env.example .env
# Edit .env with your API keys

# 4. Initialize Iceberg tables
python scripts/init_iceberg.py

# 5. Start data collection
python collectors/reddit_collector.py &
python collectors/twitter_collector.py &

# 6. Start Spark streaming jobs
spark-submit --master local[*] streaming/preprocess_job.py
spark-submit --master local[*] streaming/sentiment_job.py
spark-submit --master local[*] streaming/enrichment_job.py
spark-submit --master local[*] streaming/aggregation_job.py
```

## Project Structure

```
sentiment-streaming/
├── collectors/          # Data source collectors
├── streaming/           # PySpark streaming jobs
├── models/              # ML model interfaces
├── storage/             # Iceberg table definitions
├── config/              # Configuration files
├── docker/              # Docker configurations
├── scripts/             # Utility scripts
├── tests/               # Tests
├── notebooks/           # Jupyter notebooks
└── docs/                # Documentation
```

## Data Flow

1. **Collection**: Scrapers fetch social media data and publish to Kafka
2. **Bronze**: Raw data stored with minimal transformation
3. **Silver**: Cleaned data with sentiment scores and enrichment
4. **Gold**: Aggregated metrics for analytics and dashboards

## Technology Stack

- **Streaming**: Apache Kafka, Spark Structured Streaming
- **Storage**: Apache Iceberg, MinIO
- **Processing**: PySpark, Python 3.9+
- **ML/Analytics**: Scikit-learn, Transformers
- **Infrastructure**: Docker Compose
- **Monitoring**: Prometheus, Grafana

## Configuration

- Environment variables in `.env`
- Kafka topics in `config/kafka_topics.yaml`
- Iceberg schema in `storage/schemas/`
- Spark configs in `config/spark_config.yaml`