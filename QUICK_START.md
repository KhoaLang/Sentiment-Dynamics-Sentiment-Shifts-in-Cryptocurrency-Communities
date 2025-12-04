# Quick Start Guide

## Prerequisites

1. **Docker and Docker Compose** installed
2. **Python 3.9+** with pip
3. **Java 11+** (for Spark)
4. **Sufficient memory** (8GB+ recommended)
5. **API keys** for Reddit and Twitter (optional)

## Setup

### 1. Clone and Setup

```bash
# Install Python dependencies
pip install -r requirements.txt

# Copy environment configuration
cp .env.example .env

# Edit .env with your API keys and configuration
nano .env
```

### 2. Start Infrastructure

```bash
# Start all infrastructure services
docker-compose up -d

# Wait for services to be ready (30-60 seconds)
docker-compose ps
```

### 3. Initialize System

```bash
# Initialize tables, topics, and indices
python scripts/init_system.py
```

### 4. Start the System

**Option 1: Start everything**
```bash
python run_system.py start
```

**Option 2: Start specific components**
```bash
# Start model server
python models/model_server.py &

# Start webhook collector
python collectors/run_webhook_collector.py &

# Start streaming jobs
spark-submit --master local[*] streaming/preprocess_job.py &
spark-submit --master local[*] streaming/sentiment_job.py &
```

### 5. Collect Data

**Reddit Collector:**
```bash
# Dry run to test
python collectors/run_reddit_collector.py --dry-run

# Start collection
python collectors/run_reddit_collector.py
```

**Twitter Collector:**
```bash
# Configure Twitter API keys in .env first
python collectors/run_twitter_collector.py --dry-run
python collectors/run_twitter_collector.py
```

**Webhook Collector:**
```bash
# Send test data
curl -X POST http://localhost:8000/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "text": "Bitcoin is going to the moon! 🚀",
    "source": "test",
    "author": "test_user"
  }'
```

## Verify System

### Check System Status
```bash
# Interactive mode
python run_system.py interactive

# Or check status
python run_system.py status
```

### Verify Data Flow

1. **Check Kafka topics:**
```bash
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

2. **Check Iceberg tables:**
```bash
docker exec spark-sql spark-sql -e "SHOW TABLES IN iceberg.bronze;"
```

3. **Check model server:**
```bash
curl http://localhost:8000/health
```

4. **Monitor streaming jobs:**
```bash
# Check Spark UI
open http://localhost:4040
```

## Dashboard Setup

### Grafana
- URL: http://localhost:3000
- Username: admin
- Password: admin

### OpenSearch Dashboards
- URL: http://localhost:5601
- Configure indices: `sentiment-search`, `real-time-metrics`

## Troubleshooting

### Common Issues

1. **Port conflicts:**
```bash
# Check what's using ports
netstat -tulpn | grep :9092  # Kafka
netstat -tulpn | grep :8081  # Schema Registry
netstat -tulpn | grep :9000  # MinIO
```

2. **Memory issues:**
```bash
# Increase Spark memory in config/spark_config.yaml
# Or run with local[*] for testing
```

3. **Docker services not starting:**
```bash
# Check logs
docker-compose logs kafka
docker-compose logs minio
docker-compose logs spark-sql
```

4. **Python dependency issues:**
```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# or .venv\Scripts\activate  # Windows
pip install -r requirements.txt
```

### Reset System

```bash
# Stop everything
python run_system.py stop

# Clean Docker volumes
docker-compose down -v

# Restart
docker-compose up -d
python scripts/init_system.py
python run_system.py start
```

## Next Steps

1. **Monitor streaming jobs** in Spark UI (http://localhost:4040)
2. **Query data** using Spark SQL or Iceberg
3. **Set up alerts** in Grafana
4. **Scale out** for production use
5. **Customize models** with your own sentiment analysis models

## Production Deployment

For production deployment:
1. Use external Kafka cluster
2. Use managed Iceberg catalog (AWS Glue, etc.)
3. Configure proper authentication
4. Set up monitoring and alerting
5. Implement backup and disaster recovery