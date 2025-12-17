# Infra kick-start

```
docker compose up
```


# Run spark job

```
spark-submit \
  --packages io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.3 \
  bronze_streaming.py
```


# Minio

Install mc client

```
brew install minio/stable/mc

mc alias set local http://localhost:9000 minio minio123
```

Apply this policy to allow a table the ListBucket permission

```
mc anonymous set-json ./main/storage/policies/delta-bronze-policy.json local/delta-bronze
```

Check for status

```
mc stat local/delta-bronze
```

List all child

```
mc ls local/delta-bronze
```


# Scraper

Run Reddit Scraper

```
python3 ./reddit_scraper.py \
  --sites-file ./sites_to_scrape.txt \
  --kafka-bootstrap localhost:9092\
  --kafka-topic reddit.bronze\
  --limit 5 \
  --max-pages 2 \
  --sleep-min 4 \
  --sleep-max 8
```

# Kafka

Monitor Kafka Topic in kafka container.

```
/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic reddit.bronze \
  --from-beginning
```