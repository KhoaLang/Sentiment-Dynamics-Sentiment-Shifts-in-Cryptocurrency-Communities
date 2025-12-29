# Infra kick-start

```
docker compose up
```


# Run spark job

Bronze, Silver

```
/Users/khoaiquin/Storage/spark-3.5.7-bin-hadoop3/bin/spark-submit \
  --packages io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.3 \
  bronze_streaming.py
```

Gold

```
/Users/khoaiquin/Storage/spark-3.5.7-bin-hadoop3/bin/spark-submit \
  --packages io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.3,org.postgresql:postgresql:42.7.3 \
  ../utils/gold_migrate_to_postgres.py
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

# Gold model analysis

Run the spark-jobs/standalone_inference.py first to pre-install the model to local env.


# Prometheus

Manually setup Prometheus sink for Spark

```
curl -L -o spark-metrics_2.12-3.5.0.jar \
https://repo1.maven.org/maven2/org/apache/spark/spark-metrics_2.12/3.5.0/spark-metrics_2.12-3.5.0.jar

export SPARK_CLASSPATH=$PWD/spark-metrics_2.12-3.5.0.jar
```


# Grafana 

Dashboard for Gold

Start server

```
./sbin/start-thriftserver.sh \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.hadoop.fs.s3a.endpoint=http://localhost:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minio \
  --conf spark.hadoop.fs.s3a.secret.key=minio123 \
  --conf spark.hadoop.fs.s3a.path.style.access=true
```

