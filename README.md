# Stock Market Real-Time Data Analysis — Kafka + AWS Pipeline

A real-time data streaming pipeline that simulates live stock market data, streams it through Apache Kafka on AWS EC2, and stores the results in Amazon S3 for downstream analysis with AWS Glue and Athena.

## Architecture

```
Stock Data (CSV) → Kafka Producer (EC2) → Kafka Topic → Kafka Consumer (EC2) → S3 → Glue Crawler → Athena
```

![Architecture](https://github.com/dracarrys/Stock-Market-Real-Time-Data-Analysis/assets/100908058/281a209c-f2fc-413c-942e-227afb2ad937)

![Full Pipeline](https://github.com/dracarrys/Stock-Market-Real-Time-Data-Analysis/assets/100908058/c40a41e1-fe55-4a0b-8b94-f314c5a4b6ea)

## Tech Stack

| Layer | Tool |
|-------|------|
| Streaming | Apache Kafka 3.7 |
| Cluster Coordination | Apache ZooKeeper |
| Compute | AWS EC2 |
| Storage | Amazon S3 |
| Cataloging | AWS Glue Crawler |
| Querying | Amazon Athena |
| Language | Python |

## How It Works

### Producer (`KafkaProducer.ipynb`)
- Reads historical stock index data from `indexProcessed.csv`
- Randomly samples one row per second to simulate a live market feed
- Serializes each record as JSON and publishes to a Kafka topic on EC2

```python
while True:
    dict_stock = df.sample(1).to_dict(orient="records")[0]
    producer.send('demo_test', value=dict_stock)
    sleep(1)
```

### Consumer (`KafkaConsumer.ipynb`)
- Subscribes to the Kafka topic
- Deserializes each incoming message
- Writes each record as a JSON file to S3 using `s3fs`

```python
for count, i in enumerate(consumer):
    with s3.open("s3://kafka-stock-market-bucket/stock_market_{}.json".format(count), 'w') as file:
        json.dump(i.value, file)
```

### Kafka Setup (EC2)
Kafka was deployed on an AWS EC2 instance. Key setup steps:

```bash
# Start ZooKeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka broker
export KAFKA_HEAP_OPTS="-Xmx256M -Xms128M"
bin/kafka-server-start.sh config/server.properties

# Create topic
bin/kafka-topics.sh --create --bootstrap-server <EC2-IP>:9092 \
  --replication-factor 1 --partitions 1 --topic demo_test
```

## Dataset

`indexProcessed.csv` — Historical stock market index data used to simulate a real-time feed by random sampling.

## Author

Gkeri Pepelasi
