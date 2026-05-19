# Real-Time Banking Data Platform

## Overview
<img width="1536" height="1024" alt="ChatGPT Image May 19, 2026, 01_34_18 AM (1)" src="https://github.com/user-attachments/assets/2943b0ba-8915-4851-868d-d9239154d8dc" />

This project demonstrates a complete real-time banking streaming architecture using:

- Apache Kafka
- Apache Spark Structured Streaming
- Docker Compose
- Python Producers
- Rule-Based Fraud Detection
- Agentic AI Fraud Monitoring
- Ollama Local LLM Integration

The platform simulates:

1. Banking transactions being generated continuously
2. Kafka streaming infrastructure transporting events
3. Spark processing fraud detection in real time
4. Fraud alerts being generated into a separate Kafka topic
5. Local AI agents monitoring and explaining suspicious activity

---

# Final Architecture

```text
Python Producer
      ↓
Kafka Topic: banking-transactions
      ↓
Spark Structured Streaming Fraud Engine
      ↓
Kafka Topic: fraud-alerts
      ↓
Python Agentic AI Consumer
      ↓
Ollama Local LLM
      ↓
AI Fraud Investigation Summaries
```

---

# Technologies Used

| Technology | Purpose |
|---|---|
| Kafka | Event streaming platform |
| Spark | Distributed stream processing |
| Docker Compose | Infrastructure orchestration |
| Python | Producers + AI agents |
| Ollama | Local LLM serving |
| TinyLlama | Local lightweight AI model |
| Faker | Fake banking transaction generation |
| kafka-python | Kafka producer/consumer client |
| PySpark | Structured streaming engine |

---

# Project Structure

```text
real-time-banking-data-platform/
│
├── producer/
│   └── transaction_producer.py
│
├── consumer/
│   └── spark_streaming.py
│
├── agents/
│   └── fraud_monitor_agent.py
│
├── docker-compose.yml
│
├── README.md
│
└── data/
```

---

# Phase 1 — Environment Setup

## 1. Install WSL Ubuntu

Inside Windows PowerShell:

```powershell
wsl --install
```

Restart machine if required.

---

## 2. Install Python Virtual Environment

```bash
sudo apt update

sudo apt install python3-venv -y
```

Create virtual environment:

```bash
mkdir ~/first_project

cd ~/first_project

python3 -m venv .venv
```

Activate virtual environment:

```bash
source .venv/bin/activate
```

---

# Phase 2 — GitHub Authentication

## Generate SSH Key

```bash
ssh-keygen -t ed25519 -C "your_email@gmail.com"
```

Start SSH agent:

```bash
eval "$(ssh-agent -s)"
```

Add SSH key:

```bash
ssh-add ~/.ssh/id_ed25519
```

Show public key:

```bash
cat ~/.ssh/id_ed25519.pub
```

Copy key into:

GitHub → Settings → SSH and GPG Keys

---

# Phase 3 — Clone Repository

```bash
git clone git@github.com:YOUR_USERNAME/real-time-banking-data-platform.git
```

Enter project:

```bash
cd real-time-banking-data-platform
```

---

# Phase 4 — Install Docker

## Install Docker

```bash
sudo apt install docker.io -y
```

## Install Docker Compose

```bash
sudo apt install docker-compose-v2 -y
```

## Add User To Docker Group

```bash
sudo usermod -aG docker $USER
```

Restart terminal.

Verify:

```bash
docker ps
```

---

# Phase 5 — Kafka + Spark Infrastructure

## docker-compose.yml

```yaml
version: '3.9'

services:

  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    container_name: zookeeper

    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000

    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    container_name: kafka

    depends_on:
      - zookeeper

    ports:
      - "9092:9092"
      - "29092:29092"

    environment:
      KAFKA_BROKER_ID: 1

      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181

      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT

      KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:29092,EXTERNAL://localhost:9092

      KAFKA_LISTENERS: INTERNAL://0.0.0.0:29092,EXTERNAL://0.0.0.0:9092

      KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL

      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1

  spark-master:
    image: apache/spark:3.5.1
    container_name: spark-master

    command: >
      /opt/spark/bin/spark-class
      org.apache.spark.deploy.master.Master

    ports:
      - "8080:8080"
      - "7077:7077"

    volumes:
      - ./:/opt/spark-apps

  spark-worker:
    image: apache/spark:3.5.1
    container_name: spark-worker

    depends_on:
      - spark-master

    command: >
      /opt/spark/bin/spark-class
      org.apache.spark.deploy.worker.Worker
      spark://spark-master:7077

    ports:
      - "8081:8081"

    volumes:
      - ./:/opt/spark-apps
```

---

# Start Infrastructure

```bash
docker compose up -d
```

Verify containers:

```bash
docker ps
```

---

# Kafka Concepts Learned

| Concept | Meaning |
|---|---|
| Broker | Kafka server instance |
| Topic | Event stream/channel |
| Event | JSON message |
| Producer | Sends events |
| Consumer | Reads events |
| Partition | Topic scaling unit |
| Offset | Message position |

---

# Create Kafka Topics

## banking-transactions Topic

```bash
docker exec -it kafka kafka-topics \
--create \
--topic banking-transactions \
--bootstrap-server kafka:29092 \
--partitions 1 \
--replication-factor 1
```

## fraud-alerts Topic

```bash
docker exec -it kafka kafka-topics \
--create \
--topic fraud-alerts \
--bootstrap-server kafka:29092 \
--partitions 1 \
--replication-factor 1
```

List topics:

```bash
docker exec -it kafka kafka-topics \
--list \
--bootstrap-server kafka:29092
```

---

# Phase 6 — Banking Transaction Producer

## Install Python Libraries

```bash
pip install kafka-python faker pyspark ollama
```

---

## producer/transaction_producer.py

```python
from kafka import KafkaProducer

from faker import Faker

import json
import random
import time


fake = Faker()


producer = KafkaProducer(

    bootstrap_servers='localhost:9092',

    value_serializer=lambda v:
    json.dumps(v).encode('utf-8')
)


transaction_types = [

    "POS",
    "ATM",
    "ONLINE",
    "TRANSFER"
]

while True:

    transaction = {

        "customer_id":
            random.randint(1000, 5000),

        "customer_name":
            fake.name(),

        "transaction_type":
            random.choice(transaction_types),

        "amount":
            round(random.uniform(10, 5000), 2),

        "country":
            fake.country(),

        "timestamp":
            time.time()
    }

    producer.send(
        "banking-transactions",
        value=transaction
    )

    print(
        f"Produced: {transaction}"
    )

    time.sleep(2)
```

---

# Run Producer

```bash
python producer/transaction_producer.py
```

---

# Phase 7 — Spark Fraud Detection Engine

## consumer/spark_streaming.py

```python
from pyspark.sql import SparkSession

from pyspark.sql.functions import *

from pyspark.sql.types import *


spark = SparkSession.builder \
    .appName("BankingStreaming") \
    .master("spark://spark-master:7077") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
    ) \
    .getOrCreate()

schema = StructType([

    StructField("customer_id", IntegerType()),

    StructField("customer_name", StringType()),

    StructField("transaction_type", StringType()),

    StructField("amount", DoubleType()),

    StructField("country", StringType()),

    StructField("timestamp", DoubleType())
])


df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "banking-transactions") \
    .load()


parsed_df = df.selectExpr(
    "CAST(value AS STRING)"
)


json_df = parsed_df.select(
    from_json(
        col("value"),
        schema
    ).alias("data")
).select("data.*")


fraud_df = json_df.filter(
    col("amount") > 4000
).withColumn(
    "fraud_reason",
    lit("HIGH_AMOUNT")
).withColumn(
    "risk_level",
    lit("HIGH")
).withColumn(
    "alert_timestamp",
    current_timestamp()
)


kafka_output_df = fraud_df.select(
    to_json(
        struct("*")
    ).alias("value")
)


query = kafka_output_df.writeStream \
    .outputMode("append") \
    .format("kafka") \
    .option(
        "kafka.bootstrap.servers",
        "kafka:29092"
    ) \
    .option(
        "topic",
        "fraud-alerts"
    ) \
    .option(
        "checkpointLocation",
        "/tmp/fraud-checkpoint"
    ) \
    .start()


query.awaitTermination()
```

---

# Run Spark Fraud Engine

```bash
docker exec -it spark-master \
/opt/spark/bin/spark-submit \
--conf spark.jars.ivy=/tmp/.ivy \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
/opt/spark-apps/consumer/spark_streaming.py
```

---

# Phase 8 — Agentic AI Fraud Monitoring

## Install Ollama

```bash
curl -fsSL https://ollama.com/install.sh | sh
```

---

# Pull Lightweight Model

```bash
ollama pull tinyllama
```

Verify:

```bash
ollama list
```

---

# agents/fraud_monitor_agent.py

```python
from kafka import KafkaConsumer

import ollama

import json


consumer = KafkaConsumer(

    "fraud-alerts",

    bootstrap_servers="localhost:9092",

    auto_offset_reset="latest",

    value_deserializer=lambda m:
    json.loads(m.decode("utf-8"))
)


print("Fraud Monitoring Agent Started...")


for message in consumer:

    fraud_event = message.value

    print("\nReceived Fraud Alert:")
    print(fraud_event)


    fraud_reason = fraud_event["fraud_reason"]


    if fraud_reason == "HIGH_AMOUNT":

        reason_text = (
            "Transaction exceeded configured fraud threshold of 4000."
        )

        action_text = (
            "Recommend analyst review and customer verification."
        )

    else:

        reason_text = (
            "Unknown fraud pattern detected."
        )

        action_text = (
            "Recommend manual investigation."
        )


    prompt = f"""

    Fraud Event:
    {json.dumps(fraud_event, indent=2)}

    Verified Fraud Reason:
    {reason_text}

    Recommended Action:
    {action_text}

    Rules:
    - Do NOT invent information
    - Maximum 3 lines
    - Keep concise and professional

    """


    response = ollama.chat(

        model="tinyllama",

        messages=[

            {
                "role": "user",
                "content": prompt
            }
        ]
    )


    explanation = response["message"]["content"]


    print("\nAI Fraud Analysis:")
    print(explanation)

    print("\n" + "=" * 80)
```

---

# Run Agentic AI Monitor

```bash
python agents/fraud_monitor_agent.py
```

---

# Final Run Sequence

## Terminal 1 — Producer

```bash
python producer/transaction_producer.py
```

## Terminal 2 — Spark Fraud Engine

```bash
docker exec -it spark-master \
/opt/spark/bin/spark-submit \
--conf spark.jars.ivy=/tmp/.ivy \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
/opt/spark-apps/consumer/spark_streaming.py
```

## Terminal 3 — Agentic AI Monitor

```bash
python agents/fraud_monitor_agent.py
```

---

# Example Fraud Alert

```json
{
  "customer_id": 4566,
  "customer_name": "Jessica Garrett",
  "transaction_type": "POS",
  "amount": 4496.83,
  "country": "Saint Martin",
  "fraud_reason": "HIGH_AMOUNT",
  "risk_level": "HIGH"
}
```

---

# Key Concepts Learned

## Kafka

- Topics
- Producers
- Consumers
- Brokers
- Event streaming
- Internal vs external listeners
- Kafka networking

## Spark

- Structured Streaming
- Distributed execution
- Streaming transformations
- Kafka integration
- Checkpointing
- Fraud event enrichment

## Agentic AI

- Local LLM inference
- Ollama integration
- Grounded AI reasoning
- Hallucination mitigation
- Deterministic orchestration
- Event-driven AI systems

---

# Enterprise Architecture Insights

This project demonstrates modern enterprise AI architecture principles:

| Layer | Responsibility |
|---|---|
| Kafka | Event transport |
| Spark | Fraud computation |
| Python grounding layer | Deterministic reasoning |
| LLM | Summarization |
| Ollama | Local inference serving |

The system intentionally separates:

- deterministic fraud logic
- AI explanation layer

This reduces hallucination risks and aligns with real-world banking AI system design.

---

# Future Improvements

Potential next steps:

- Multiple fraud rules
- Window-based fraud detection
- Customer behavioral profiling
- Fraud dashboards
- LangGraph/OpenClaw orchestration
- Multi-agent investigation workflows
- Persistent fraud databases
- Streamlit analyst dashboard
- ML anomaly detection
- Vector databases
- Retrieval-augmented fraud investigations

---

# Git Commands Used

## Add Files

```bash
git add .
```

## Commit

```bash
git commit -m "message"
```

## Push

```bash
git push origin main
```

---

# Final Outcome

This project successfully demonstrates:

- Distributed Kafka infrastructure
- Spark streaming fraud detection
- Real-time fraud alert pipelines
- Agentic AI monitoring
- Local LLM integration
- Event-driven banking architecture
- Enterprise AI engineering concepts

The result is a complete end-to-end real-time AI-powered fraud monitoring platform.

