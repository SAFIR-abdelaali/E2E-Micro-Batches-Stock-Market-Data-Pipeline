# End-to-End Stock Market Micro-Batch Data Pipeline

## Overview

This project implements an end-to-end data pipeline that ingests real-time stock market data from the Finnhub API using a micro-batch architecture. The data is processed through Kafka, stored in MinIO, loaded into Snowflake, transformed with dbt, and finally visualized in Power BI.

## Architecture

![Data Pipeline](assets/data%20pipeline.jpg)

## Tech Stack

- Python
- Apache Kafka
- MinIO AWS S3 compatible
- Apache Airflow
- Snowflake
- dbt
- Power BI
- Docker & Docker Compose

## Project Structure

```
.
├── producer/          # Finnhub producer
├── consumer/          # Kafka consumer
├── infrastructure/
│   ├── dags/          # Airflow DAGs
│   ├── dbt_stock/     # dbt models
│   ├── docker-compose.yml
│   └── keys/          # RSA keys (not committed)
├── assets/
└── README.md
```

## Pipeline Flow

1. The producer fetches stock quotes from the Finnhub API.
2. Data is published to a Kafka topic monitored using Kafdrop and zookeeper.
3. The consumer reads Kafka messages and stores them as JSON files in MinIO AWS S3 compatible.
4. Airflow DAG downloads the JSON files and loads them into Snowflake data warehouse.
5. dbt transforms the raw data into analytics-ready models.
6. Power BI connects to Snowflake to build interactive dashboards.

## Prerequisites

- Docker & Docker Compose
- Python 3.9+
- Snowflake account
- Finnhub API key
- Power BI Desktop

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/SAFIR-abdelaali/E2E-Micro-Batches-Stock-Market-Data-Pipeline.git
cd rt-stock-market-mds
```

### 2. Configure credentials

Configure:

- Finnhub API key
- Snowflake account
- RSA key pair for Airflow/dbt authentication (best option for snowflake free trial accounts)

### 3. Start the infrastructure

```bash
docker compose up -d
```

### 4. Start the producer

```bash
python producer/producer.py
```

### 5. Start the consumer

```bash
python consumer/consumer.py
```

### 6. Trigger the Airflow pipeline

Open:

```
http://localhost:8080
```

Run the `bucket_to_warehouse` DAG.

### 7. Run dbt

```bash
cd infrastructure/dbt_stock

dbt run
dbt test
```

### 8. Open Power BI

Connect to Snowflake and use the dbt models to create reports.

## Components

| Component | Purpose |
|----------|---------|
| Kafka | Streaming stock events |
| MinIO | Object storage for raw JSON files |
| Airflow | Data orchestration |
| Snowflake | Data warehouse |
| dbt | Data transformation |
| Power BI | Reporting and visualization |

