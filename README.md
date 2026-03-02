# Credit Card Fraud Detection Pipeline

## Project Overview

This project implements an end-to-end data engineering pipeline to detect and analyze fraudulent credit card transactions.

The system:
- Streams transaction data using Apache Kafka
- Processes real-time data using Apache Spark Structured Streaming
- Performs batch analytics using PySpark (RDD + DataFrame API)
- Orchestrates workflows using Apache Airflow
- Stores raw and transformed data in Parquet format

---

## Business Problem

Banks process millions of transactions daily.  
Detecting fraudulent transactions in real time is critical to prevent financial losses.

This pipeline simulates transaction events and applies rule-based fraud detection logic.

---

## Architecture

Transaction Generator → Kafka → Spark Streaming → Raw Storage → Batch ETL → Analytics Output → Airflow Orchestration

---

## Tech Stack

- Apache Kafka
- Apache Spark (Structured Streaming + RDD + Spark SQL)
- Apache Airflow
- PySpark
- Parquet
- Faker (for data generation)

---

## Project Structure

```
creditcard_project/
│
├── README.md
│
├── data/
│   ├── regions.csv
│   ├── raw/
│   └── transformed/
│
├── kafka/
│   └── producer.py
│
├── spark/
│   ├── stream_consumer.py
│   ├── batch_rdd_etl.py
│   └── batch_df_etl.py
│
├── airflow/
│   └── dags/
│       └── creditcard_dag.py
│
└── config/
    └── spark-defaults.conf
```

---

## Module 1: Kafka Producer

### Topic Name:
`credit_card_transactions`

Generates transaction events in JSON format:

```json
{
  "transaction_id": "TXN-10001",
  "card_id": "CARD-9876",
  "merchant": "Amazon",
  "category": "Shopping",
  "amount": 250.75,
  "transaction_type": "ONLINE",
  "location": "New York",
  "transaction_time": "2026-02-25T10:32:00Z",
  "is_international": false
}
```

---

##  Module 2: Spark Streaming Fraud Detection

Reads transactions from Kafka and applies fraud rules.

### Fraud Detection Rules

A transaction is marked as fraud if:

- Amount > 2000
- International transaction
- Transaction between 12 AM – 4 AM
- Suspicious merchant category

Adds a new column:

```
is_fraud
```

Writes output to:

```
data/raw/
```

Partitioned by:
```
date
```

---

##  Module 3A: RDD Batch Processing

- Load raw parquet
- Filter fraud transactions
- Calculate total fraud amount per card
- Save to `data/transformed/rdd_output/`

Uses:
- map()
- filter()
- reduceByKey()

---

## Module 3B: DataFrame / Spark SQL Processing

### 1. Hourly Fraud Summary
- Total transactions
- Fraud transactions
- Fraud percentage

### 2. Top Risky Merchants
- Rank merchants by fraud count
- Uses Window functions

### 3. Fraud by Region
- Join with regions.csv
- Aggregate fraud amount

### 4. Fraud Category Breakdown
- Pivot by category

Output saved to:
```
data/transformed/df_output/
```

---

## Module 4: Airflow DAG

DAG Name:
`creditcard_fraud_pipeline`

### DAG Flow:

start  
→ check_kafka_topic  
→ run_streaming_job  
→ wait_for_raw_data  
→ run_rdd_etl  
→ run_df_etl  
→ validate_output  
→ end  

### DAG Configuration:
- schedule_interval = @daily
- retries = 2
- retry_delay = 5 minutes

---

## Output Generated

- Fraud transactions dataset
- Hourly fraud metrics
- Top risky merchants
- Fraud by region
- Fraud category pivot table

---

##  How to Run

### 1. Start Kafka
### 2. Create Topic
### 3. Run Producer
### 4. Run Streaming Consumer
### 5. Run Batch Jobs
### 6. Trigger Airflow DAG

---

## Key Concepts Demonstrated

- Real-time streaming ingestion
- Structured Streaming
- RDD transformations
- Spark SQL aggregations
- Window functions
- Data partitioning
- Workflow orchestration
- End-to-end data pipeline design

---

## Future Improvements

- Machine Learning fraud prediction model
- Deployment on AWS S3
- Real-time dashboard (Power BI / Tableau)
- Docker containerization

---

# Author
Eva Patel & Brian Tokumoto