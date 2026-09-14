# ShopPulse: Real-Time E-Commerce Lakehouse Pipeline

A streaming data pipeline that replaces a 12-hour batch reporting delay with real-time processing. Ingests simulated e-commerce transactions through Apache Kafka, processes streams using PySpark, stores columnar data in HDFS Parquet, and exposes an Apache Hive external table for OLAP queries.

---

## Architecture & Data Flow

<img width="2720" height="2400" alt="shoppulse_realtime_pipeline_architecture" src="https://github.com/user-attachments/assets/3754e15d-e667-41fb-b9f6-4749f12e79ed" />

---

## Tech Stack
* **Messaging:** Apache Kafka
* **Stream Processing:** PySpark Structured Streaming
* **Storage:** HDFS
* **Query & Metastore:** Apache Hive
* **Orchestration:** Bash Shell Scripts

---

## Project Structure

```text
├── producer/
│   └── generate_orders.py
├── spark/
│   └── spark_streaming.py
├── hive/
│   └── create_table.sql
├── scripts/
│   ├── start_pipeline.sh
│   └── stop_pipeline.sh
└── README.md
```

## Pipeline Components

* `producer/generate_orders.py`: Python script simulating live order transactions and publishing them to Kafka.
* `spark/spark_streaming.py`: PySpark application consuming from Kafka, parsing schemas, computing metrics, and writing Parquet files to HDFS.
* `hive/create_table.sql`: Hive DDL creating the external database and table mapped to HDFS, alongside 6 analytical SQL queries.
* `scripts/start_pipeline.sh`: Bash script to check service health, create directories, run DDL, and launch background processes.
* `scripts/stop_pipeline.sh`: Bash script for graceful shutdown using PID tracking while preserving states.
