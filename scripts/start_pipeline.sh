#!/bin/bash

echo "=== Starting ShopPulse E-Commerce Real-Time Pipeline ==="

# Create necessary local directories for logs and pids if not exist
mkdir -p logs pids

# 1. Verify HDFS & Kafka status
echo "[1/5] Checking HDFS and Kafka services..."
jps | grep -q "NameNode" || { echo "Error: Hadoop NameNode is not running!"; exit 1; }
jps | grep -q "Kafka" || { echo "Error: Kafka broker is not running!"; exit 1; }

# 2. Create prerequisite HDFS directories
echo "[2/5] Initializing HDFS directories for Parquet and Checkpoints..."
hdfs dfs -mkdir -p /user/hive/warehouse/ecommerce_dw.db/streaming_orders
hdfs dfs -mkdir -p /user/spark/checkpoints/streaming_orders

# 3. Execute Hive DDL to setup external table
echo "[3/5] Setting up Hive External Table..."
hive -f create_table.sql

# 4. Launch Python Producer in background
echo "[4/5] Launching Kafka Producer..."
python3 generate_orders.py > logs/producer.log 2>&1 &
echo $! > pids/producer.pid

# 5. Launch Spark Streaming Consumer in background
echo "[5/5] Launching Spark Structured Streaming Consumer..."
python3 spark_streaming.py > logs/spark_streaming.log 2>&1 &
echo $! > pids/spark.pid

echo "=== Pipeline Started Successfully! ==="
