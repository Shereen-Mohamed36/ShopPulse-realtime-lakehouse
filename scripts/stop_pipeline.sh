#!/bin/bash

echo "=== Stopping ShopPulse E-Commerce Real-Time Pipeline ==="

# 1. Stop Spark Streaming Consumer using saved PID
if [ -f pids/spark.pid ]; then
    echo "[1/4] Stopping Spark Streaming Consumer..."
    kill $(cat pids/spark.pid) 2>/dev/null && rm -f pids/spark.pid
else
    echo "[1/4] Spark PID file not found, forcing kill..."
    pkill -f spark_streaming.py
fi

# 2. Stop Kafka Producer using saved PID
if [ -f pids/producer.pid ]; then
    echo "[2/4] Stopping Kafka Producer..."
    kill $(cat pids/producer.pid) 2>/dev/null && rm -f pids/producer.pid
else
    echo "[2/4] Producer PID file not found, forcing kill..."
    pkill -f generate_orders.py
fi

# 3. Stop Kafka Broker and Zookeeper
echo "[3/4] Stopping Kafka and Zookeeper..."
kafka-server-stop.sh 2>/dev/null
zookeeper-server-stop.sh 2>/dev/null

# 4. Stop Hadoop HDFS
echo "[4/4] Stopping Hadoop HDFS..."
stop-dfs.sh

echo "=== Pipeline Stopped Successfully! ==="
