from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType
from pyspark.sql.functions import from_json, col

# 1. Initialize Spark session with Kafka package (matching Spark 3.1.2)
spark = SparkSession.builder \
    .appName("ShopPulseKafkaStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Define explicit schema matching the data stream specification
order_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("order_time", StringType(), True)
])

# 3. Read streaming data from Kafka topic
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "topic1_logs") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Parse JSON and unpack fields
parsed_df = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), order_schema).alias("data")) \
    .select("data.*")

# 5. Feature enrichment: compute total_amount and cast order_time to Timestamp
enriched_df = parsed_df \
    .withColumn("total_amount", (col("quantity") * col("price")).cast(DoubleType())) \
    .withColumn("order_timestamp", col("order_time").cast("timestamp")) \
    .drop("order_time")

# 6. Stream the transformed records to HDFS in Parquet format
query = enriched_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "/user/hive/warehouse/ecommerce_dw.db/streaming_orders") \
    .option("checkpointLocation", "/user/spark/checkpoints/streaming_orders") \
    .trigger(processingTime="10 seconds") \
    .start()

query.awaitTermination()