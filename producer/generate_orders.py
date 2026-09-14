import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

# Initialize Kafka Producer pointing to localhost:9092
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

topic_name = 'topic1_logs'
order_id_counter = 1001

print(f"Starting Order Producer... Streaming to topic: {topic_name}")

try:
    while True:
        order_data = {
            "order_id": order_id_counter,
            "customer_id": random.randint(100, 500),
            "product_id": random.randint(1, 50),
            "quantity": random.randint(1, 10),
            "price": round(random.uniform(5.0, 500.0), 2),
            "order_time": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # Send message to Kafka
        producer.send(topic_name, value=order_data)
        print(f"Sent: {order_data}")
        
        order_id_counter += 1
        
        # Ingestion interval between 1.0 to 2.0 seconds
        time.sleep(random.uniform(1.0, 2.0))
        
except KeyboardInterrupt:
    print("\nProducer stopped by user.")
finally:
    producer.close()
