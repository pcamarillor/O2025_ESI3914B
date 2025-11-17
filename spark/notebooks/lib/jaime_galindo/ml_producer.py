from order_generator import OrderGenerator
import json
import sys
from kafka import KafkaProducer
import time


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage python3 ml_producer.py <broker> <topic>")
        sys.exit(1)
    
    broker = sys.argv[1]
    topic = sys.argv[2]

    # Initializer Kafka producer
    producer = KafkaProducer(
        bootstrap_servers = [broker],
        value_serializer = lambda x : json.dumps(x).encode('utf-8')
    )

    print("sending... ")
    try:
        i = 5500
        while True:
            order = OrderGenerator.create_random_order(i)
            producer.send(topic, value=order)
            print(f"Sent: {order}")
            time.sleep(5)
    except KeyboardInterrupt:
        print("kafka producer stopped by user")
    finally: 
        producer.flush()
        producer.close()

