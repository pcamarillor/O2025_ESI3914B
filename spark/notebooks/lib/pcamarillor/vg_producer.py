import json
import random
import string
import sys
import time

from datetime import datetime
from kafka import KafkaProducer

# Helper method

def random_player_id():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))

def random_coordinates():
    return round(random.uniform(-90, 90), 5), round(random.uniform(-180, 180), 5)

def random_device():
    return random.choice([
        "iphone",
        "xioami",
        "pixel",
        "samsung"
    ])

def random_action():
    return random.choice([
        "catch_pokemon",
        "visit_pokestop",
        "battle_gym",
        "trade_pokemon"
    ])

def gen_telemetry():
    lat, lon = random_coordinates()
    return {
        "player_id": random_player_id(),
        "timestamp": datetime.now().isoformat(),
        "device": random_device(),
        "action": random_action(),
        "location": {"latitude": lat, "longitude": lon},
        "speed": round(random.uniform(0, 20), 2),
        "battery_level": random.randint(2, 100)
    }

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage python3 vg_producer.py <broker> <topic>")
        sys.exit(1)

    broker = sys.argv[1]
    topic = sys.argv[2]

    # Initializer Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=[broker],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

    print("sending....")
    try:
        while True:
            message = gen_telemetry()
            producer.send(topic, value=message)
            print(f"Sent: {message}")
            time.sleep(5)
    except KeyboardInterrupt:
        print("Kafka producer stopped by user")
    finally:
        producer.flush()
        producer.close()