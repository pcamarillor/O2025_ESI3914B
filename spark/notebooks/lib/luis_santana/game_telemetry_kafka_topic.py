import json
import random
import sys
import time
from kafka import KafkaProducer
from faker import Faker
import random
from datetime import datetime, timedelta

fake = Faker()
Faker.seed(42)
random.seed(42)

def generate_player_sessions(n):
    # Generate player session data
    sessions = []
    for i in range(n):
        start_time = fake.date_time_between(start_date='-30d', end_date='now')
        duration = random.randint(300, 7200)
        
        session = {
            "session_id": fake.uuid4(),
            "player_id": f"player_{random.randint(1000, 9999)}",
            "start_time": start_time.isoformat(),
            "end_time": (start_time + timedelta(seconds=duration)).isoformat(),
            "platform": random.choice(["PC", "PlayStation", "Xbox", "Switch", "Mobile"]),
            "game_version": f"{random.randint(1, 3)}.{random.randint(0, 9)}",
            "player_level": random.randint(1, 100),
            "gameplay_metrics": {
                "xp_gained": random.randint(0, 5000),
                "deaths": random.randint(0, 15),
                "kills": random.randint(0, 50),
                "score": random.randint(0, 100000),
                "achievements_unlocked": random.randint(0, 5)
            },
            "performance_metrics": {
                "avg_fps": round(random.uniform(30, 144), 2),
                "avg_latency_ms": random.randint(10, 150),
                "disconnects": random.randint(0, 2)
            },
            "country": fake.country_code()
        }
        sessions.append(session)
    return sessions


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 game_telemetry_kafka_topic.py <kafka_broker> <topic>")
        sys.exit(1)
    
    broker = sys.argv[1]
    topic = sys.argv[2]
    
    producer = KafkaProducer(
        bootstrap_servers=[broker],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    
    try:
        while True:
            message =generate_player_sessions(1)[0]
            producer.send(topic, value=message)
            print(f"Sent: {message}")
            time.sleep(5)
    except KeyboardInterrupt:
        print("Kafka producer stopped by user.")
    finally:
        producer.flush()
        producer.close()