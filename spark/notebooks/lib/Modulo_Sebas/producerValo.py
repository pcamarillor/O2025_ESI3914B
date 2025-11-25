import csv
import json
import sys
import time
from kafka import KafkaProducer

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python3 producerValo.py <broker> <topic> <csv_file>")
        sys.exit(1)

    broker = sys.argv[1]
    topic = sys.argv[2]
    csv_file = sys.argv[3]

    producer = KafkaProducer(
        bootstrap_servers=[broker],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

    print(f"Sending Valorant events...")

    try:
        with open(csv_file, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                message = {
                    "event_id": row["event_id"],
                    "match_id": row["match_id"],
                    "player_id": row["player_id"],
                    "player_name": row["player_name"],
                    "rank": row["rank"],
                    "map_name": row["map_name"],
                    "game_mode": row["game_mode"],
                    "event_type": row["event_type"],
                    "weapon_or_ability": row["weapon_or_ability"],
                    "timestamp": row["timestamp"]
                }

                producer.send(topic, value=message)
                print(f"Sent: {message}")

                time.sleep(3)

    except KeyboardInterrupt:
        print("\nKafka producer stopped by user")

    finally:
        producer.flush()
        producer.close()
