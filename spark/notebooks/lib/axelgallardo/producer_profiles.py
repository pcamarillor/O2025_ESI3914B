import csv, json, time, os
from kafka import KafkaProducer

TOPIC = "project-stream"
BOOTSTRAP = "kafka:9093"
INPUT_CSV = "/opt/spark/work-dir/data/Proyecto/profiles.csv"
ROWS_PER_SEC = 20

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    key_serializer=lambda k: str(k).encode("utf-8"),
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    linger_ms=50,
    acks="all",
)

with open(INPUT_CSV, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    delay = 1.0 / ROWS_PER_SEC if ROWS_PER_SEC > 0 else 0.0

    for row in reader:
        msg = {
            "id": int(row["id"]) if row["id"] else None,
            "first_name": row["first_name"] or None,
            "last_name": row["last_name"] or None,
            "email": row["email"] or None,
            "gender": row["gender"] or None,
            "age": int(row["age"]) if row["age"] else None,
            "education": row["education"] or None,
        }

        key = msg["id"] if msg["id"] is not None else ""
        producer.send(TOPIC, key=key, value=msg)

        if delay > 0:
            time.sleep(delay)

producer.flush()
producer.close()
print(f"Envío completado a topic '{TOPIC}'.")
