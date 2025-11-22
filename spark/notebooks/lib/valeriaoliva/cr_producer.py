import json
import random
import sys
import time

from datetime import datetime, timezone, timedelta
from faker import Faker
from kafka import KafkaProducer

fake = Faker("es_MX")

genders = ["H", "M"]
admission_types = ["Emergencia", "Urgencias", "Consulta"]
diagnoses = ["Fractura", "Infarto", "Accidente vial", "Deshidratación", "Hipertensión"]
meds = ["Analgésico", "Antibiótico", "Antiinflamatorio", "Suero"]
equip = ["Camilla", "Monitor signos vitales", "Desfibrilador", "Ventilador", "Collarín"]
hospital_ids = [f"CRMX_{str(i).zfill(4)}" for i in range(200)]
ambulance_ids = [f"AMBCR_{str(i).zfill(4)}" for i in range(250)]

def generate_event():
    admission_dt = datetime.now(timezone.utc) - timedelta(minutes=random.randint(0, 60))
    admission_timestamp = admission_dt.replace(microsecond=0).isoformat()

    had_transfer = random.random() < 0.55
    if had_transfer:
        departure_dt = admission_dt - timedelta(minutes=random.randint(25, 40))
        arrival_dt = admission_dt - timedelta(minutes=random.randint(1, 5))
        transfer = True
        unit_id = random.choice(ambulance_ids)
        departure_time = departure_dt.replace(microsecond=0).isoformat()
        arrival_time = arrival_dt.replace(microsecond=0).isoformat()
    else:
        transfer = False
        unit_id = None
        departure_time = None
        arrival_time = None
        
    medication_used = random.sample(meds, k=random.randint(0, 2))
    equipment_used = random.sample(equip, k=random.randint(0, 2))

    record = {
        "hospital_id": random.choice(hospital_ids),
        "patient_id": f"P{str(fake.unique.random_int(1, 99999999)).zfill(8)}",
        "last_name": fake.last_name(),
        "name": fake.first_name(),
        "age": random.randint(1, 99),
        "gender": random.choice(genders),
        "contact": str(fake.unique.random_int(0, 9999999999)).zfill(10),
        "admission_type": random.choice(admission_types),
        "admission_timestamp": admission_timestamp,
        "diagnosis": random.choice(diagnoses),
        "ambulance_transfer": transfer,
        "unit_id": unit_id,
        "departure_time": departure_time,
        "arrival_time": arrival_time,
        "medication_used": medication_used,
        "equipment_used": equipment_used,
        "event": fake.sentence(nb_words=10)
    }
    return record

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage python3 cr_producer.py <broker> <topic>")
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
            message = generate_event()
            producer.send(topic, value=message)
            print(f"Sent: {message}")
            time.sleep(5)
    except KeyboardInterrupt:
        print("Kafka producer stopped by user")
    finally:
        producer.flush()
        producer.close()