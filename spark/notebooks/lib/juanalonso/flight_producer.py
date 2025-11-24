import time
import json
import random

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from flight_data_generator import FlightDataGenerator

KAFKA_SERVER = "localhost:9092"
TOPIC_NAME = "flight_tickets_stream"

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

if __name__ == "__main__":
    print("Iniciando productor de tickets de vuelo...")

    try:
        generator = FlightDataGenerator()

        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_SERVER],
            value_serializer=json_serializer
        )

        print(f"Conectado a Kafka en {KAFKA_SERVER}. Enviando datos al topic '{TOPIC_NAME}'...")

        while True:
            ticket_record = generator.generate_ticket_record()

            print(f"Produciendo Ticket ID: {ticket_record['ticket_id']}")
            producer.send(TOPIC_NAME, value=ticket_record)

            time.sleep(random.uniform(0.5, 2.0))

    except NoBrokersAvailable:
        print(f"Error: No se pudo conectar a Kafka en {KAFKA_SERVER}.")
        print("Asegúrate de que tu clúster de Kafka esté corriendo.")
    except KeyboardInterrupt:
        print("\nDeteniendo productor...")
    finally:
        if 'producer' in locals() and producer:
            print("Cerrando conexión con Kafka.")
            producer.flush()
            producer.close()
