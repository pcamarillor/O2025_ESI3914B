import time
import json
import random
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from data_generator import EcommDataGenerator

KAFKA_SERVER = 'localhost:9092'
TOPIC_NAME = 'ecomm_sales_stream'

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

print("Iniciando productor de e-commerce...")

try:
    # --- 2. Creación de Instancias ---
    generator = EcommDataGenerator(num_clientes=500, num_productos=100)
    
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_SERVER],
        value_serializer=json_serializer
    )

    print(f"Conectado a Kafka en {KAFKA_SERVER}. Enviando datos al topic '{TOPIC_NAME}'...")
    
    # --- 3. Bucle de Producción Continua ---
    while True:
        sale_record = generator.generate_sale_record()
        
        print(f"Produciendo Venta ID: {sale_record['venta_id']}")
        producer.send(TOPIC_NAME, value=sale_record)
        
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