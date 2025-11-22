import json
import time
from faker import Faker
import pandas as pd
import random
from datetime import datetime, timedelta
import os
from kafka import KafkaProducer
from faker import Faker

# Kafka
KAFKA_BOOTSTRAP_SERVERS = ['kafka:9093'] 
TOPIC_NAME = 'log-producer'
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)
fake = Faker()

SPEED_FACTOR=10000 # Tiempo de espera entre eventos, 1 minuto/SPEEDFACTOR
FILE='./project/csvs/checkins.csv'
# Ordenamos Por tiempo el df
checkins_df = pd.read_csv(FILE)
checkins_df = checkins_df.sort_values(by='timestamp').reset_index(drop=True)

print(f"Escenario listo: {len(checkins_df)} eventos ordenados cronológicamente.")
print(f"Primer evento: {checkins_df['timestamp'].iloc[0]}")
print(f"Ultimo evento: {checkins_df['timestamp'].iloc[-1]}")
print("Iniciando en kafka...")

# Productor a nuestro topico de kafka
try:
    last_event_time = datetime.strptime(checkins_df.iloc[0]['timestamp'], "%Y-%m-%d %H:%M:%S") # Ultimo evento (inicialmente es el primero)
    
    for index, row in checkins_df.iterrows():
        event_time = datetime.strptime(row['timestamp'], "%Y-%m-%d %H:%M:%S")
        row_dict = row.to_dict()
        
        # Calculamos espera a partir del ultimo evento
        time_diff_seconds = (event_time - last_event_time).total_seconds()
        
        # Si hay diferencia esperamos tomando en cuenta nuestro SPEED_FACTOR
        if time_diff_seconds > 0:
            wait_time = time_diff_seconds / SPEED_FACTOR
            if wait_time > 0:
                time.sleep(wait_time)
        
        # Enviamos a kafka
        producer.send(TOPIC_NAME, value=row_dict)

        
        print(f"[{row_dict['timestamp']}] {row['user_id']} -> {row['equipment']}")
        
        # Actualizamos la hora del ultimo evento
        last_event_time = event_time

except KeyboardInterrupt:
    print("\nDeteniendo...")

finally:
    producer.flush()
    producer.close()
    print("Producer cerrado.")