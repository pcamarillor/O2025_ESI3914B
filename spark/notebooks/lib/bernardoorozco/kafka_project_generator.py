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

# GYMS
gyms=[] 
cities=["Guadalajara", "Zapopan", "Tlaquepaque"] 
for i in range(1,10): 
    gyms.append({ "gym_id": f"G{i}", "name": f"SmartFit Fake {i}", "city": random.choice(cities) })

gyms_df = pd.DataFrame(gyms)
print(f"[OK] gyms.csv - {len(gyms_df)} records")

# USERS
membership_types = ["Basic", "Pro", "Premium"]
users = []
for i in range(1, 500):
    users.append({
        "user_id": f"U{i}",
        "name": fake.name(),
        "age": random.randint(18, 70),
        "membership_type": random.choice(membership_types),
        "gym_id": random.choice(gyms_df["gym_id"])  # usuario asignado a un gym
    })
users_df = pd.DataFrame(users)
print(f"[OK] users.csv - {len(users_df)} records")

# CHECKINS (Uso de los equipos)
""" PRODUCER PASADO
La parte compleja:
-Cada usuario tiene entre 5 y 20 visitas en el mes.
-En cada visita usa de 3 a 6 maquinas con duraciones entre 10 y 30 minutos por maquina.
-El timestamp se actualiza sumando la duracion anterior simulando una sesion real continua.
-Los horarios son dce 6AM a 11PM
-El resultado es consistente, no hay usuarios que esten en dos maquinas al mismo tiempo ni saltos de hora absurdos.
"""

""" PRODUCER NUEVO (Actual)
Haremos lo mismo que el producer pasado tomando en cuenta ciertas consideraciones
-Es ek tiempo real con kafka, asi que generaremos logs el ultimo dia, hora...
-La simulacion comienza a partir de datetime.now()
"""

# FACTOR DE VELOCIDAD
# 1.0 = Tiempo Real (Si entre eventos hay 10 mins, espera 10 mins)
# 60.0 = 1 minuto real pasa en 1 segundo (Modo rapido para presentar o practicar)

SPEED_FACTOR = 60
start_simulation_time = datetime.now()

equipments = ["Treadmill", "Bench Press", "Elliptical", "Stationary Bike", "Rowing Machine", "Squat Rack", "Ab Machine", "Leg Press", "Lat Pulldown", "Shoulder Press", "Chest Press", "Weight Bench"]
# start_dates = [(datetime.now() - timedelta(days=x)).replace(hour=0, minute=0, second=0, microsecond=0)for x in range(1)]
checkins = []

# Horas de simulacion de logs
simulation_window_hours = 4 

# Iteramos sobre cada user
for i in range(len(users_df)):
    user = users_df.iloc[i]
    
    # solo 25% de los users van al gym las procimas horas
    if random.random() > 0.25:
        continue

    # Algunos planes de gyms tipo smartfit te permiten ir a otras instalaciones, no todos van a su gym
    if random.random() < 0.8:   # 80% de probabilidad que vayan a su gym
        gym_id = user["gym_id"]
    else:   # 20% de probabilidad que con su plan vayan a otro Smartfit fake
        gym_id = random.choice(gyms_df["gym_id"])
    
    # Hora de llegada aleatoria dentro de las proximas horas
    minutes_aux = random.randint(0, simulation_window_hours * 60)
    arrival_time = start_simulation_time + timedelta(minutes=minutes_aux)
    
    # Usa entre 3 y 5 maquinas
    used_equipments = random.sample(equipments, random.randint(3, 5))
    session_time = arrival_time
    
    for eq in used_equipments:
        duration = random.randint(10, 30) # minutos de uso por maquina
        checkins.append({
            "checkin_id": f"C{len(checkins)}",
            "gym_id": gym_id,
            "user_id": user["user_id"],
            "user_name": user["name"], 
            "timestamp": session_time, 
            "equipment": eq,
            "duration_min": duration
        })
        
        # El siguiente checkin es despues de terminar este
        session_time += timedelta(minutes=duration)

# Ordenamos Por tiempo el df
checkins_df = pd.DataFrame(checkins)
checkins_df = checkins_df.sort_values(by='timestamp').reset_index(drop=True)

print(f"Escenario listo: {len(checkins_df)} eventos ordenados cronológicamente.")
print(f"Primer evento: {checkins_df['timestamp'].iloc[0]}")
print(f"Ultimo evento: {checkins_df['timestamp'].iloc[-1]}")
print("Iniciando en kafka...")

# Productor a nuestro topico de kafka
try:
    last_event_time = checkins_df.iloc[0]['timestamp'] # Ultimo evento (inicialmente es el primero)
    
    for index, row in checkins_df.iterrows():
        event_time = row['timestamp']
        row_dict = row.to_dict()
        row_dict['timestamp'] = row['timestamp'].strftime("%Y-%m-%d %H:%M:%S")
        
        # Calculamos espera a partir del ultimo evento
        time_diff_seconds = (event_time - last_event_time).total_seconds()
        
        # Si hay diferencia esperamos tomando en cuenta nuestro SPEED_FACTOR
        if time_diff_seconds > 0:
            wait_time = time_diff_seconds / SPEED_FACTOR
            if wait_time > 0:
                time.sleep(wait_time)
        
        # Enviamos a kafka
        producer.send(TOPIC_NAME, value=row_dict)
        
        print(f"[{row_dict['timestamp']}] {row['user_name']} -> {row['equipment']}")
        
        # Actualizamos la hora del ultimo evento
        last_event_time = event_time

except KeyboardInterrupt:
    print("\nDeteniendo...")

finally:
    producer.flush()
    producer.close()
    print("Producer cerrado.")