from faker import Faker
import pandas as pd
import random
from datetime import datetime, timedelta
import os

fake = Faker()

# GYMS
gyms=[] 
cities=["Guadalajara", "Zapopan", "Tlaquepaque"] 
for i in range(1,10): 
    gyms.append({ "gym_id": f"G{i}", "name": f"SmartFit Fake {i}", "city": random.choice(cities) })

gyms_df = pd.DataFrame(gyms)
gyms_df.to_csv("project/csvs/gyms.csv", index=False, mode="w")
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
users_df.to_csv("project/csvs/users.csv", index=False,  mode="w")
print(f"[OK] users.csv - {len(users_df)} records")


# CHECKINS (Uso de los equipos)
"""
La parte compleja:
-Cada usuario tiene entre 5 y 20 visitas en el mes.
-En cada visita usa de 3 a 6 maquinas con duraciones entre 10 y 30 minutos por maquina.
-El timestamp se actualiza sumando la duracion anterior simulando una sesion real continua.
-Los horarios son dce 6AM a 11PM
-El resultado es consistente, no hay usuarios que esten en dos maquinas al mismo tiempo ni saltos de hora absurdos.
"""

equipments = ["Treadmill", "Bench Press", "Elliptical", "Stationary Bike", "Rowing Machine", "Squat Rack", "Ab Machine", "Leg Press", "Lat Pulldown", "Shoulder Press", "Chest Press", "Weight Bench"]
start_dates = [(datetime.now() - timedelta(days=x)).replace(hour=0, minute=0, second=0, microsecond=0)for x in range(30)]
checkins = []

# Iteramos sobre cada user
for i in range(len(users_df)):
    user_id = users_df.loc[i, "user_id"]
    gym_id = random.choice(gyms_df["gym_id"])
    
    # Entre 5 y 20 dias de gym en los ultimos 30 dias sin repetirse
    gym_visits = random.sample(start_dates, random.randint(5, 20))
    
    for visit in gym_visits:
        # Horario de entre 6am y 11pm (el horario maximo de llegada 9pm suponiendo que uin user puede llegar a durar 2 horas, para que no se pase de las 11)
        start_hour = random.randint(6, 21)
        session_time = visit.replace(hour=start_hour, minute=0, second=0)
        
        # El usuario usa entre 3 y 6 maquina por sesion
        used_equipments = random.sample(equipments, random.randint(3, 6))
        
        for eq in used_equipments:
            duration = random.randint(10, 30)  # minutos por maquina
            
            checkins.append({
                "checkin_id": f"C{len(checkins) + 1}",
                "gym_id": gym_id,
                "user_id": user_id,
                "timestamp": session_time.strftime("%Y-%m-%d %H:%M:%S"),
                "equipment": eq,
                "duration_min": duration
            })
            
            # Actualizar el tiempo para la siguiente maquina
            session_time += timedelta(minutes=duration)

checkins_df = pd.DataFrame(checkins)
checkins_df.to_csv("project/csvs/checkins.csv", index=False, mode="w")
print(f"[OK] checkins.csv - {len(checkins_df)} registros generados correctamente")