from faker import Faker
import pandas as pd
import random
from datetime import datetime, timedelta

fake=Faker()

# Fake Gyms
gyms=[]
cities=["Guadalajara", "Zapopan", "Tlaquepaque"]
for i in range(1,10):
    gyms.append({
        "gym_id": f"G{i}",
        "name": f"Gym Fit {i}",
        "city": random.choice(cities)
    })

gyms_df=pd.DataFrame(gyms)
gyms_df.to_csv("gyms.csv", index=False)
print(f"[OK] gyms.csv -> {len(gyms_df)}  records")


# Fake Users
users=[]
membership_type=["Basic", "Pro", "Premium"]

for i in range(1, 500):
    users.append({
        "user_id": f"U{i}",
        "name": fake.name(),
        "age": random.randint(18,70),
        "membership_type": random.choice(membership_type)
    })
users_df=pd.DataFrame(users)
users_df.to_csv("users.csv", index=False)
print(f"[OK] users.csv -> {len(users_df)}  records")


# Fake chekins
equipments= ["Treadmill", "Bench Press", "Elliptical", "Stationary Bike", "Rowing Machine", "Squat Rack", "Ab Machine", "Leg Press", "Lat Pulldown", "Shoulder press", "Chest press", "Weight bench"]
checkins=[]
for i in range(1, 30000):
    user=random.choice(users_df["user_id"])
    gym=random.choice(gyms_df["gym_id"])
    duration_minuts=random.randint(15, 120)
    timestamp=fake.date_time_between(start_date="-30d", end_date="now")

    checkins.append({
        "chekin_id": f"C{i}",
        "gym_id": gym,
        "user_id": user,
        "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        "equipment": random.choice(equipments),
        "duration_min": duration_minuts
    })

checkins_df=pd.DataFrame(checkins)
checkins_df.to_csv("checkins.csv", index=False)

print(f"[OK] chekins.csv -> {len(checkins_df)}  records")
