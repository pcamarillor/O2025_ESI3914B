from faker import Faker
import random
import uuid
import os
from datetime import datetime, timedelta
import argparse

fake = Faker()

MAPS = ["Bind", "Haven", "Split", "Icebox", "Breeze"]
GAME_MODES = ["SpikeRush", "Competitive", "Unrated"]
RANKS = ["Iron", "Bronze", "Silver", "Gold", "Platinum", "Diamond", "Immortal", "Radiant"]
EVENT_TYPES = ["Kill", "Death", "AbilityUse", "SpikePlant", "SpikeDefuse"]
WEAPONS_ABILITIES = ["Vandal", "Phantom", "Operator", "Classic", "Sheriff", 
                     "Knife", "Cannonball", "Grenade", "HealingOrb", "Smoke"]

def gen_data(n_records=10000):
    data = []
    for _ in range(n_records):
        match_id = f"M{random.randint(5000,9999)}"
        player_id = f"P{random.randint(1000,1999)}"
        event_id = str(uuid.uuid4())
        record = {
            "event_id": event_id,
            "match_id": match_id,
            "player_id": player_id,
            "player_name": fake.user_name(),
            "rank": random.choice(RANKS),
            "map_name": random.choice(MAPS),
            "game_mode": random.choice(GAME_MODES),
            "event_type": random.choice(EVENT_TYPES),
            "weapon_or_ability": random.choice(WEAPONS_ABILITIES),
            "timestamp": fake.date_time_between(start_date='-30d', end_date='now')
        }
        data.append(record)
    return data

def save_spark_friendly_csv(data, out_path="data/valorant_events.csv"):
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        header = list(data[0].keys())
        f.write(",".join(header) + "\n")
        for row in data:
            line = ",".join(str(row[col]) for col in header)
            f.write(line + "\n")
    print(f"Archivo CSV generado: {out_path}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out_path", default="data/valorant_events.csv")
    parser.add_argument("--n_records", type=int, default=10000)
    args = parser.parse_args()

    data = gen_data(args.n_records)
    save_spark_friendly_csv(data, args.out_path)
    print(f"Dataset con {args.n_records} registros generado exitosamente.")

if __name__ == "__main__":
    main()
