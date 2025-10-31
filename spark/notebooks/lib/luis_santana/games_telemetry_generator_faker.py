from faker import Faker
import random
import pandas as pd
from datetime import datetime, timedelta
import os

fake = Faker()
Faker.seed(42)
random.seed(42)

def generate_player_sessions(n):
    # Generate player session data
    sessions = []
    for i in range(n):
        start_time = fake.date_time_between(start_date='-30d', end_date='now')
        duration = random.randint(300, 7200)
        
        session = {
            "session_id": fake.uuid4(),
            "player_id": f"player_{random.randint(1000, 9999)}",
            "start_time": start_time.isoformat(),
            "end_time": (start_time + timedelta(seconds=duration)).isoformat(),
            "platform": random.choice(["PC", "PlayStation", "Xbox", "Switch", "Mobile"]),
            "game_version": f"{random.randint(1, 3)}.{random.randint(0, 9)}",
            "player_level": random.randint(1, 100),
            "gameplay_metrics": {
                "xp_gained": random.randint(0, 5000),
                "deaths": random.randint(0, 15),
                "kills": random.randint(0, 50),
                "score": random.randint(0, 100000),
                "achievements_unlocked": random.randint(0, 5)
            },
            "performance_metrics": {
                "avg_fps": round(random.uniform(30, 144), 2),
                "avg_latency_ms": random.randint(10, 150),
                "disconnects": random.randint(0, 2)
            },
            "country": fake.country_code()
        }
        sessions.append(session)
    return sessions


def generate_performance_metrics(n,players):
    #Generate performance metrics
    metrics = []
    for i in range(n):
        rand_player = players[random.randint(0, len(players)-1)]
        metric = {
            "metric_id": fake.uuid4(),
            "timestamp": fake.date_time_between(start_date='-5d', end_date='now').isoformat(),
            "session_id": rand_player["session_id"],
            "system_stats": {
                "fps": random.randint(30, 144),
                "cpu_usage": round(random.uniform(20, 95), 2),
                "gpu_usage": round(random.uniform(30, 100), 2),
                "memory_usage_mb": random.randint(2000, 16000),
                "temperature_celsius": random.randint(45, 85)
            },
            "network_stats": {
                "latency_ms": random.randint(10, 200),
                "packet_loss": round(random.uniform(0, 5), 2),
                "bandwidth_mbps": round(random.uniform(5, 100), 2)
            }
        }
        metrics.append(metric)
    return metrics



# Generate datasets
print("Generating telemetry")
sessions_data = generate_player_sessions(1000)
metrics_data = generate_performance_metrics(2000,sessions_data)


# Save to csv

def save_to_csv(data, filename):
    df = pd.json_normalize(data)
    df.to_csv(filename, index=False)


save_to_csv(sessions_data, "./spark/data/Proyect/Telemetry/player_sessions.csv")
save_to_csv(metrics_data, "./spark/data/Proyect/Telemetry/performance_metrics.csv")

print("Data generation complete. Files saved.")
