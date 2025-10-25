from faker import Faker
import random
import uuid
from datetime import datetime, timedelta
import json
from pathlib import Path
import math

fake = Faker()
Faker.seed(42)
random.seed(42)

OUTPUT_DIR = Path("./spark/data/data_proy/data_proy_waze")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

NUM_EVENTS = 60000  

alert_types = [None, "accident", "police", "road_closed", "pothole", "congestion"]

def rand_coord(center=(19.4326, -99.1332), radius_km=30):
    lat0, lon0 = center
    lat = lat0 + (random.random() - 0.5) * (radius_km / 110.0)
    lon = lon0 + (random.random() - 0.5) * (radius_km / (111.0 * abs(math.cos(math.radians(lat0)))))
    return round(lat, 6), round(lon, 6)

def make_event(start_time):
    evt_time = start_time + timedelta(seconds=random.randint(0, 3600*24))
    evt_type = random.choices(["location","alert","route_request"], weights=[0.7,0.2,0.1])[0]
    lat, lon = rand_coord()
    event = {
        "event_id": str(uuid.uuid4()),
        "device_id": f"dev_{random.randint(1,5000)}",
        "user_id": f"user_{random.randint(1,10000)}",
        "timestamp": evt_time.isoformat(),
        "lat": lat,
        "lon": lon,
        "speed_kmh": round(random.random()*120, 2),
        "heading": round(random.random()*360, 2),
        "event_type": evt_type,
        "alert_type": None,
        "route_points": None
    }
    if evt_type == "alert":
        event["alert_type"] = random.choice([t for t in alert_types if t])
    if evt_type == "route_request":
        points = []
        for _ in range(random.randint(3,6)):
            p_lat, p_lon = rand_coord()
            points.append([p_lat, p_lon])
        event["route_points"] = points
    return event

def generate_events(num_events=NUM_EVENTS):
    start_time = datetime.utcnow()
    file_path = OUTPUT_DIR / "events.jsonl"
    with file_path.open("w", encoding="utf-8") as f:
        for _ in range(num_events):
            evt = make_event(start_time)
            f.write(json.dumps(evt) + "\n")
    print(f"Generated {num_events} events to {file_path} (size: {file_path.stat().st_size} bytes)")

if __name__ == "__main__":
    generate_events()
