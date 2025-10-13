import time
import random
import json
from datetime import datetime, timezone
from pathlib import Path

levels = ["ERROR", "WARN", "DEBUG", "INFO"]
actions = ["payment", "order", "user"]
events = ["created", "updated", "deleted", "error", "login"]
users = ["ana", "valeria", "oliva", "hdz"]

def create_logs():
    ts = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    level = random.choice(levels)
    action = random.choice(actions)
    event = random.choice(events)
    user = random.choice(users)
    logs = {
        "time": ts,
        "level": level,
        "action": action,
        "event": event,
        "user": user
    }
    return logs

def create_batch(filepath, n):
    with open(filepath, "w") as f:
        f.writelines(json.dumps(create_logs()) + "\n" for _ in range(n))

i = 0
base_dir = Path("./data/logs")

while True:
    i += 1
    file = base_dir / f"logs_{i}.json"
    n = random.randint(10, 30)
    create_batch(file, n)
    print("Batch created", file)
    time.sleep(5)
