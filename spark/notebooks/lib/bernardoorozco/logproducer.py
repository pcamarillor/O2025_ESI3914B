import json
import random, time, os
from datetime import datetime

LOG_DIR = '../../../data/logs'

status_codes = [200, 404, 500, 403, 502]
endpoints = ["/home", "/login", "/api/data", "/checkout", "/admin"]
user = ['Bernardo', 'Pablo','Diego', 'Luis', 'Paola']

def generate_log():
    return {
        "user": random.choice(user),
        "timestamp": datetime.now().strftime("%d/%m/%Y, %H:%M:%S"),
        "endpoint": random.choice(endpoints),
        "status": random.choice(status_codes)
    }


while True:
    log = generate_log()
    filename = f"log_{int(time.time())}.json"
    with open(os.path.join(LOG_DIR, filename), "w") as f:
        f.write(json.dumps(log) + "\n")  
    time.sleep(3)