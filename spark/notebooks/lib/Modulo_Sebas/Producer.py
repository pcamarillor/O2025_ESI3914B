import os
import time
import random
from datetime import datetime

LOG_DIR = "/opt/spark/work-dir/data/logs"
os.makedirs(LOG_DIR, exist_ok=True)

LEVELS = ["INFO", "WARN", "ERROR"]
MESSAGES = {
    "INFO": [
        "User login successful",
        "Background job completed",
        "Health check OK",
        "File uploaded successfully"
    ],
    "WARN": [
        "Disk usage 85%",
        "Memory usage high",
        "Response time slow",
        "Deprecated API usage detected"
    ],
    "ERROR": [
        "500 Internal Server Error",
        "503 Service Unavailable",
        "Database connection failed",
        "Timeout while connecting to API"
    ]
}
SERVERS = ["server-node-1", "server-node-2", "server-node-3"]

def generate_log_entry():
    """Genera una línea de log aleatoria."""
    level = random.choice(LEVELS)
    message = random.choice(MESSAGES[level])
    server = random.choice(SERVERS)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return f"{timestamp} | {level} | {message} | {server}"

def main():
    print("Iniciando generador de logs...\nPresiona Ctrl+C para detenerlo.")
    while True:
        log_entry = generate_log_entry()
        print(log_entry)

        filename = f"log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        filepath = os.path.join(LOG_DIR, filename)
        with open(filepath, "w") as f:
            f.write(log_entry + "\n")

        time.sleep(random.randint(2, 5))
if __name__ == "__main__":
    main()
