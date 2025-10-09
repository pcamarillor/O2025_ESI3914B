# spark_cluster/notebooks/lib/jjodiaz/log_producer.py

import os
import time
import random
from datetime import datetime

LOG_DIR = "../../../data/logs/input" 

if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

LOG_LEVELS = ["INFO", "WARN", "ERROR"]
MESSAGES = {
    "INFO": ["User login successful", "Data processed correctly", "Request received"],
    "WARN": ["Disk usage 85%", "High CPU load detected", "Response time is slow"],
    "ERROR": ["500 Internal Server Error", "Database connection failed", "Authentication token expired"]
}
SERVERS = ["server-node-1", "server-node-2", "server-node-3"]

def generate_log_entry():
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    level = random.choices(LOG_LEVELS, weights=[7, 2, 1], k=1)[0] 
    message = random.choice(MESSAGES[level])
    server = random.choice(SERVERS)
    
    return f"{timestamp} | {level} | {message} | {server}"

if __name__ == "__main__":
    print(f"Iniciando productor de logs. Escribiendo archivos en: {LOG_DIR}")
    try:
        while True:
            log_line = generate_log_entry()
            
            file_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
            file_path = os.path.join(LOG_DIR, f"log_{file_timestamp}.txt")
            
            with open(file_path, "w") as f:
                f.write(log_line + "\\n")
                
            print(f"Archivo generado: {file_path}")
            
            time.to_sleep = random.randint(1, 3)
            time.sleep(time.to_sleep)
            
    except KeyboardInterrupt:
        print("\\nProductor de logs detenido.")