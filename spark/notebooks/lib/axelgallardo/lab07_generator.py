#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import time
import random
from datetime import datetime

# Carpeta que Spark está monitoreando
OUT_DIR = "/opt/spark/work-dir/data/lab07/"
os.makedirs(OUT_DIR, exist_ok=True)

# Catálogo simple de logs
LEVELS  = ["INFO", "WARN", "ERROR"]
MODULES = ["server-node-1", "server-node-2", "auth", "payments"]
MSGS = {
    "INFO": [
        "User login successful",
        "Background job complete",
        "Heartbeat OK",
        "Cache refreshed",
    ],
    "WARN": [
        "Disk usage 85%",
        "High memory usage",
        "Slow query detected",
    ],
    "ERROR": [
        "500 Internal Server Error",
        "502 Bad Gateway",
        "Timeout while calling dependency",
    ],
}

def generate_logs(n_files: int = 5, lines_per_file: int = 8, delay_between_files: int = 3) -> None:
    """
    Genera n_files archivos NDJSON con 'lines_per_file' eventos cada uno,
    dejando 'delay_between_files' segundos entre archivos para provocar micro-batches.

    Cada línea tiene el formato:
      {"Date": "YYYY-MM-DD HH:MM:SS", "Level": "...", "Message": "...", "Module": "..."}
    """
    for i in range(n_files):
        filename = f"batch_{int(time.time())}_{i}.json"
        path = os.path.join(OUT_DIR, filename)

        with open(path, "w", encoding="utf-8") as f:
            for _ in range(lines_per_file):
                level = random.choices(LEVELS, weights=[6, 3, 1])[0]
                row = {
                    "Date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "Level": level,
                    "Message": random.choice(MSGS[level]),
                    "Module": random.choice(MODULES),
                }
                f.write(json.dumps(row) + "\n")

        # Pausa entre archivos para que Spark los procese en micro-batches separados
        time.sleep(delay_between_files)

if __name__ == "__main__":
    # Ejecutado como script: genera 5 archivos y termina
    generate_logs(n_files=5, lines_per_file=8, delay_between_files=3)
