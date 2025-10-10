import time
import random
import datetime
import os
from pathlib import Path

# =================================================================
# CONFIGURACIÓN
# =================================================================
# RUTA ABSOLUTA que Spark monitoreará
LOG_DIR = Path("/opt/spark/work-dir/labs/lab07/streaming_input") 
INTERVAL_SECONDS = 5
LINES_PER_FILE = 5

LOG_LEVELS = ["INFO", "WARN", "ERROR"]
SERVER_NODES = ["server-node-1", "server-node-2", "server-node-3"]
MESSAGES = {
    "INFO": ["User login successful", "API request processed", "Database connection established"],
    "WARN": ["Disk usage 85%", "High memory usage detected", "Timeout on external service"],
    # Incluimos el error crítico que Spark debe detectar
    "ERROR": ["404 Not Found", "500 Internal Server Error", "Database write failure"] 
}

def generate_log_entry():
    """Genera una línea de log con el formato: TIMESTAMP | LEVEL | MESSAGE | NODE"""
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    level = random.choices(LOG_LEVELS, weights=[0.6, 0.3, 0.1], k=1)[0]
    message = random.choice(MESSAGES[level])
    node = random.choice(SERVER_NODES)
    return f"{timestamp} | {level} | {message} | {node}"

def write_log_file():
    """Genera un archivo con entradas de log en la carpeta de monitoreo."""
    # Nombre de archivo único para que Spark lo reconozca como nuevo
    filename = datetime.datetime.now().strftime("server_log_%Y%m%d%H%M%S%f.log")
    file_path = LOG_DIR / filename
    
    print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] Generando archivo: {file_path}")

    with open(file_path, "w") as f:
        for _ in range(LINES_PER_FILE):
            log_line = generate_log_entry()
            f.write(log_line + "\n")


# BUCLE PRINCIPAL
if __name__ == "__main__":
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Iniciando Productor. Logs escribiéndose en: {LOG_DIR}")
    print(f"**IMPORTANTE: Ejecute su Jupyter Notebook AHORA.** Presione Ctrl+C para detener.")
    
    try:
        while True:
            write_log_file()
            time.sleep(INTERVAL_SECONDS) 
    except KeyboardInterrupt:
        print("\nProductor de logs detenido por el usuario.")