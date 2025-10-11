import random
import time
import os
from datetime import datetime

# Define the log directory
log_dir = "/ruta/a/tu/directorio/logs"  

if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Log levels
log_levels = ['INFO', 'WARN', 'ERROR', 'DEBUG']

# Simulate log entries
def generate_log_entry():
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    level = random.choice(log_levels)
    if level == 'ERROR':
        error_code = random.choice([500, 404, 403, 502])
        message = f"HTTP {error_code} - Simulated server error."
    else:
        message = f"Normal operation, no issues detected."
    return f"{timestamp} {level} {message}\n"

# Write logs to a file every few seconds
def write_logs():
    while True:
        log_entry = generate_log_entry()
        with open(os.path.join(log_dir, "server_logs.txt"), 'a') as file:
            file.write(log_entry)
        time.sleep(2)  # Ajusta el tiempo según lo necesites (2 segundos aquí)

if __name__ == "__main__":
    write_logs()
