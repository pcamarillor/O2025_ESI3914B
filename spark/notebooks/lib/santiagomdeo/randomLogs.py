import os
import time
import random
from datetime import datetime

#the ar is ont reconized by the directory.
LOG_DIR = r"C:\Users\Santi\Documents\development\Procesamineto de datos masivos\clase2\O2025_ESI3914B\spark\data\server_logs"
os.makedirs(LOG_DIR, exist_ok=True)


LOG_LEVELS = ["INFO", "WARNING", "ERROR", "DEBUG"]
MESSAGES = [
    "User logged in successfully.",
    "Ending the world started successfully",
    "File not found: config_this.yaml",
    "I actually have no idea what happenend",
    "Server started on 6769.",
    "Memory usage at 145%. you are using to much vram",
    "API request timed out.",
    "user 'admin' kinda forgot",
    "Background task completed."
]

def generate_log_entry():
    """Generate a single log entry."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    level = random.choice(LOG_LEVELS)
    message = random.choice(MESSAGES)
    return f"[{timestamp}] [{level}] {message}"

def write_log(batch_size=5, sleep_time=1):
    for _ in range(batch_size):
        # Unique filename for each log file
        log_filename = datetime.now().strftime("server_%Y-%m-%d_%H-%M-%S-%f.log")
        log_path = os.path.join(LOG_DIR, log_filename)

        entry = generate_log_entry()
        with open(log_path, "w") as f:  # write a new file
            f.write(entry + "\n")

        print(f"Wrote log: {entry}")
        time.sleep(sleep_time)

if __name__ == "__main__":
    print(f"Writing logs to: {LOG_DIR}")
    while True:
        write_log(batch_size=3, sleep_time=5)
