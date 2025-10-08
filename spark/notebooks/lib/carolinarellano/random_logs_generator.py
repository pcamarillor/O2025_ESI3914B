import os
import random
import time
import json
from datetime import datetime
from pathlib import Path

# Configuration
LOG_DIR = "/opt/spark/work-dir/data/carolinarellano/logs"  # Directory to drop log files into
LOG_LEVELS = ["INFO", "WARN", "ERROR"]
MESSAGES = {
    "INFO": ["User login successful", "Data processed correctly", "Request received"],
    "WARN": ["Disk usage 85%", "High CPU load detected", "Response time is slow"],
    "ERROR": ["500 Internal Server Error", "Database connection failed", "Null pointer exception"]
}
SERVERS = ["server-node-1", "server-node-2", "server-node-3", "server-node-4"]

def generate_log_entry():
    """Generates a single random log entry as a JSON object."""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    level = random.choices(LOG_LEVELS, weights=[0.7, 0.2, 0.1], k=1)[0]
    message = random.choice(MESSAGES[level])
    server = random.choice(SERVERS)
    
    return {
        "timestamp": timestamp,
        "level": level,
        "message": message,
        "server": server
    }

def main():
    """Main function to generate log files."""
    log_dir_path = Path(LOG_DIR)
    if not log_dir_path.exists():
        print(f"Creating directory: {LOG_DIR}")
        log_dir_path.mkdir(parents=True, exist_ok=True)
        
    print("Starting log producer...")
    file_count = 0
    try:
        while True:
            file_count += 1
            file_name = f"log_{file_count}_{int(time.time())}.json"
            file_path = log_dir_path / file_name
            
            log_entries = []
            num_entries = random.randint(5, 150)
            for _ in range(num_entries):
                log_entries.append(generate_log_entry())
            
            with open(file_path, 'w') as f:
                json.dump(log_entries, f, indent=2)
            
            print(f"Generated {file_path} with {num_entries} entries.")
            time.sleep(5)
            
    except KeyboardInterrupt:
        print("\nStopping log producer.")

if __name__ == "__main__":
    main()