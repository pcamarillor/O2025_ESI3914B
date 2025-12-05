import os
import time
import random
from datetime import datetime

# Directory where log files will be written
LOG_DIR = os.path.join(os.getcwd(), "data", "custom-logs")
print(str(LOG_DIR))

if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

status_codes = [200, 200, 200, 404, 500, 500, 503]
endpoints = ["/login", "/logout", "/dashboard", "/api/v1/data", "/api/v1/upload"]

def generate_log_entry():
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    code = random.choice(status_codes)
    endpoint = random.choice(endpoints)
    return f"{timestamp} - {endpoint} - {code}\n"

def main():
    while True:
        filename = f"log_{int(time.time())}.txt"
        path = os.path.join(LOG_DIR, filename)
        with open(path, "w") as f:
            for _ in range(random.randint(10, 30)):
                f.write(generate_log_entry())
        print(f"[+] Created new log file: {filename}")
        time.sleep(5)  # create new log file every 5 seconds

if __name__ == "__main__":
    main()