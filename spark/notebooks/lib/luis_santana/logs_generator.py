import random
import time
import datetime
import os

DEFAULT_DIR = os.path.join(os.getcwd(), "spark", "data", "logs")
LEVELS = ["INFO", "WARN", "ERROR", "DEBUG"]
HOSTS = ["server-node-1", "server-node-2", "server-node-3"]

def random_message(level: str) -> str:
    #make more error and 505 likely to appear
    if level in ["ERROR", "WARN"]:
        return random.choice([
            "500 Internal Server Error",
            "502 Bad Gateway",
            "503 Service Unavailable",
            "404 Not Found"
        ])
    else:
        choice = random.choice([
            "500 Internal Server Error",
            "502 Bad Gateway",
            "503 Service Unavailable",
            "404 Not Found"
            "User login successful",
            "Cache warm completed",
            "Health check passed",
            "Background job finished",
            "Disk usage",
            "Ram usage",
            "CPU usage"
        ])
        if choice in ["Disk usage", "Ram usage", "CPU usage"]:
            usage = random.randint(50, 100)
            return f"{choice} {usage}%"
        else:
            return choice

def generate_log(ts: datetime.datetime, level: str, msg: str, host: str) -> str:
    return f"{ts.strftime('%Y-%m-%d %H:%M:%S')} | {level} | {msg} | {host}\n"

def main():
    os.makedirs(DEFAULT_DIR, exist_ok=True)  # ensure folder exists
    print(f"[producer] Writing logs into: {DEFAULT_DIR} for 5 minutes (Ctrl+C to stop)")
    end_time = time.time() + 5 * 60  # 5 minutes
    while time.time() < end_time:
        time.sleep(random.randint(1, 15))
        lines = random.randint(1, 3)
        filename = f"logs_{int(time.time())}.log"
        filepath = os.path.join(DEFAULT_DIR, filename)

        with open(filepath, "w", encoding="utf-8") as f:
            for _ in range(lines):
                ts = datetime.datetime.now()
                level = random.choice(LEVELS)
                msg = random_message(level)
                host = random.choice(HOSTS)
                f.write(generate_log(ts, level, msg, host))
        print(f"[producer] Wrote {lines} lines to {filepath}")

if __name__ == "__main__":
    main()

