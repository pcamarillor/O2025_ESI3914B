# Generador de logs generado con ayuda de IA
import os
import time
import random
import datetime as dt

DIR = "data/logs"
LEVELS = ["INFO", 
          "NOTICE", 
          "WARNING", 
          "ERROR", 
          "CRITICAL"
]
HOSTS = ["api-gateway", 
         "auth-service", 
         "payment-node", 
         "db-cluster", 
         "cache-proxy"
]

def random_message(level: str) -> str:
    """Crea un mensaje aleatorio basado en el nivel."""
    normal_msgs = [
        "Connection established",
        "Session token validated",
        "Query executed successfully",
        "Background worker completed task",
        "Disk load",
        "CPU temperature",
        "Memory usage",
        "Heartbeat signal received"
]
    warning_msgs = [
        "Slow query detected",
        "Cache miss threshold exceeded",
        "High memory consumption",
        "Disk space below 10%",
        "Network latency spike"
    ]
    error_msgs = [
        "Timeout contacting upstream service",
        "502 Bad Gateway",
        "503 Service Unavailable",
        "Transaction rollback",
        "Uncaught exception in worker thread"
    ]
    critical_msgs = [
        "Kernel panic in VM container",
        "Database cluster unreachable",
        "Node out of sync with master",
        "Filesystem corruption detected"
    ]

    if level == "CRITICAL":
        msg = random.choice(critical_msgs)
    elif level == "ERROR":
        msg = random.choice(error_msgs)
    elif level == "WARNING":
        msg = random.choice(warning_msgs)
    else:
        msg = random.choice(normal_msgs)

    if any(keyword in msg for keyword in ["usage", "load", "temperature"]):
        return f"{msg}: {random.randint(50, 100)}%"
    return msg

def random_level() -> str:
    weights = [0.4, 0.2, 0.2, 0.15, 0.05]  
    return random.choices(LEVELS, weights=weights, k=1)[0]

def random_host() -> str:
    HOSTS = ["api-gateway",
             "auth-service", "payment-node", "db-cluster", "cache-proxy"]
    return random.choice(HOSTS)

def random_timestamp() -> str:
    return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def build_log_entry() -> str:
    ts = random_timestamp()
    lvl = random_level()
    msg = random_message(lvl)
    host = random_host()
    return f"{ts} | {lvl:<9} | {msg} | {host}\n"

def main(duration_min: int = 1):
    os.makedirs(DIR, exist_ok=True)
    print(f"[logger] Writing logs to: {DIR}")

    end_time = time.time() + duration_min * 60
    while time.time() < end_time:
        filename = f"log_{int(time.time())}.log"
        path = os.path.join(DIR, filename)
        entries = random.randint(1, 4)

        with open(path, "w", encoding="utf-8") as f:
            for _ in range(entries):
                f.write(build_log_entry())

        print(f"[logger] Wrote {entries} lines to {filename}")
        time.sleep(random.uniform(1, 10))

if __name__ == "__main__":
    main()