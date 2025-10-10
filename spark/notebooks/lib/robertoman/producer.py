# spark/notebooks/lib/robertoman/producer.py
import os
import random
import time
from datetime import datetime

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
FULL_DIR = os.path.join(BASE_DIR, "data", "lab7_roberto")

# Comenzamos creando nuestras listas con los posibles textos de nuestros logs
SERVERS = ["server-node-1", "server-node-2", "server-node-3", "server-node-4"]

INFO_MESSAGES = [
    "User login successful",
    "Data processed correctly",
    "Request received",
    "Health check passed",
]
WARN_MESSAGES = [
    "Disk usage 85%",
    "High CPU load detected",
    "Response time is slow",
    "Cache nearing capacity",
]
ERROR_MESSAGES = [
    "500 Internal Server Error",  # nos interesa este
    "500 Upstream service unavailable",
    "500 Database connection timeout",
    "404 Resource not found",
    "401 Unauthorized access attempt",
]


# usamos una funcion para insertar la hora en los logs, algo sencillo la hora del momento
def time_stamp_now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# para nuestro lab es buena idea tener este booleano en donde se puede forzar al error, para
# analyzes error patterns in real time,
# usamos rnadom choices para ir esocigiendo que tipo de linea de LOG se genera (error, info, warning)
def make_line(force_500: bool = False) -> str:
    if force_500:
        level = "ERROR"
        message = "500 Internal Server Error"
    else:
        level = random.choices(
            ["INFO", "WARN", "ERROR"], weights=[0.60, 0.20, 0.20], k=1
        )[0]
        if level == "INFO":
            message = random.choice(INFO_MESSAGES)
        elif level == "WARN":
            message = random.choice(WARN_MESSAGES)
        else:
            message = random.choice(ERROR_MESSAGES)
    server = random.choice(SERVERS)
    return f"{time_stamp_now()} | {level} | {message} | {server}"


def build_lines(n: int, burst_size: int = 4, burst_prob: float = 0.30):
    lines = []
    # flag
    inserted_burst = False
    i = 0
    while i < n:
        if (
            (not inserted_burst)
            and (random.random() < burst_prob)
            and (i + burst_size <= n)
        ):
            for _ in range(burst_size):
                lines.append(make_line(force_500=True))
            inserted_burst = True
            i += burst_size
        else:
            lines.append(make_line(force_500=False))
            i += 1
    return lines


def write_file(FULL_dir: str, log_id: int, lines) -> str:
    # creamos nuestros log file
    os.makedirs(FULL_dir, exist_ok=True)
    fname = os.path.join(FULL_dir, f"log_{log_id}_{int(time.time())}.log")
    with open(fname, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    return fname


FILES = 4
LINES_PER_FILE = 15
INTERVAL_SECS = 3.0
BURST_SIZE = 4
BURST_PROB = 0.35


def main():
    print(
        f"out-dir={FULL_DIR} files={FILES} lines/file={LINES_PER_FILE} "
        f"interval={INTERVAL_SECS}s burst={BURST_SIZE} p={BURST_PROB} \n"
    )
    for i in range(1, FILES + 1):
        lines = build_lines(
            LINES_PER_FILE, burst_size=BURST_SIZE, burst_prob=BURST_PROB
        )
        path = write_file(FULL_DIR, i, lines)
        print(f"({i}/{FILES}) en la ruta {path}")
        time.sleep(INTERVAL_SECS)


if __name__ == "__main__":
    random.seed()
    main()
