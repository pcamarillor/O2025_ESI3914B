import random
import time
from datetime import datetime
from pathlib import Path


class StreamLogProducer:
    def __init__(
        self,
        target_dir: str = "/opt/spark/work-dir/data/juanalonso/logs",
        interval: int = 5,
        entries_per_file: int = 5
    ):
        self.target_dir = Path(target_dir)
        self.interval_seconds = interval
        self.entries_count = entries_per_file

        self.levels = ["INFO", "WARN", "ERROR"]
        self.nodes = ["server-node-1", "server-node-2", "server-node-3", "server-node-4"]
        self.messages = {
            "INFO": ["User login successful", "Port Currently Listening", "API got 'GET' request"],
            "WARN": ["Disk usage 85%", "High CPU load detected", "Timeout, no response received"],
            "ERROR": ["500 Internal Server Error", "404 Not Found", "401 Unauthorized"]
        }

        self._ensure_dir_exists()

    def _ensure_dir_exists(self):
        if not self.target_dir.exists():
            print(f"Creating log directory: {self.target_dir}")
            self.target_dir.mkdir(parents=True, exist_ok=True)

    def _build_entry(self):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        level = random.choices(self.levels, weights=[0.6, 0.3, 0.1])[0]
        message = random.choice(self.messages[level])
        node = random.choice(self.nodes)
        return f"{timestamp} | {level} | {message} | {node}"

    def _write_file(self):
        filename = datetime.now().strftime("logfile_%Y%m%d%H%M%S%f.txt")
        file_path = self.target_dir / filename

        with open(file_path, "w") as f:
            for _ in range(self.entries_count):
                f.write(self._build_entry() + "\n")

        print(f"[{datetime.now().strftime('%H:%M:%S')}] Wrote file: {file_path}")

    def run_continuous(self):
        print(f"Starting log producer. Writing logs to: {self.target_dir}")
        print("Press Ctrl+C to stop.")

        try:
            while True:
                self._write_file()
                time.sleep(self.interval_seconds)
        except KeyboardInterrupt:
            print("\nLog producer stopped by user.")