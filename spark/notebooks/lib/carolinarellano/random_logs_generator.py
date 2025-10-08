import random
import time
import json
from datetime import datetime
from pathlib import Path


class LogGenerator:
    def __init__(self, log_dir: str = "/opt/spark/work-dir/data/carolinarellano/logs"):
        self.log_dir = Path(log_dir)
        self.log_levels = ["INFO", "WARN", "ERROR"]
        self.messages = {
            "INFO": ["User login successful", "Data processed correctly", "Request received"],
            "WARN": ["Disk usage 85%", "High CPU load detected", "Response time is slow"],
            "ERROR": ["500 Internal Server Error", "Database connection failed", "Null pointer exception"]
        }
        self.servers = ["server-node-1", "server-node-2", "server-node-3", "server-node-4"]
        self.file_count = 0
        self._setup_directory()
    
    def _setup_directory(self):
        """Creates the log directory if it doesn't exist."""
        if not self.log_dir.exists():
            print(f"Creating directory: {self.log_dir}")
            self.log_dir.mkdir(parents=True, exist_ok=True)
    
    def _create_log_entry(self):
        """Creates a single random log entry."""
        return {
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "level": random.choices(self.log_levels, weights=[0.7, 0.2, 0.1])[0],
            "message": random.choice(self.messages[random.choice(self.log_levels)]),
            "server": random.choice(self.servers)
        }
    
    def generate_log_file(self, num_entries: int = None):
        """Generates a log file with random entries."""
        if num_entries is None:
            num_entries = random.randint(5, 150)
        
        # Create log entries
        entries = [self._create_log_entry() for _ in range(num_entries)]
        
        # Save to file
        self.file_count += 1
        file_name = f"log_{self.file_count}_{int(time.time())}.json"
        file_path = self.log_dir / file_name
        
        with open(file_path, 'w') as f:
            json.dump(entries, f, indent=2)
        
        print(f"Generated {file_path} with {num_entries} entries.")
        return file_path
    
    def start_continuous_generation(self, sleep_interval: int = 5):
        """Starts continuous log generation."""
        print("Starting log generator...")
        
        try:
            while True:
                self.generate_log_file()
                time.sleep(sleep_interval)
        except KeyboardInterrupt:
            print("\nStopping log generator.")
    
    def start_generating_logs(self, sleep_interval: int = 5):
        """Alias for start_continuous_generation for backward compatibility."""
        return self.start_continuous_generation(sleep_interval)


def main():
    log_generator = LogGenerator()
    log_generator.start_continuous_generation()


if __name__ == "__main__":
    main()