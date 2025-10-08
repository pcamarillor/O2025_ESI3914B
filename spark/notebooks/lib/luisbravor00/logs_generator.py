import os
import csv
import random
import time
from datetime import datetime, timedelta

class LogGenerator():
    def __init__(self, output_file, logs_qty):
        self.output_file = output_file
        self.logs_qty = logs_qty
        self.log_levels = ["INFO", "WARN", "ERROR"]
        self.log_servers = ["server-node-1", "server-node-2", "server-node-3", "server-node-4"]
        self.log_messages = {
            "INFO": [
                "User login successful",
                "Configuration loaded",
                "Backup completed",
                "Connection established",
                "Health check passed"
            ],
            "WARN": [
                "Disk usage 95%",
                "High memory consumption",
                "Network latency detected",
                "CPU usage exceeded threshold",
                "Low available storage"
            ],
            "ERROR": [
                "500 Internal Server Error",
                "Database connection failed",
                "Timeout while processing request",
                "File not found",
                "Permission denied"
            ]
        }
    
    def write_to_csv(self, logs):
        print(f"Writing {self.logs_qty} logs...")
        try:
            # Create the output directory/file
            os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
            # Open and write into the file
            with open(self.output_file, mode='w', newline="") as f:
                writer = csv.writer(f, delimiter='|')
                # Add all the logs into the csv file
                for log in logs:
                    writer.writerow([f" {col.strip()} " for col in log]) # Write every column separated by |
                return True
        except Exception as e:
            print(f"Could not write into the csv file. Error: {e}")
            return False
    
    def generate_logs(self):
        now = datetime.now()
        seconds_diff = 1
        logs = []

        for i in range(self.logs_qty):
            # Get the random information for the current log
            random_seconds = random.randint(0, 3600)
            date = now + timedelta(seconds=random_seconds)
            level = random.choice(self.log_levels)
            message = random.choice(self.log_messages[level])
            server = random.choice(self.log_servers)

            # Append every new log to the file
            logs.append([date.strftime("%Y-%m-%d %H:%M:%S"), level, message, server])
        
        # Verify if the logs were written to the csv file
        finished = self.write_to_csv(logs)
        if finished:
            print(f"Succesfully wrote logs to {self.output_file} csv file!")
        else:
            print("An issue was encountered when trying to create or add the logs to the csv file.")

if __name__ == "__main__":
    for i in range(10):
        # Create the csv file and wait 5 seconds before creating the other file
        LogGenerator(f"/opt/spark/work-dir/data/lab07_luis_bravo_logs/error_log{i*1}.csv", 10).generate_logs()
        time.sleep(5)