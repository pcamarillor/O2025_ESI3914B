import os
import time
import random
from datetime import datetime

# Lista de niveles de log y mensajes ejemplo

class customLogs:

    LEVELS = ["INFO", "WARNING", "ERROR", "DEBUG"]
    MESSAGES = [
        "Retriving Data",
        "Updating Data",
        "Connection to DB",
        "Memory usage high",
        "Database query executed",
        "Invalid data",
        "New user registered",
        "Unauthorized"
    ]

    def create_log(self):
        """Genera una línea de log con formato similar a logs reales."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        level = random.choice(self.LEVELS)
        message = random.choice(self.MESSAGES)
        if level == "WARNING" or level == "ERROR":
            message= level+" in "+ message
        elif level == "INFO":
            message= message+" succesfully"

        return f"[{timestamp}] {level}: {message}"


    def create_log_files(self, LOG_PATH:str,n_Files:int, minLogsPerFile=10, maxLogsPerFile=100):
        os.makedirs(LOG_PATH, exist_ok=True)
        for i in range(n_Files):
            time.sleep(random.uniform(1, 3))
            file_path = os.path.join(LOG_PATH, f"logs_{int(time.time())}.log")

            with open(file_path, "w") as f:
                for _ in range(random.randint(minLogsPerFile, maxLogsPerFile)):
                    log_line = self.create_log()
                    f.write(log_line + "\n")
                    print(log_line)
            

def main():
    LOG_PATH = "/opt/spark/work-dir/data/custom-logs"
    clogs= customLogs()
    clogs.create_log_files(LOG_PATH, 10)

if __name__ == "__main__":
    main()
