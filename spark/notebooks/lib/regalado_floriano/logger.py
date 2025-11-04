from math import prod
import random
import time
import datetime
import os
from itertools import product


now = datetime.datetime.now
DEFAULT_DIR = os.path.join(os.getcwd(), "spark", "data", "logs")
LOG_LEVELS= ["INFO", "WARN", "ERROR", "DEBUG"]
HOSTS =  [  f"{y}-node-{x}" for x in range(1,10) for y in "api db front auth".split()]

ERROR_LOGS = [
            "500 Internal Server Error",
            "502 Bad Gateway",
            "503 Service Unavailable",
            "404 Not Found"
]

HEALTH_LOGS = [
    "Health check passed",
    "CPU Check passed",
    "CPU Check Failed with usage=",
    "Ping Check passed",
    "Ping Check Failed with time=",
    "Health check not passed (Att 1)",
    "Health check not passed (Att 2)"
]

ALL_LOGS = ERROR_LOGS.copy()
ALL_LOGS.extend(HEALTH_LOGS)
ALL_LOGS.extend([ "Login Success", "Logout Success" ])

# pick_a_log :: str -> str
def pick_a_log(level) :
    bag = ALL_LOGS
    if level in ["ERROR", "WARN"]:
        bag = ERROR_LOGS
    choice = random.choice(bag)

    if "Failed" in choice:
        choice += str(random.randint(60,100))

    return choice

# generate_log:: (datetime.datetime, str, str, str) -> str
generate_log = (lambda ts ,level ,msg, host : 
    f"{level}|{ts.strftime('%Y-%m-%d %H:%M:%S')}|{msg}|{host}\n" )

#main :: None -> None
def main():
    
    os.makedirs(DEFAULT_DIR, exist_ok=True)  
    print(f"[logger] writing to {DEFAULT_DIR} "
    end_time = time.time() + 5 * 60  

    while time.time() < end_time:
        time.sleep(random.randint(1, 15))
        log_record_no = random.randint(1, 40)
        filename = f"logs_{int(time.time())}.log"
        filepath = os.path.join(DEFAULT_DIR, filename)

        with open(filepath, "w", encoding="utf-8") as f:
            vampireSlayer = ""
            for _ in range(log_record_no):
                log_level = random.choice(LOG_LEVELS)
                msg = pick_a_log(log_level)
                vampireSlayer += generate_log(now(), log_level, msg, random.choice(HOSTS))
            #print(vampireSlayer)
            f.write(vampireSlayer)

        print(f"[logger] logged {log_record_no} logs to {filepath}")

if __name__ == "__main__":
    main()
