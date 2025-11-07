import sys
import json
import random
import time

from datetime import datetime, timedelta
from kafka import KafkaProducer

# The oldest created_at value on the dataset is May 21st, 2007 (use it as lowest date)
oldest_created_at = datetime(2007, 5, 21)
# The most recent updated_at value on the dataset is October 12th, 2018 (use it as the newest date)
newest_updated_at = datetime(2018, 10, 12)

# def calculate_account_category(views):
#     # Calculate the account_category depending on the views quantity
#     if views < 1000:
#         return "Nano"
#     elif views < 10000:
#         return "Micro"
#     elif views < 50000:
#         return "Mid-Tier"
#     elif views < 100000:
#         return "Macro"
#     elif views < 500000:
#         return "Mega"
#     else:
#         return "Elite"

# def calculate_activity_status(updated_at):
#     # Calculate days since the most recent update (use the newest date that someone updated it's info)
#     days_since_update = (newest_updated_at - updated_at).days
    
#     # Return the a the activity_status depending on the value of days_since_update
#     if days_since_update <= 30:
#         return "Very Active"
#     elif days_since_update <= 90:
#         return "Active"
#     elif days_since_update <= 180:
#         return "Less Active"
#     else:
#         return "Inactive"

def random_language():
    # Return a random choice between EN (English), DE (Deutsch) and Other
    return random.choice([
        "EN", "DE", "Other"
    ])

def calculate_life_time(created_at, updated_at):
    return (updated_at - created_at).days

def random_updated_at(created_at):
    # Calculate the time difference between the newest update and when it was created
    time_diff = (newest_updated_at - created_at).days

    # If the time_diff is 0, then add 1 to that date and return it
    if time_diff <= 0:
        return created_at + timedelta(days=1)
    
    # If not, then calculate a random number from 1 to the difference and add it up to it's creation
    random_days = random.randint(1, time_diff)
    return created_at + timedelta(days=random_days)

def random_created_at():
    # Calculate the difference of days between the newest update and the oldest account creation
    time_diff = (newest_updated_at - oldest_created_at).days
    # Get random days between 0 and the difference of calculated days
    random_days = random.randint(0, time_diff)
    # Return the sum of the oldest account plus the random days
    return oldest_created_at + timedelta(days=random_days)

def random_number_range(start, end):
    # Return a random integer using a range between start and end values
    return random.randint(start, end)

def generate_telemetry():
    views = random_number_range(1, 1000000)
    created_at = random_created_at()
    updated_at = random_updated_at(created_at)
    
    return {
        "views": views,
        "mature": random_number_range(0, 1),
        "life_time": calculate_life_time(created_at, updated_at),
        "created_at": str(created_at.date()),
        "updated_at": str(updated_at.date()),
        "numeric_id": random_number_range(0, 1000000),
        "dead_account": random_number_range(0, 1),
        "language": random_language(),
        "affiliate": random_number_range(0, 1),
    }


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage python3 producer_script_luis_bravo.py <broker> <topic>")
        sys.exit(1)

    broker = sys.argv[1]
    topic = sys.argv[2]

    # Initialize Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=[broker],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    i = 1

    try:
        while True:
            message = generate_telemetry()
            producer.send(topic, message)
            print(f"Message #{i} sent: {message}")
            i += 1
            time.sleep(2)
    except KeyboardInterrupt:
        print("Kafka producer stopped by user.")
    finally:
        producer.flush()
        producer.close()

    ### COMMENT ALL THE LINES ABOVE AND UNCOMMENT THE ONES BELOW JUST TO PRINT THE INFORMATION
    # try:
    #     while True:
    #         message = generate_telemetry()
    #         print(f"Message sent with values: {json.dumps(message, indent=2, default=str)}")
    #         time.sleep(3)
    # except KeyboardInterrupt:
    #     print("Producer stopped by user.")