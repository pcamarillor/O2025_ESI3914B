import json
import random
import sys
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer


CHICAGO_STREETS = [
    "MICHIGAN AVE", "STATE ST", "WACKER DR", "LAKE SHORE DR", 
    "HALSTED ST", "ASHLAND AVE", "WESTERN AVE", "PULASKI RD",
    "CICERO AVE", "STONY ISLAND AVE", "COTTAGE GROVE AVE",
    "ROOSEVELT RD", "CERMAK RD", "ARCHER AVE", "95TH ST",
    "CHICAGO AVE", "NORTH AVE", "FULLERTON AVE", "BELMONT AVE",
    "IRVING PARK RD", "MONTROSE AVE", "LAWRENCE AVE", "FOSTER AVE",
    "DEVON AVE", "TOUHY AVE", "DIVISION ST", "GRAND AVE",
    "MADISON ST", "WASHINGTON BLVD", "JACKSON BLVD", "VAN BUREN ST",
    "HARRISON ST", "POLK ST", "TAYLOR ST", "18TH ST", "26TH ST",
    "31ST ST", "35TH ST", "47TH ST", "55TH ST", "63RD ST", 
    "71ST ST", "79TH ST", "87TH ST", "103RD ST", "111TH ST"
]

# RANDOM DATA GENERATORS
def random_street_name():
    """Pick a random Chicago street"""
    return random.choice(CHICAGO_STREETS)

def random_location():
    """Generate random coordinates within Chicago bounds"""
    # Chicago approximate bounds
    latitude = round(random.uniform(41.65, 42.05), 6)
    longitude = round(random.uniform(-87.95, -87.52), 6)
    return latitude, longitude

def random_timestamp():
    """
    Generate random timestamp within last 7 days
    Simulates historical data for better analytics
    """
    # Random day in the last 7 days
    days_ago = random.randint(0, 7)
    
    # Random hour (0-23)
    hour = random.randint(0, 23)
    
    # Random minute (0-59)
    minute = random.randint(0, 59)
    
    # Random second (0-59)
    second = random.randint(0, 59)
    
    # Create timestamp
    base_time = datetime.now() - timedelta(days=days_ago)
    simulated_time = base_time.replace(
        hour=hour, 
        minute=minute, 
        second=second,
        microsecond=0)
    
    return simulated_time

def random_traffic_volume(hour):
    """
    Generate realistic traffic volume based on hour
    Peak hours (7-9 AM, 5-7 PM) have higher volumes
    """ 
    # Peak morning (7-9 AM)
    if 7 <= hour <= 9:
        return random.randint(8000, 18000)
    
    # Peak evening (5-7 PM)
    elif 17 <= hour <= 19:
        return random.randint(10000, 20000)
    
    # Midday moderate traffic
    elif 10 <= hour <= 16:
        return random.randint(5000, 12000)
    
    # Night/early morning low traffic
    else:
        return random.randint(1000, 6000)

def random_sensor_id():
    """Generate a random sensor ID"""
    return random.randint(1000, 9999)


# TRAFFIC TELEMETRY GENERATOR
def generate_traffic_telemetry():
    """
    Generate a single traffic sensor reading
    Returns dict with same 6 columns as batch processing
    """
    latitude, longitude = random_location()
    timestamp = random_timestamp()
    telemetry = {
        "ID": random_sensor_id(),
        "STREET_NAME": random_street_name(),
        "TIMESTAMP": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        "TOTAL_VEHICLE_VOLUME": random_traffic_volume(timestamp.hour),
        "LATITUDE": latitude,
        "LONGITUDE": longitude
    }
    
    return telemetry

# MAIN PRODUCER
if __name__ == "__main__":
    # Parse command line arguments
    if len(sys.argv) != 3:
        print("Usage: python3 traffic_producer.py <broker> <topic>")
        print("Example: python3 traffic_producer.py kafka:9093 chicago-traffic-telemetry")
        sys.exit(1)
    
    broker = sys.argv[1]
    topic = sys.argv[2]
    
    # Create Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=[broker],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
     
    try:
        while True:
            # Generate synthetic traffic reading
            message = generate_traffic_telemetry()
            
            # Send to Kafka
            producer.send(topic, value=message)
            
            # Print every record
            print(f"Sent: {message}")
            
            time.sleep(2)
    
    except KeyboardInterrupt:
        print("\n\nProducer stopped by user")
    
    finally:
        producer.flush()
        producer.close()
