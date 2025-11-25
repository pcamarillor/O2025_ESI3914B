import json
import random
import sys
import time
from kafka import KafkaProducer
from faker import Faker
from datetime import datetime, timedelta
from contextlib import suppress

fake = Faker()
Faker.seed(1)
random.seed(1)

def generate_tweet():
    now = datetime.now()
    random_days = random.randint(0, 1000)
    random_hours = random.randint(0, 23)
    random_minutes = random.randint(0, 59)
    random_seconds = random.randint(0, 59)
    
    random_timestamp = now - timedelta(
        days=random_days,
        hours=random_hours,
        minutes=random_minutes,
        seconds=random_seconds
    )
    
    tweet = {
        "Tweet_ID": random.randint(1000000, 10000000),
        "Username": fake.user_name(),
        "Text": fake.sentence(nb_words=random.randint(1, 50)),
        "Retweets": random.randint(0, 100),
        "Likes": random.randint(0, 1000),
        "Timestamp": random_timestamp.isoformat()
    }
    return tweet

def produce():
    tweet = generate_tweet()
    # print(tweet)
    producer.send(topic, value=tweet)

if __name__ == "__main__":
    broker = sys.argv[1]
    topic = sys.argv[2]
    sleep = int(sys.argv[3]) 

    producer = KafkaProducer(
        bootstrap_servers=[broker],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

    with suppress(KeyboardInterrupt):
        while True:
            produce()
            time.sleep(sleep) 

    print("Kafka producer stopped by user.")
    producer.flush()
    producer.close()
