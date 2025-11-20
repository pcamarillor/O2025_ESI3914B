import json
import random
import sys
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer

global i #Para guardar el el orden del ID

BRANDS = ["Apple", "Google", "Motorola", "OnePlus", "Realme", "Samsung", "Xiaomi"]

MODELS = {
    "Apple": ["iPhone 15 Pro", "iPhone 15", "iPhone 14 Pro", "iPhone 14", "iPhone SE"],
    "Samsung": ["Galaxy S24 Ultra", "Galaxy S24", "Galaxy Z Fold 5", "Galaxy A54", "Galaxy S23"],
    "Xiaomi": ["Redmi Note 13 Pro", "Xiaomi 14", "Poco X6 Pro", "Redmi 12", "Mi 13T"],
    "Google": ["Pixel 8 Pro", "Pixel 8", "Pixel 7a", "Pixel Fold"],
    "OnePlus": ["OnePlus 12", "OnePlus 11", "OnePlus Nord 3", "OnePlus Open"],
    "Motorola": ["Edge 40 Pro", "Moto G84", "Edge 30 Ultra", "Razr 40"],
    "Realme": ["Realme GT 5", "Realme 12 Pro", "Realme Narzo 60", "Realme C55"]
}

COUNTRIES = {
    "Brazil": {"currency": "BRL", "language": "Portuguese", "names": ["João", "Maria", "Pedro", "Ana", "Lucas"]},
    "Australia": {"currency": "AUD", "language": "English", "names": ["Jack", "Chloe", "Liam", "Sophie", "Ethan"]},
    "UK": {"currency": "GBP", "language": "English", "names": ["James", "Emma", "Oliver", "Olivia", "Harry"]},
    "USA": {"currency": "USD", "language": "English", "names": ["John", "Sarah", "Michael", "Emily", "David"]},
    "Canada": {"currency": "CAD", "language": "English/French", "names": ["Noah", "Charlotte", "Liam", "Amélie", "Benjamin"]},
    "India": {"currency": "INR", "language": "Hindi/English", "names": ["Raj", "Priya", "Amit", "Sneha", "Arjun"]},
    "Germany": {"currency": "EUR", "language": "German", "names": ["Hans", "Anna", "Klaus", "Sophie", "Max"]},
    "UAE": {"currency": "AED", "language": "Arabic", "names": ["Ahmed", "Fatima", "Omar", "Aisha", "Hassan"]}
}

EXCHANGE_RATES = {
    "GBP": 0.78,
    "EUR": 0.93,
    "USD": 1.0,
    "CAD": 1.38,
    "AUD": 1.53,
    "AED": 3.67,
    "BRL": 5.7,
    "INR": 83.0
}

POSITIVE_REVIEWS = [
    "Exceeded my expectations. Love it!",
    "Impressive camera quality and performance.",
    "Very satisfied with this purchase.",
    "Beautiful design and smooth performance.",
    "Excellent camera and display quality."
]

NEUTRAL_REVIEWS = [
    "Decent phone, nothing special.",
    "Meets basic expectations.",
    "Fair quality for the cost.",
    "Standard phone, no complaints.",
    "Acceptable performance overall."
]

NEGATIVE_REVIEWS = [
    "Very disappointed. Not worth the price.",
    "Battery drains too quickly.",
    "Poor camera quality. Expected better.",
    "Expensive for what you get.",
    "Regret buying this phone."
]

SOURCES = [
    "BestBuy",
    "Flipkart",
    "Amazon",
    "AliExpress"
    "eBay"
]

#funciones random
def random_customer_name(country):
    return random.choice(COUNTRIES[country]["names"])

def random_country():
    return random.choice(list(COUNTRIES.keys()))

def random_brand_model():
    brand = random.choice(BRANDS)
    model = random.choice(MODELS[brand])
    return brand, model

def random_price(brand):
    price_ranges = {
        "Apple": (699, 1199),
        "Samsung": (599, 1099),
        "Google": (499, 899),
        "Xiaomi": (199, 699),
        "OnePlus": (399, 899),
        "Motorola": (199, 599),
        "Realme": (149, 499)
    }
    return round(random.uniform(*price_ranges[brand]), 2)

def generate_review_text(rating):
    if rating >= 4:
        return random.choice(POSITIVE_REVIEWS)
    elif rating == 3:
        return random.choice(NEUTRAL_REVIEWS)
    else:
        return random.choice(NEGATIVE_REVIEWS)

def sentiment_from_rating(rating):
    if rating >= 4:
        return "Positive"
    elif rating == 3:
        return "Neutral"
    else:
        return "Negative"

def gen_review():
    country = random_country()
    country_info = COUNTRIES[country]
    brand, model = random_brand_model()
    price_usd = random_price(brand)
    currency = country_info["currency"]
    exchange_rate = EXCHANGE_RATES[currency]
    price_local = round(price_usd * exchange_rate, 2)
    rating = random.randint(1, 5)
    review_text = generate_review_text(rating)
    review_date = datetime.now()
    
    return {
        "review_id": i,
        "customer_name": random_customer_name(country),
        "age": random.randint(18, 65),
        "brand": brand,
        "model": model,
        "price_usd": price_usd,
        "price_local": f"{price_local}",
        "currency": currency,
        "exchange_rate_to_usd": exchange_rate,
        "rating": rating,
        "review_text": review_text,
        "sentiment": sentiment_from_rating(rating),
        "country": country,
        "language": country_info["language"],
        "review_date": review_date.strftime("%d/%m/%Y"),
        "verified_purchase": random.choice([True, False]),
        "battery_life_rating": random.randint(1, 5),
        "camera_rating": random.randint(1, 5),
        "performance_rating": random.randint(1, 5),
        "design_rating": random.randint(1, 5),
        "display_rating": random.randint(1, 5),
        "review_length": len(review_text),
        "word_count": len(review_text.split()),
        "helpful_votes": random.randint(0, 17),
        "source": random.choice(SOURCES)
    }

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 mobile_reviews_producer.py <broker> <topic>")
        sys.exit(1)

    i = 50000

    broker = sys.argv[1]
    topic = sys.argv[2]

    # Initialize Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=[broker],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

    print(f"Sending mobile phone reviews '{topic}'...")
    try:
        while True:
            message = gen_review()
            producer.send(topic, value=message)
            i+=1
            print(f"Sent: {message}")
            time.sleep(1)
    except KeyboardInterrupt:
        print("Kafka producer stopped by user")
    finally:
        producer.flush()
        producer.close()

