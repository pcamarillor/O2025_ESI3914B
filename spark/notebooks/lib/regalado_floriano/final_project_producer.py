import random
import json
import random
import sys
import time
from kafka import KafkaProducer


class Producer:

    @staticmethod
    def random_city():

        cities = [ "Abu Dhabi", "Bangalore", "Beijing", "Berlin", "Birmingham", "Brisbane", "Cape Town", "Chennai", "Chicago", "Delhi", "Dubai", "Frankfurt", "Houston", "Hyderabad", "Johannesburg", "Kyoto", "Liverpool", "London", "Los Angeles", "Lyon", "Manchester", "Marseille", "Melbourne", "Montreal", "Mumbai", "Munich", "New York", "Osaka", "Paris", "Pune", "Rio de Janeiro", "San Francisco", "Shanghai", "Shenzhen", "Singapore", "Sydney", "São Paulo", "Tokyo", "Toronto", "Vancouver"]
        return random.choice(cities)

    @staticmethod
    def city_country_map(city):
        city_country = {
            "Abu Dhabi":         "UAE", "Bangalore":       "India", "Beijing":       "China", "Berlin":     "Germany", "Birmingham":          "UK", "Brisbane":   "Australia", "Cape Town":"South Africa", "Chennai":       "India", "Chicago":         "USA", "Delhi":       "India", "Dubai":         "UAE", "Frankfurt":     "Germany", "Houston":         "USA", "Hyderabad":       "India", "Johannesburg":"South Africa", "Kyoto":       "Japan", "Liverpool":          "UK", "London":          "UK", "Los Angeles":         "USA", "Lyon":      "France", "Manchester":          "UK", "Marseille":      "France", "Melbourne":   "Australia", "Montreal":      "Canada", "Mumbai":       "India", "Munich":     "Germany", "New York":         "USA", "Osaka":       "Japan", "Paris":      "France", "Pune":       "India", "Rio de Janeiro":      "Brazil", "San Francisco":         "USA", "Shanghai":       "China", "Shenzhen":       "China", "Singapore":   "Singapore", "Sydney":   "Australia", "São Paulo":      "Brazil", "Tokyo":       "Japan", "Toronto":      "Canada", "Vancouver":      "Canada",
        }
        return city_country[city]

    @staticmethod
    def gen_telemetry_houses() -> str:
        property_id = random.randint(20001,40001)
        city = Producer.random_city()
        country = Producer.city_country_map(city)
        property_type = random.choice(( "Apartment", "Farmhouse", "Independent House", "Studio", "Townhouse", "Villa",))
        furnishing_status = random.choice( ( "Fully-Furnished", "Semi-Furnished", "Unfurnished"))
        constructed_year = random.randint(1960,2023)
        property_size_sqft = random.randint(401,4001)
        price = random.randint(56288,4202733)
        previous_owners = random.randint(1,61)
        rooms = random.randint(1,9)
        bathrooms = random.randint(1,9)
        garage = bool(random.getrandbits(1))
        garden = bool(random.getrandbits(1))
        crime_cases_reported = random.randint(0,11)
        legal_cases_on_property = bool(random.getrandbits(1))
        customer_salary = random.randint(2001, 100001)
        loan_amount = random.randint(23504, 3520151)
        loan_tenure_years = random.randint(10,31)
        monthly_expenses = random.randint(500,20000)
        down_payment = random.randint(8966,2492724)
        emi_to_income_ratio = random.random()
        satisfaction_score = random.randint(1,11)
        neighbourhood_rating = random.randint(1,11)
        connectivity_score = random.randint(1,11)
        decision = bool(random.getrandbits(1))

        res = f"{{{{'property_id'={property_id}}},{{'country'={country}}},{{'city'={city}}},{{'property_type'={property_type}}},{{'furnishing_status'={furnishing_status}}},{{'property_size_sqft'={property_size_sqft}}},{{'price'={price}}},{{'constructed_year'={constructed_year}}},{{'previous_owners'={previous_owners}}},{{'rooms'={rooms}}},{{'bathrooms'={bathrooms}}},{{'garage'={garage}}},{{'garden'={garden}}},{{'crime_cases_reported'={crime_cases_reported}}},{{'legal_cases_on_property'={legal_cases_on_property}}},{{'customer_salary'={customer_salary}}},{{'loan_amount'={loan_amount}}},{{'loan_tenure_years'={loan_tenure_years}}},{{'monthly_expenses'={monthly_expenses}}},{{'down_payment'={down_payment}}},{{'emi_to_income_ratio'={emi_to_income_ratio}}},{{'satisfaction_score'={satisfaction_score}}},{{'neighbourhood_rating'={neighbourhood_rating}}},{{'connectivity_score'={connectivity_score}}},{{'decision'={decision}}}}}"
        return res 



if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 vg_producer.py <broker> <topic>")
        sys.exit(1)

    broker = sys.argv[1]
    topic = sys.argv[2]

    # Initialize Kafka producer with the correct serializer argument
    producer = KafkaProducer(
        bootstrap_servers=[broker],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

    print("sending...")
    try:
        while True:
            message = Producer.gen_telemetry_houses()
            producer.send(topic, value=message)
            print(f"Sent: {message}")
            time.sleep(2)
    except KeyboardInterrupt:
        print("Kafka producer stopped by user")
    finally:
        producer.flush()
        producer.close()

