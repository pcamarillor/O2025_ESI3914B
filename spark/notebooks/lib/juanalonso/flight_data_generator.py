import random
import uuid
from datetime import datetime, timedelta

from faker import Faker

class FlightDataGenerator:
    def __init__(self):
        self.faker = Faker("en_US")

        self.airlines = [
            "AeroJet", "SkyWings", "LatamExpress",
            "GlobalAir", "PacificSky", "EuroFly"
        ]
        self.cities = [
            "Mexico City", "Guadalajara", "Monterrey",
            "Los Angeles", "Houston", "New York",
            "Madrid", "Bogota", "Lima"
        ]

        self.time_slots = [
            "Early_Morning", "Morning", "Afternoon",
            "Evening", "Night", "Late_Night"
        ]

        self.stop_options = ["zero", "one", "two_or_more"]

    def _random_route(self):
        source, dest = random.sample(self.cities, 2)
        return source, dest

    def _random_times(self):
        dep_slot = random.choice(self.time_slots)
        arr_slot = random.choice(self.time_slots)
        return dep_slot, arr_slot

    def _random_duration(self):
        return round(random.uniform(60, 900), 1)

    def _random_days_left(self):
        return random.randint(0, 60)

    def _random_price(self, duration, stops):
        base = 1000 + duration * 2.5
        if stops == "zero":
            base += 1200
        elif stops == "two_or_more":
            base -= 400

        noise = random.uniform(-500, 700)
        price = max(500, base + noise)
        return round(price, 2)

    def generate_ticket_record(self):
        airline = random.choice(self.airlines)
        source, dest = self._random_route()
        departure_time, arrival_time = self._random_times()
        stops = random.choice(self.stop_options)
        duration = self._random_duration()
        days_left = self._random_days_left()
        price = self._random_price(duration, stops)

        booking_date = datetime.utcnow().date()
        travel_date = booking_date + timedelta(days=days_left)

        record = {
            "ticket_id": str(uuid.uuid4()),
            "airline": airline,
            "source_city": source,
            "destination_city": dest,
            "departure_time": departure_time,
            "arrival_time": arrival_time,
            "stops": stops,
            "duration": duration,
            "days_left": days_left,
            "booking_date": booking_date.isoformat(),
            "travel_date": travel_date.isoformat(),
            "price": price
        }

        return record
