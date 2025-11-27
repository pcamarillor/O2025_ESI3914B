import sys
import os
import csv
import random
import time
from datetime import datetime, timedelta
from pathlib import Path

HEADER = ["Ride_ID","Driver_ID","Ride_City","Date", "Distance_km","Duration_min","Fare","Rating","Promo_Code"]

PROMOS = ["", "", "", "PROMO10", "FIRST50", "SPRING5"]  # blanks for no promo

def random_date(start_date, end_date):
    delta = end_date - start_date
    rand_days = random.randint(0, delta.days)
    return start_date + timedelta(days=rand_days)

def load_driver_ids(drivers_csv_path):
    ids = []
    try:
        with open(drivers_csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for r in reader:
                try:
                    ids.append(int(r.get("Driver_ID") or r.get("Driver_ID".strip())))
                except:
                    continue
    except FileNotFoundError:
        raise FileNotFoundError(f"Drivers CSV no encontrado en: {drivers_csv_path}")
    if not ids:
        raise ValueError("No se leyeron Driver_IDs del CSV de drivers.")
    return ids

def random_ride_row(ride_id, driver_ids, cities, start_date, end_date):
    driver_id = random.choice(driver_ids)
    ride_date = random_date(start_date, end_date)
    city = random.choice(cities)
    distance = round(random.uniform(0.5, 40.0), 2)
    # duration roughly 2-4 min per km plus some fixed overhead
    duration = max(1, int(distance * random.uniform(2.0, 4.0) + random.randint(1,5)))
    fare = round(2.5 + distance * random.uniform(0.8, 2.0) + random.uniform(0,5), 2)
    rating = round(random.uniform(3.0, 5.0), 1)
    promo = random.choice(PROMOS)
    return [ride_id, driver_id, city, ride_date.strftime("%m/%d/%Y"), distance, duration, fare, rating, promo]

def main():
    if len(sys.argv) < 2:
        print("Uso: python producer_rides.py <num_files>")
        sys.exit(1)
    try:
        n_files = int(sys.argv[1])
        if n_files <= 0:
            raise ValueError()
    except ValueError:
        print("El parámetro <num_files> debe ser un entero positivo.")
        sys.exit(1)

    drivers_rel = os.path.join('..', '..', '..', '..', 'data', 'final_project', 'Cityride Drivers Data.csv')
    drivers_csv = os.path.abspath(os.path.join(os.path.dirname(__file__), drivers_rel))

    default_rel_out = os.path.join('..', '..', '..', '..', 'data', 'final_project', 'producer', 'rides')
    default_out_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), default_rel_out))

    out_path = Path(default_out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    print(f"Salida: {out_path}")
    # load driver ids
    driver_ids = load_driver_ids(drivers_csv)

    cities = ["San Francisco", "Los Angeles", "Chicago", "New York", "Miami"]
    start_date = datetime.now() - timedelta(days=365)  # last year
    end_date = datetime.now()
    base_ride_id = random.randint(100000, 999999)
    for i in range(1, n_files+1):
        ts = int(time.time())
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        filename = f"Cityride_Rides_{ts}_{timestamp}_{i}.csv"
        filepath = out_path / filename
        n_rows = random.randint(50, 300)

        with open(filepath, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(HEADER)
            for r in range(n_rows):
                row = random_ride_row(base_ride_id, driver_ids, cities, start_date, end_date)
                writer.writerow(row)
                base_ride_id += 1

        print(f"Generado: {filepath.name} ({n_rows} filas)")
        time.sleep(5)

if __name__ == "__main__":
    main()