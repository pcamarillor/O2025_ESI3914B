import json
import sys
import time
import pandas as pd

from datetime import datetime
from kafka import KafkaProducer

def load_walmart_data(csv_file):
    """Load Walmart sales CSV data"""
    df = pd.read_csv(csv_file)
    return df

def gen_walmart_record(row, iteration, idx):
    """Generate Walmart sales record from DataFrame row"""
    return {
        "Store": int(row["Store"]),
        "Date": str(row["Date"]),
        "Weekly_Sales": float(row["Weekly_Sales"]),
        "Holiday_Flag": int(row["Holiday_Flag"]),
        "Temperature": float(row["Temperature"]),
        "Fuel_Price": float(row["Fuel_Price"]),
        "CPI": float(row["CPI"]),
        "Unemployment": float(row["Unemployment"]),
        "timestamp": datetime.now().isoformat(),
        "iteration": iteration,
        "record_id": f"{iteration}_{idx}"
    }


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print(f"Usage {sys.argv[0]}.py <broker> <topic>")
        sys.exit(1)

    broker = sys.argv[1]
    topic = sys.argv[2]
    csv_file = sys.argv[3]

    # Load Walmart data
    df = load_walmart_data(csv_file)

    # Initializer Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=[broker],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

    it = 0
    try:
        while True:
            it += 1
            print(f"\nIteration {it}: Sending {len(df)} records")
            
            for idx, row in df.iterrows():
                message = gen_walmart_record(row, it, idx)
                producer.send(topic, value=message)
                
                if (idx + 1) % 100 == 0:
                    print(f"Sent: {idx + 1}/{len(df)} records")
                
                time.sleep(0.1)
            
            print(f"Iteration {it} completed")
            time.sleep(2)
            
    except KeyboardInterrupt:
        print("Kafka producer stopped by user")
    finally:
        producer.flush()
        producer.close()