# the purpose of this code is to crawl the iteso page
# and to filter for useless links and to expand carefully
import csv
import os
import sys
import json
import time
import requests
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from kafka import KafkaProducer

def crawl(seed_url, max_depth, broker, topic, verified_domains_file="verified_domains.txt"):

    #Kafka producer shennanigans
    producer = KafkaProducer(
        bootstrap_servers=[broker],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

    to_visit = [(seed_url, 0)]
    visited = set()
    verified_domains = set()
    rejected_domains = set()

    always_reject = {
        "facebook.com", "www.facebook.com",
        "twitter.com", "www.twitter.com",
        "instagram.com", "www.instagram.com",
        "youtube.com", "www.youtube.com",
        "tiktok.com", "www.tiktok.com",
        "linkedin.com", "www.linkedin.com",
        "goo.gl"
    }


        # CSV buffering system
    buffer = []
    batch_counter = 1
    save_directory = "/data/metrics_health_output"
    error = 0

    os.makedirs(save_directory, exist_ok=True)



    while to_visit:

        #grab item from list
        current_url, depth = to_visit.pop(0)


        #check if weve seen the website
        if current_url in visited or depth > max_depth:
            #we start the cycle again
            continue
        visited.add(current_url)
        print(f"Visiting {current_url} (depth {depth})")


        
        try:
            response = requests.get(current_url, timeout=5)
            if response.status_code != 200:
                print(f"we didnt get the link error")
                error +=1
                continue
            

            soup = BeautifulSoup(response.text, "html.parser")
        except Exception as e:
            print(f".-. Failed, beutiful soup doesnt know {current_url}: {e}")
            continue

        # Extract all links
        for link in soup.find_all("a", href=True):
            href = link["href"]
            next_url = urljoin(current_url, href)
            parsed = urlparse(next_url)

            if parsed.scheme not in ["http", "https"]:
                continue

            #cheese from beutiful soup to get the domain
            domain = parsed.netloc.lower()

            # Auto-accept ITESO domains
            if "iteso.mx" in domain:
                verified_domains.add(domain)

            # Auto-reject list
            elif any(bad in domain for bad in always_reject):
                rejected_domains.add(domain)
                continue

            # Previously rejected
            elif domain in rejected_domains:
                continue

            # Ask user for unverified domains
            elif domain not in verified_domains:
                choice = input(f" Domain {domain} not verified. Allow crawl? [y/n]: ").strip().lower()
                if choice == "y":
                    verified_domains.add(domain)
                else:
                    rejected_domains.add(domain)
                    continue
            
            # we do double verification here, which is unnecesary but nice in the long run.
            if next_url not in visited and depth + 1 <= max_depth:
                to_visit.append((next_url, depth + 1))

                # ------------------------------------------------------------
                # SEND CONNECTION TO KAFKA (every link)


                message = {
                    "from": current_url,
                    "to": next_url,
                    "depth": depth,
                    "timestamp": datetime.utcnow().isoformat()
                }
                producer.send(topic, value=message)
                print(f"Sent to Kafka: {message}")

                # ---- LOCAL CSV BUFFER (instead of Kafka metrics) ---------------------
                metrics = {
                    "timestamp": datetime.utcnow().isoformat(),
                    "current_url": current_url,
                    "depth": depth,
                    "visited_count": len(visited),
                    "queue_size": len(to_visit),
                    "errors_count": len(rejected_domains),
                    "failed_previously":error
                }
                error = 0
                buffer.append(metrics)

                # Every 100 entries → write CSV
                print(f"Length of the buffer{len(buffer)}")
                if len(buffer) >= 100:
                    filename = f"save_{batch_counter}.csv"
                    filepath = os.path.join(save_directory, filename)

                    # Write CSV
                    with open(filepath, "w", newline="", encoding="utf-8") as f:
                        writer = csv.DictWriter(f, fieldnames=buffer[0].keys())
                        writer.writeheader()
                        writer.writerows(buffer)

                    print(f"[CSV SAVED] {filepath}")

                    batch_counter += 1
                    buffer = []  # reset buffer
                #-----------------------------------------------------------------







    # Save verified domains list
    with open(verified_domains_file, "w") as f:
        for d in sorted(verified_domains):
            f.write(d + "\n")

    # Flush remaining entries <100
    if buffer:
        filename = f"save_{batch_counter}.csv"
        filepath = os.path.join(save_directory, filename)

        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=buffer[0].keys())
            writer.writeheader()
            writer.writerows(buffer)

        print(f"[CSV SAVED FINAL] {filepath}")



    print("we finishded crawling")

    # Proper kafka shutdown 
    producer.flush()
    producer.close()


# ------------------------------------------------------------
# Main entrypoint
# ------------------------------------------------------------
if __name__ == "__main__":
    print("We initialized the producer, now verifying arguments")

    
    # python3 crawler.py <seed_url> <max_depth> <broker> <topic>
    if len(sys.argv) != 5:
        print("Usage: python3 crawler.py <seed_url> <max_depth> <broker> <topic>")
        sys.exit(1)

    print("Nice arguments, now we crawl")

    seed_url = sys.argv[1]
    max_depth = int(sys.argv[2])
    broker = sys.argv[3]
    topic = sys.argv[4]

    crawl(seed_url, max_depth=max_depth, broker=broker, topic=topic)