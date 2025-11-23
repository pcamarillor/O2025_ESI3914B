# the purpose of this code is to crawl the iteso page
# and to filter for useless links and to expand carefully

import sys
import json
import time
import requests
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
                continue
            
            #HERE WE COULD ADD A SECOND KAFKA COMMUNICATION OF THE STATUS OF EACH LINKE WE CHECKED

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

            # ------------------------------------------------------------
            # SEND CONNECTION TO KAFKA (every link)


            message = {
                "from": current_url,
                "to": next_url,
                "depth": depth
            }
            producer.send(topic, value=message)
            print(f"Sent to Kafka: {message}")


            # ------------------------------------------------------------

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






    # Save verified domains list
    with open(verified_domains_file, "w") as f:
        for d in sorted(verified_domains):
            f.write(d + "\n")

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