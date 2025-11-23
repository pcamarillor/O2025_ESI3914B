#this code was assisted with chatgpt

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import csv

#i just modified this, so before running make an attempt to see if it does send the file to the correct directory, same thing twice but in the teachers repository

def crawl(seed_url, max_depth=2, verified_domains_file="verified_domains.txt", connections_file="../../../../data/project02streamingdata/connections2.csv"):
    to_visit = [(seed_url, 0)]
    visited = set()
    verified_domains = set()
    rejected_domains = set()
    connections = []

    always_reject = {
        "facebook.com",
        "www.facebook.com",
        "twitter.com",
        "www.twitter.com",
        "instagram.com",
        "www.instagram.com",
        "youtube.com",
        "www.youtube.com",
        "tiktok.com",
        "www.tiktok.com",
        "linkedin.com",
        "www.linkedin.com",
        "goo.gl"
    }

    while to_visit:
        current_url, depth = to_visit.pop(0)
        if current_url in visited or depth > max_depth:
            continue
        visited.add(current_url)

        print(f"Visiting {current_url} (depth {depth})")

        try:
            response = requests.get(current_url, timeout=5)
            if response.status_code != 200:
                continue
            soup = BeautifulSoup(response.text, "html.parser")
        except Exception as e:
            print(f".-. Failed {current_url}: {e}")
            continue

        for link in soup.find_all("a", href=True):
            href = link["href"]
            next_url = urljoin(current_url, href)
            parsed = urlparse(next_url)

            if parsed.scheme not in ["http", "https"]:
                continue

            domain = parsed.netloc.lower()

            # Always record the edge (even if rejected)
            connections.append((current_url, next_url))

            # Skip anchors (#)
            if next_url.endswith("#"):
                continue

            #Auto-accept ITESO domains
            if "iteso.mx" in domain:
                verified_domains.add(domain)
            #Auto-reject always_reject list
            elif any(bad in domain for bad in always_reject):
                rejected_domains.add(domain)
                continue
            #Skip if previously rejected
            elif domain in rejected_domains:
                continue
            #Ask for unknown domains
            elif domain not in verified_domains:
                choice = input(f"❓ Domain {domain} not verified. Allow crawl? [y/n]: ").strip().lower()
                if choice == "y":
                    verified_domains.add(domain)
                else:
                    rejected_domains.add(domain)
                    continue

            #Add to queue if verified and within depth limit
            if next_url not in visited and depth + 1 <= max_depth:
                to_visit.append((next_url, depth + 1))

    # Save verified domains
    with open(verified_domains_file, "w") as f:
        for d in sorted(verified_domains):
            f.write(d + "\n")

    # Save connections
    with open(connections_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["from", "to"])
        writer.writerows(connections)

    print("✅ Done. Results saved.")

# Example run
crawl("https://www.iteso.mx", max_depth=2)
