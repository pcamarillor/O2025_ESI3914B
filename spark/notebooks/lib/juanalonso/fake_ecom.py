# fake_ecom.py (fixed)
import os
from faker import Faker
import pandas as pd
import random
from datetime import datetime, timedelta

class FakeEcomGenerator:
    def __init__(self, seed=42):
        self.f = Faker()
        Faker.seed(seed)
        random.seed(seed)

    def generate_products(self, n_products=1000, n_brands=50, n_categories=20):
        brands = [self.f.company() for _ in range(n_brands)]
        categories = [self.f.word() for _ in range(n_categories)]
        rows = []
        for i in range(n_products):
            rows.append({
                'product_id': f'prod_{i:04d}',
                'name': self.f.catch_phrase(),
                'price': round(random.uniform(5, 500), 2),
                'sku': self.f.bothify(text='??-####'),
                'brand': random.choice(brands),
                'category': random.choice(categories)
            })
        return pd.DataFrame(rows)

    def generate_events(self, n_users=500, n_events=50000, product_ids=None):
        users = [f'user_{i:04d}' for i in range(n_users)]
        event_types = ['purchase', 'add_to_cart', 'review', 'return']
        rows = []
        start = datetime.now() - timedelta(days=90)
        for i in range(n_events):
            pid = random.choice(product_ids)
            evt = random.choice(event_types)
            ts = start + timedelta(seconds=random.randint(0, 90*24*3600))
            row = {
                'user_id': random.choice(users),
                'product_id': pid,
                'event_type': evt,
                'event_ts': ts,
                'session_id': f'session_{random.randint(0,200000)}',
                'price': None,
                'rating': None
            }
            if evt == 'purchase' or evt == 'return':
                row['price'] = round(random.uniform(5, 500),2)
            if evt == 'review':
                row['rating'] = random.randint(1,5)
            rows.append(row)
        return pd.DataFrame(rows)

    def generate_and_save_all(self, path='data/juanalonso/ecom', n_products=1000, n_users=500, n_events=50000):
        """Generates products and events and saves CSVs to path."""
        products = self.generate_products(n_products=n_products)
        events = self.generate_events(n_users=n_users, n_events=n_events, product_ids=products['product_id'].tolist())
        os.makedirs(path, exist_ok=True)
        products.to_csv(f"{path}/products.csv", index=False)
        events.to_csv(f"{path}/user_events.csv", index=False)
        return products, events

