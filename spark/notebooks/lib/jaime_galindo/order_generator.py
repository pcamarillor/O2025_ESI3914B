
from faker import Faker #type: ignore
from faker_commerce import Provider #type: ignore
import random


class OrderGenerator:

    @staticmethod
    def create_random_order(order_id):
        fake = Faker()
        fake.add_provider(Provider)
        
        num_items = random.randint(1, 4)
        items = []
        total_amount = 0

        for _ in range(num_items):
            # Variables random para orden
            unit_price = round(random.uniform(100.0, 2500.0), 2)
            quantity = random.randint(1, 3)
            discount = random.choice([0, 0.1, 0.15, 0.2])
            final_price = round(unit_price * (1 - discount), 2)
            total_amount += final_price * quantity
            category = random.choice(["Electrónica > Audio", "Hogar > Cocina", "Deportes > Fitness", "Moda > Calzado"])
            region = random.choice(["GDL", "CDMX", "MTY", "CUN"])
            payment_method = random.choice(["MP", "Credit Card", "Debit Card", "OXXO"])
            logistics_provider = random.choice(["Estafeta", "DHL", "FedEx", "Paquetexpress"])
            warehouse_origin = f"{random.choice(['CDMX', 'GDL'])}-{random.choice(['Norte', 'Sur'])}"
            tracking_id = f"{random.choice(['EST', 'DHL', 'FDX'])}-{fake.uuid4()[:10].upper()}"

            # Items random para orden
            item = {
                "item_id": f"I-{fake.uuid4()[:6]}",
                "title": fake.ecommerce_name(),
                "category": category,
                "quantity": quantity,
                "unit_price": unit_price,
                "final_price": final_price,
                "discount_applied": discount > 0
            }
            items.append(item)

        # Regresar JSON de la orden
        return {
            "order_id": f"O-{order_id:06}",
            "timestamp": fake.iso8601(),
            "user": {
                "user_id": f"U-{fake.uuid4()[:6]}",
                "region": region,
                "payment_method": payment_method,
            },
            "items": items,
            "total_amount": round(total_amount, 2),
            "shipping": {
                "logistics_provider": logistics_provider,
                "warehouse_origin": warehouse_origin,
                "estimated_delivery": fake.future_date().isoformat(),
                "tracking_id": tracking_id
            }
        }
