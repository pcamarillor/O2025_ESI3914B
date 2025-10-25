# jjodiaz/data_generator.py

import csv
import random
from faker import Faker
from datetime import datetime, timedelta
import uuid
import os

class EcommDataGenerator:
    """
    Genera datos sintéticos de ventas de e-commerce con un esquema denormalizado.
    """
    def __init__(self, num_clientes=500, num_productos=100, locale='es_MX'):
        self.fake = Faker(locale)
        self.num_clientes = num_clientes
        self.num_productos = num_productos
        # Define listas ANTES de usarlas en los generadores
        self.categorias = ['Electrónicos', 'Ropa', 'Hogar', 'Libros', 'Juguetes', 'Alimentos']
        self.ciudades = ['Culiacán', 'Mazatlán', 'Guadalajara', 'Ciudad de México', 'Monterrey', 'Tijuana']
        # Ahora sí, genera clientes y productos
        self.clientes = self._generate_clientes()
        self.productos = self._generate_productos()

    def _generate_clientes(self):
        """Genera una lista de clientes únicos."""
        clientes = {}
        for _ in range(self.num_clientes):
            cliente_id = f"CUS_{self.fake.unique.random_number(digits=6)}"
            clientes[cliente_id] = {
                'cliente_id': cliente_id,
                'nombre_cliente': self.fake.name(),
                'ciudad_cliente': random.choice(self.ciudades),
                'email_cliente': self.fake.email()
            }
        return clientes

    def _generate_productos(self):
        """Genera una lista de productos únicos."""
        productos = {}
        for _ in range(self.num_productos):
            producto_id = f"PROD_{self.fake.unique.random_number(digits=5)}"
            categoria = random.choice(self.categorias)
            productos[producto_id] = {
                'producto_id': producto_id,
                'nombre_producto': f"{categoria} Modelo {self.fake.word().capitalize()}",
                'categoria_producto': categoria,
                'precio_unitario': round(random.uniform(50.0, 5000.0), 2)
            }
        return productos

    def generate_sale_record(self):
        """Genera un registro de venta denormalizado."""
        cliente_id = random.choice(list(self.clientes.keys()))
        producto_id = random.choice(list(self.productos.keys()))

        cliente = self.clientes[cliente_id]
        producto = self.productos[producto_id]

        venta_id = str(uuid.uuid4())
        fecha = self.fake.date_between(start_date='-1y', end_date='today').strftime('%Y-%m-%d')
        cantidad = random.randint(1, 5)
        precio_unitario = producto['precio_unitario']
        total_venta = round(precio_unitario * cantidad, 2)

        return {
            'venta_id': venta_id,
            'fecha': fecha,
            'cliente_id': cliente['cliente_id'],
            'nombre_cliente': cliente['nombre_cliente'],
            'ciudad_cliente': cliente['ciudad_cliente'],
            'email_cliente': cliente['email_cliente'],
            'producto_id': producto['producto_id'],
            'nombre_producto': producto['nombre_producto'],
            'categoria_producto': producto['categoria_producto'],
            'precio_unitario': precio_unitario,
            'cantidad': cantidad,
            'total_venta': total_venta
        }

    def generate_data(self, num_records):
        """Genera una lista de registros de venta."""
        return [self.generate_sale_record() for _ in range(num_records)]

    def save_to_csv(self, data, file_path, file_name="generated_sales.csv"):
        """Guarda los datos generados en un archivo CSV."""
        if not data:
            print("No hay datos para guardar.")
            return

        os.makedirs(file_path, exist_ok=True)
        full_file_path = os.path.join(file_path, file_name)
        headers = data[0].keys()

        try:
            with open(full_file_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=headers)
                writer.writeheader()
                writer.writerows(data)
            print(f"Datos guardados exitosamente en {full_file_path}")
        except IOError as e:
            print(f"Error al escribir el archivo CSV: {e}")


if __name__ == "__main__":
    NUM_VENTAS = 10000
    OUTPUT_PATH = "../../../data/generated_ecommerce_sales/"

    generator = EcommDataGenerator(num_clientes=500, num_productos=100)
    datos_ventas = generator.generate_data(NUM_VENTAS)
    generator.save_to_csv(datos_ventas, OUTPUT_PATH)