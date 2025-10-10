import os
import json
import random
import time
import datetime
import uuid


# Constantes

# Ruta de salida relativa desde la ubicación del script.
# Sube 4 niveles (de jaime_galindo -> lib -> notebooks) y luego baja a data/log_entries
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'data', 'log_entries')

LOG_LEVELS = ['INFO', 'WARN', 'ERROR', 'DEBUG']
SERVER_NODES = ['server-node-1', 'server-node-2', 'api-gateway-a', 'api-gateway-b', 'db-primary']

# diccionario con posibles mensajes para cada log
MESSAGES = {
    'INFO': [
        'User login successful',
        'Data processing complete',
        'New file received from source',
        'Scheduled job finished'
    ],
    'WARN': [
        'Disk usage {}%'.format(random.randint(70, 89)),
        'High memory usage detected',
        'Response time exceeded threshold: {}ms'.format(random.randint(500, 1500))
    ],
    'ERROR': [
        '500 Internal Server Error',
        'Database connection failed',
        'Null pointer exception in module X',
        'Authentication token expired'
    ],
    'DEBUG': [
        'Function entry point reached',
        'Variable value: customer_id={}'.format(random.randint(1000, 9999)),
        'Executing query...'
    ]
}

NUMBER_OF_LOGS_TO_GENERATE = 100 # Cantidad de archivos 
GENERATION_INTERVAL_SECONDS = 2  # Tiempo de espera entre la creación de cada archivo

# --- Lógica del Script ---

def generate_random_log_entry():
    """Crea un diccionario de Python con datos de log aleatorios."""
    # Obtiene el tipo de mensaje y el mensaje entre los que tienen ese tipo de forma aleatoria
    level = random.choice(LOG_LEVELS)
    message_template = random.choice(MESSAGES[level])
    
    # Rellena los placeholders {} en los mensajes que los tengan
    message = message_template.format() 
    
    entry = {
        "timestamp": datetime.datetime.now().isoformat(), # Formato ISO 8601 es estándar y fácil de parsear
        "level": level,
        "message": message,
        "node": random.choice(SERVER_NODES)
    }
    return entry

def main():
    """Función principal para generar y guardar los archivos de log."""
    print(f"Iniciando generador de logs.")
    print(f"Los archivos se guardarán en: {os.path.abspath(OUTPUT_DIR)}")

    # Validar directorio de salida
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    try:
        for i in range(NUMBER_OF_LOGS_TO_GENERATE):
            log_entry = generate_random_log_entry()
            
            # Usar UUID para garantizar nombres de archivo únicos
            filename = f"log_{uuid.uuid4()}.json"
            # Crear el archivo en el path elegido
            filepath = os.path.join(OUTPUT_DIR, filename)

            # Abrir el archivo para escribir la información            
            with open(filepath, 'w') as f:
                json.dump(log_entry, f)
                
            print(f"[{i+1}/{NUMBER_OF_LOGS_TO_GENERATE}] Log generado: {filename}")
            
            time.sleep(GENERATION_INTERVAL_SECONDS)
            
    except KeyboardInterrupt:
        print("\nProceso interrumpido por el usuario.")
    finally:
        print("Generador de logs finalizado.")

if __name__ == "__main__":
    main()