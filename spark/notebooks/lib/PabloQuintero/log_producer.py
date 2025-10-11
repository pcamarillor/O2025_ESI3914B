import time
import random
import socket

def generate_log_entry():
 
    status_codes = [200, 200, 404, 500, 500, 500]  
    methods = ["GET", "POST", "PUT", "DELETE"]
    paths = ["/home", "/api/data", "/login", "/logout", "/dashboard"]

    status = random.choice(status_codes)
    method = random.choice(methods)
    path = random.choice(paths)
    ip = f"192.168.1.{random.randint(1, 255)}"

    log_entry = f'{ip} - - [2025-10-10 10:10:10] "{method} {path} HTTP/1.1" {status} {random.randint(10, 500)}'
    return log_entry

def start_producer(host='localhost', port=9999, interval=0.5, count=10):
    try:
        print(f"Conectando a {host}:{port}...")
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((host, port))
            print("✅ Conexión establecida. Enviando logs...\n")

            for i in range(count):
                log = generate_log_entry()
                print(f"📤 [{i+1}/{count}] Enviado: {log}")
                s.sendall((log + "\n").encode("utf-8"))
                time.sleep(interval)

            print("\nEnvío de logs finalizado.")

    except ConnectionRefusedError:
        print(f"ERROR: No se pudo conectar a {host}:{port}. ¿Está netcat corriendo?")
    except Exception as e:
        print(f"ERROR inesperado: {e}")

if __name__ == "__main__":
    start_producer()
