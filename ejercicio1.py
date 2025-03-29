import psycopg2
import json
from datetime import datetime

# Paso 1: Crear la base de datos y la tabla desde Python
def create_database_and_table():
    try:
        # Conexión al servidor PostgreSQL (sin especificar una base de datos)
        conn = psycopg2.connect(
            user="postgres", # Usuario predeterminado
            password="postgres",
            host="localhost",
            port="5432"
        )
        conn.autocommit = True # Necesario para crear una base de datos
        cursor = conn.cursor()

        # Crear la base de datos "cybersecurity_logs" si no existe
        cursor.execute("SELECT datname FROM pg_database WHERE datname='cybersecurity_logs';")
        if not cursor.fetchone():
            cursor.execute("CREATE DATABASE cybersecurity_logs;")
            print("Base de datos 'cybersecurity_logs' creada correctamente.")
        else:
            print("La base de datos 'cybersecurity_logs' ya existe.")

        # Cerrar la conexión inicial
        cursor.close()
        conn.close()

        # Conectar a la nueva base de datos
        conn = psycopg2.connect(
        database="cybersecurity_logs",
        user="postgres",
        password="postgres",
        host="localhost",
        port="5432"
        )
        cursor = conn.cursor()

        # Crear la tabla "structured_logs" si no existe
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS structured_logs (
        id SERIAL PRIMARY KEY,
        timestamp TIMESTAMP NOT NULL,
        ip_address VARCHAR(15),
        event_type VARCHAR(50),
        description TEXT
        );
        """)
        print("Tabla 'structured_logs' creada correctamente.")

        # Confirmar cambios y cerrar la conexión
        conn.commit()
        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Error al crear la base de datos o la tabla: {e}")

# Paso 2: Insertar datos estructurados en la tabla
def insert_structured_logs():
    try:
        # Conectar a la base de datos
        conn = psycopg2.connect(
        database="cybersecurity_logs",
        user="postgres",
        password="postgres",
        host="localhost",
        port="5432"
        )
        cursor = conn.cursor()

        # Datos de ejemplo para insertar
        logs = [
        (datetime(2025, 2, 22, 10, 0, 0), "192.168.1.10", "login_failed", "Intento de inicio de sesión fallido desde IP 192.168.1.10"),
        (datetime(2025, 2, 22, 10, 5, 0), "192.168.1.15", "access_denied", "Acceso denegado a recurso crítico")
        ]

        # Insertar registros en la tabla
        cursor.executemany("""
        INSERT INTO structured_logs (timestamp, ip_address, event_type, description)
        VALUES (%s, %s, %s, %s);
        """, logs)

        # Confirmar cambios y cerrar la conexión
        conn.commit()
        print("Datos estructurados insertados correctamente.")
        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Error al insertar datos estructurados: {e}")

# Paso 3: Guardar logs no estructurados en un archivo JSON
def save_unstructured_logs():
    try:
    # Simulación de logs no estructurados
        unstructured_logs = [
        "[2025-02-22 10:10:00] Firewall Alert: Blocked incoming traffic from 10.0.0.1 to port 22.",
        "[2025-02-22 10:15:00] IDS Alert: Suspicious activity detected from IP 192.168.1.20."
        ]

        # Guardar logs en un archivo JSON
        with open("unstructured_logs.json", "w") as file:
            json.dump(unstructured_logs, file, indent=4)

        print("Logs no estructurados guardados en 'unstructured_logs.json'")

    except Exception as e:
        print(f"Error al guardar logs no estructurados: {e}")

# Ejecutar todas las funciones
if __name__ == "__main__":
    create_database_and_table() # Crear base de datos y tabla
    insert_structured_logs() # Insertar datos estructurados
    save_unstructured_logs() # Guardar logs no estructurados