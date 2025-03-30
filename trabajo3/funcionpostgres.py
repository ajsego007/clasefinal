import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from io import StringIO
import os
from dotenv import load_dotenv

# Configuración (usa .env para seguridad)
load_dotenv()

BASE_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', ''),
    'port': os.getenv('DB_PORT', '5432')
}

DB_NAME = os.getenv('DB_NAME', 'bdd_threats_cybersecurity')

def crear_basedatos(dbname):
    """Crea la base de datos si no existe"""
    try:
        # Conexión al servidor PostgreSQL (sin base de datos específica)
        conn = psycopg2.connect(**BASE_CONFIG)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        
        with conn.cursor() as cursor:
            # Verificar si la base de datos existe
            cursor.execute(sql.SQL("SELECT 1 FROM pg_database WHERE datname = %s"), [dbname])
            exists = cursor.fetchone()
            
            if not exists:
                cursor.execute(sql.SQL("CREATE DATABASE {}").format(
                    sql.Identifier(dbname)))
                print(f"✅ Base de datos '{dbname}' creada exitosamente")
            else:
                print(f"ℹ️ La base de datos '{dbname}' ya existe")
    
    except Exception as e:
        print(f"❌ Error al crear la base de datos: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

def guardar_registros(df, table_name, dbname=DB_NAME, lowercase_text=True):
    """
    Carga un DataFrame de pandas a una tabla de PostgreSQL.
    Crea la base de datos si no existe.
    
    Args:
        df (pd.DataFrame): DataFrame con los datos a cargar
        table_name (str): Nombre de la tabla destino
        dbname (str): Nombre de la base de datos
        lowercase_text (bool): Convertir texto a minúsculas
    """
    try:
        # 1. Crear la base de datos si no existe
        crear_basedatos(dbname)
        
        # 2. Configuración de conexión con la base de datos específica
        db_config = {**BASE_CONFIG, 'database': dbname}
        
        # 3. Preprocesamiento del DataFrame
        if lowercase_text:
            text_cols = df.select_dtypes(include=['object']).columns
            df[text_cols] = df[text_cols].apply(lambda x: x.str.lower())
        
        # 4. Conectar y cargar datos
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cursor:
                conn.autocommit = False
                
                # 5. Crear tabla (si no existe)
                columns = []
                for col, dtype in df.dtypes.items():
                    col_clean = col.replace(' ', '_').lower()  # Limpia nombres
                    if pd.api.types.is_integer_dtype(dtype):
                        pg_type = 'BIGINT'
                    elif pd.api.types.is_float_dtype(dtype):
                        pg_type = 'FLOAT'
                    elif pd.api.types.is_datetime64_any_dtype(dtype):
                        pg_type = 'TIMESTAMP'
                    else:
                        pg_type = 'TEXT'
                    columns.append(f'"{col_clean}" {pg_type}')
                
                create_query = sql.SQL("""
                    CREATE TABLE IF NOT EXISTS {table} (
                        {columns}
                    );
                """).format(
                    table=sql.Identifier(table_name),
                    columns=sql.SQL(',\n'.join(columns)))
                
                cursor.execute(create_query)
                
                # 6. Cargar datos usando COPY
                buffer = StringIO()
                df.to_csv(buffer, index=False, header=False, sep='\t', na_rep='NULL')
                buffer.seek(0)
                
                cursor.copy_expert(
                    sql.SQL("COPY {} FROM STDIN WITH (FORMAT CSV, DELIMITER '\t', NULL 'NULL')")
                    .format(sql.Identifier(table_name)),
                    buffer)
                
                conn.commit()
                print(f"✅ Datos cargados exitosamente en {dbname}.{table_name}: {len(df)} registros")
    
    except Exception as e:
        print(f"❌ Error: {e}")
        if 'conn' in locals():
            conn.rollback()
        raise

# Ejemplo de uso:
if __name__ == "__main__":
    # Ruta de acceso a los datos
    csv_path = 'Global_Cybersecurity_Threats_2015-2024.csv'

    # Especificamos el separador
    threats = pd.read_csv(csv_path, sep=',')
    
    # Llamar a la función
    guardar_registros(threats, 'cyber_threats', lowercase_text=True)